#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import re
from typing import Any, Dict, List, Optional

import mysql.connector

MYSQL_HOST = os.getenv("MYSQL_HOST", "10.111.111.111") 
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "1111")) 
MYSQL_DB = os.getenv("MYSQL_DB", "fspas")
MYSQL_USER = os.getenv("MYSQL_USER", "dbuser")
MYSQL_PASS = os.getenv("MYSQL_PASS", "12345") 

MAX_SAMPLES = 5
CONTEXT_WINDOW = 100
MAX_CONTEXT_SNIPPETS_PER_TERM = 3


def get_mysql_conn():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        database=MYSQL_DB,
        user=MYSQL_USER,
        password=MYSQL_PASS,
        autocommit=False,
    )


def normalize_candidate_text(s: str) -> str:
    s = str(s or "")
    s = s.replace("\u00A0", " ")
    s = s.lower()
    s = s.replace("-", " ").replace("_", " ").replace("/", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _safe_json_loads(v: Any) -> List[str]:
    if not v:
        return []
    if isinstance(v, list):
        return [str(x) for x in v if str(x).strip()]
    try:
        arr = json.loads(v)
        if isinstance(arr, list):
            return [str(x) for x in arr if str(x).strip()]
    except Exception:
        pass
    return []


def _merge_samples(old_items: List[str], new_items: Optional[List[str]], max_n: int = MAX_SAMPLES) -> List[str]:
    items = [str(x).strip() for x in old_items if str(x).strip()]
    seen = set(x.lower() for x in items)

    for x in (new_items or []):
        s = str(x).strip()
        if not s:
            continue
        key = s.lower()
        if key in seen:
            continue
        items.append(s)
        seen.add(key)
        if len(items) >= max_n:
            break

    return items[:max_n]


def _clean_text_for_snippet(text: str) -> str:
    t = str(text or "")
    t = t.replace("\r\n", "\n").replace("\r", "\n")
    t = t.replace("\u00A0", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _token_boundary_pattern(term: str) -> re.Pattern:
    term = str(term or "").strip()
    escaped = re.escape(term)
    escaped = escaped.replace(r"\ ", r"\s+")
    return re.compile(rf"(?i)(?<![A-Za-z0-9가-힣]){escaped}(?![A-Za-z0-9가-힣])")


def _extract_context_snippets(text: str, term: str, window: int = CONTEXT_WINDOW, max_n: int = MAX_CONTEXT_SNIPPETS_PER_TERM) -> List[str]:
    """
    text 안에서 term이 등장하는 위치를 찾아 앞뒤 문맥 snippet 반환
    """
    text = _clean_text_for_snippet(text)
    term = str(term or "").strip()

    if not text or not term:
        return []

    out = []
    seen = set()

    # 1) 경계 기반 검색 우선
    try:
        pat = _token_boundary_pattern(term)
        matches = list(pat.finditer(text))
    except Exception:
        matches = []

    # 2) fallback: 단순 포함 검색
    if not matches:
        lower_text = text.lower()
        lower_term = term.lower()
        start = 0
        matches = []
        while True:
            idx = lower_text.find(lower_term, start)
            if idx < 0:
                break
            class _M:
                def __init__(self, s, e):
                    self._s = s
                    self._e = e
                def start(self): return self._s
                def end(self): return self._e
            matches.append(_M(idx, idx + len(term)))
            start = idx + len(term)

    for m in matches:
        s = max(0, m.start() - window)
        e = min(len(text), m.end() + window)

        snippet = text[s:e].strip()

        # 앞뒤 잘린 느낌 표시
        if s > 0:
            snippet = "..." + snippet
        if e < len(text):
            snippet = snippet + "..."

        snippet = re.sub(r"\s+", " ", snippet).strip()
        if not snippet:
            continue

        key = snippet.lower()
        if key in seen:
            continue

        seen.add(key)
        out.append(snippet)

        if len(out) >= max_n:
            break

    return out


def upsert_candidate(
    cur,
    candidate_type: str,
    raw_text: str,
    scope: str = "all",
    suggested_canonical: Optional[str] = None,
    source_stage: str = "build_serving_views",
    source_rule: Optional[str] = None,
    confidence: Optional[float] = None,
    sample_doc_id: Optional[str] = None,
    sample_title: Optional[str] = None,
    sample_snippets: Optional[List[str]] = None,
) -> None:
    raw_text = str(raw_text or "").strip()
    if not raw_text:
        return

    normalized_text = normalize_candidate_text(raw_text)
    if not normalized_text:
        return

    # 💡 [신규 로직] 이미 정식 사전에 등록된 용어인지 확인
    sql_check = "SELECT 1 FROM term_aliases WHERE alias_normalized = %s AND status = 'active' LIMIT 1"
    cur.execute(sql_check, (normalized_text,))
    is_already_active = cur.fetchone() is not None

    # 💡 이미 있으면 'already_active', 없으면 'pending'으로 상태 설정
    target_review_status = 'already_active' if is_already_active else 'pending'

    # 💡 기존 SELECT문에 review_status를 추가하여 가져옵니다.
    sql_select = """
    SELECT
        candidate_id,
        detected_count,
        sample_doc_ids_json,
        sample_titles_json,
        sample_snippets_json,
        review_status
    FROM term_candidate_queue
    WHERE candidate_type = %s
      AND normalized_text = %s
      AND scope = %s
    LIMIT 1
    """
    cur.execute(sql_select, (candidate_type, normalized_text, scope))
    row = cur.fetchone()

    incoming_doc_ids = [sample_doc_id] if sample_doc_id else []
    incoming_titles = [sample_title] if sample_title else []
    incoming_snippets = sample_snippets or []

    if row:
        candidate_id = int(row[0])
        detected_count = int(row[1] or 0)
        current_review_status = str(row[5] or "pending") # 기존 상태 확인

        doc_ids = _safe_json_loads(row[2])
        titles = _safe_json_loads(row[3])
        snippets = _safe_json_loads(row[4])

        doc_ids = _merge_samples(doc_ids, incoming_doc_ids)
        titles = _merge_samples(titles, incoming_titles)
        snippets = _merge_samples(snippets, incoming_snippets)

        # 💡 [상태 전이 로직] 
        # 기존에 대기열(pending) 상태였더라도, 그 사이 정식 사전에 등록되었다면 'already_active'로 전환
        new_status = 'already_active' if (is_already_active and current_review_status == 'pending') else current_review_status

        sql_update = """
        UPDATE term_candidate_queue
        SET
            raw_text = %s,
            suggested_canonical = COALESCE(%s, suggested_canonical),
            source_stage = %s,
            source_rule = COALESCE(%s, source_rule),
            confidence = CASE
                WHEN %s IS NULL THEN confidence
                WHEN confidence IS NULL THEN %s
                WHEN %s > confidence THEN %s
                ELSE confidence
            END,
            detected_count = %s,
            sample_doc_ids_json = %s,
            sample_titles_json = %s,
            sample_snippets_json = %s,
            review_status = %s,   /* 💡 갱신된 상태 반영 */
            last_seen_at = CURRENT_TIMESTAMP
        WHERE candidate_id = %s
        """
        cur.execute(
            sql_update,
            (
                raw_text,
                suggested_canonical,
                source_stage,
                source_rule,
                confidence, confidence, confidence, confidence,
                detected_count + 1,
                json.dumps(doc_ids, ensure_ascii=False),
                json.dumps(titles, ensure_ascii=False),
                json.dumps(snippets, ensure_ascii=False),
                new_status,
                candidate_id,
            ),
        )
        return

    # 💡 INSERT 처리 부분: 마지막 파라미터가 'pending' 하드코딩에서 %s (동적 변수)로 변경됨
    sql_insert = """
    INSERT INTO term_candidate_queue
    (
        candidate_kind,
        candidate_type,
        raw_text,
        normalized_text,
        suggested_canonical,
        scope,
        source_stage,
        source_rule,
        confidence,
        detected_count,
        sample_doc_ids_json,
        sample_titles_json,
        sample_snippets_json,
        status,
        review_status
    )
    VALUES ('new_term', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending', %s)
    """
    cur.execute(
        sql_insert,
        (
            candidate_type,
            raw_text,
            normalized_text,
            suggested_canonical,
            scope,
            source_stage,
            source_rule,
            confidence,
            1,
            json.dumps(_merge_samples([], incoming_doc_ids), ensure_ascii=False),
            json.dumps(_merge_samples([], incoming_titles), ensure_ascii=False),
            json.dumps(_merge_samples([], incoming_snippets), ensure_ascii=False),
            target_review_status, # 💡 이미 사전에 있으면 already_active, 없으면 pending
        ),
    )

def collect_candidates_from_additional(
    doc: Dict[str, Any],
    additional: Dict[str, Any],
    category: str,
    source_stage: str = "build_serving_views_full",
) -> None:
    """
    FULL 결과 기준으로 후보를 term_candidate_queue에 적재
    - term_id가 이미 있는 후보는 skip
    - sample_snippets_json에는 문서 앞부분이 아니라 '실제 용어가 등장한 주변 문맥'을 저장
    """
    if not isinstance(additional, dict):
        return

    doc_id = doc.get("doc_id")
    title = doc.get("title") or ""
    content = doc.get("content") or ""

    search_text = f"{title}\n{content}".strip()

    mappings = [ 
        ("product_candidates", "product"), 
        ("process_candidates", "process"), 
        ("chemistry_candidates", "chemistry"), 
        ("defect_candidates", "defect"), 
        ("equipment_candidates", "equipment"), 
        ("analysis_candidates", "analysis"), 
    ]


    conn = get_mysql_conn()
    cur = conn.cursor()
    try:
        for field_name, candidate_type in mappings:
            items = additional.get(field_name) or []
            if not isinstance(items, list):
                continue

            for item in items:
                if not isinstance(item, dict):
                    continue

                # 이미 사전에 매핑된 후보는 skip
                if item.get("term_id") is not None:
                    continue

                raw_text = str(item.get("raw") or "").strip()
                if not raw_text:
                    continue

                canonical = item.get("canonical")
                source_rule = item.get("source")

                confidence = item.get("confidence")
                try:
                    confidence = float(confidence) if confidence is not None else None
                except Exception:
                    confidence = None

                context_snippets = _extract_context_snippets(search_text, raw_text)

                # 혹시 raw_text가 본문에서 안 잡히면 canonical도 한번 시도
                if not context_snippets and canonical and str(canonical).strip() != raw_text:
                    context_snippets = _extract_context_snippets(search_text, str(canonical).strip())

                upsert_candidate(
                    cur=cur,
                    candidate_type=candidate_type,
                    raw_text=raw_text,
                    scope=category or "all",
                    suggested_canonical=canonical,
                    source_stage=source_stage,
                    source_rule=source_rule,
                    confidence=confidence,
                    sample_doc_id=doc_id,
                    sample_title=title,
                    sample_snippets=context_snippets,
                )

        debug = additional.get("_debug") or {}
        acronyms = debug.get("acronym_candidates") or []
        if isinstance(acronyms, list):
            for raw_text in acronyms:
                raw_text = str(raw_text).strip()
                if not raw_text:
                    continue

                context_snippets = _extract_context_snippets(search_text, raw_text)

                upsert_candidate(
                    cur=cur,
                    candidate_type="acronym",
                    raw_text=raw_text,
                    scope=category or "all",
                    suggested_canonical=None,
                    source_stage=source_stage,
                    source_rule="debug_acronym_candidates",
                    confidence=0.3,
                    sample_doc_id=doc_id,
                    sample_title=title,
                    sample_snippets=context_snippets,
                )

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()