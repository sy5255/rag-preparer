#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import time
import re
from typing import Any, Dict, List, Optional

import mysql.connector


# =========================
# MYSQL SETTINGS
# =========================
MYSQL_HOST = os.getenv("MYSQL_HOST", "10.111.111.111")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "1111"))
MYSQL_DB = os.getenv("MYSQL_DB", "fspas")
MYSQL_USER = os.getenv("MYSQL_USER", "dbuser")
MYSQL_PASS = os.getenv("MYSQL_PASS", "12345")

POLL_INTERVAL_SEC = float(os.getenv("PROMOTE_POLL_INTERVAL_SEC", "10"))
BATCH_SIZE = int(os.getenv("PROMOTE_BATCH_SIZE", "100"))


# =========================
# DB
# =========================
def get_mysql_conn():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        database=MYSQL_DB,
        user=MYSQL_USER,
        password=MYSQL_PASS,
        autocommit=False,
    )


# =========================
# HELPERS
# =========================
def normalize_alias_text(s: str) -> str:
    s = str(s or "")
    s = s.replace("\u00A0", " ")
    s = s.lower()
    s = s.replace("-", " ").replace("_", " ").replace("/", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _safe_json_loads(v: Any, default: Any):
    if v is None:
        return default
    if isinstance(v, (dict, list)):
        return v
    try:
        return json.loads(v)
    except Exception:
        return default


def _dedupe_keep_order(items: List[str], max_n: Optional[int] = None) -> List[str]:
    out = []
    seen = set()
    for x in items or []:
        s = str(x or "").strip()
        if not s:
            continue
        key = s.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(s)
        if max_n is not None and len(out) >= max_n:
            break
    return out


def _pick_final_term_type(row: Dict[str, Any]) -> Optional[str]:
    v = str(row.get("approved_term_type") or "").strip()
    return v or None


def _pick_final_scope(row: Dict[str, Any]) -> str:
    v = str(row.get("approved_scope") or "").strip()
    if v:
        return v
    return str(row.get("scope") or "all").strip() or "all"


def _pick_final_canonical(row: Dict[str, Any]) -> Optional[str]:
    approved = str(row.get("approved_canonical_name") or "").strip()
    if approved:
        return approved

    suggested = str(row.get("suggested_canonical") or "").strip()
    if suggested:
        return suggested

    raw_text = str(row.get("raw_text") or "").strip()
    return raw_text or None


def _pick_final_display_name(row: Dict[str, Any], canonical_name: str) -> str:
    v = str(row.get("approved_display_name") or "").strip()
    return v or canonical_name


def _pick_final_is_verified(row: Dict[str, Any]) -> int:
    v = row.get("approved_is_verified")
    if v is None:
        return 0
    return int(v)


def _pick_final_priority(row: Dict[str, Any]) -> int:
    v = row.get("approved_priority")
    if v is None:
        return 100
    return int(v)


def _pick_final_expand_to_aliases(row: Dict[str, Any]) -> int:
    v = row.get("approved_expand_to_aliases")
    if v is None:
        return 1
    return int(v)


def _pick_final_search_boost(row: Dict[str, Any]) -> float:
    v = row.get("approved_search_boost")
    if v is None:
        return 1.0
    return float(v)

def _build_rule_based_description(
    candidate_type: str,
    canonical_name: str,
    raw_text: str,
    snippets: List[str],
) -> str:
    raw_text = str(raw_text or "").strip()
    canonical_name = str(canonical_name or "").strip()

    if candidate_type == "chemistry":
        base = f"{canonical_name} 관련 chemistry 용어"
    elif candidate_type == "process":
        base = f"{canonical_name} 관련 공정 용어"
    elif candidate_type == "product":
        base = f"{canonical_name} 관련 제품 용어"
    elif candidate_type == "defect":
        base = f"{canonical_name} 관련 defect 용어"
    elif candidate_type == "node":
        base = f"{canonical_name} 관련 node 용어"
    elif candidate_type == "owner":
        base = f"{canonical_name} 관련 담당자 용어"
    elif candidate_type == "acronym":
        base = f"{canonical_name} 관련 약어 후보"
    elif candidate_type == "equipment":
        base = f"{canonical_name} 관련 설비/장비 용어"
    elif candidate_type == "analysis":
        base = f"{canonical_name} 관련 분석 기법 용어"
    else:
        base = f"{canonical_name} 관련 용어"

    if raw_text and raw_text.lower() != canonical_name.lower():
        base += f" (원문 후보: {raw_text})"

    if snippets:
        base += ". IFA 문서 문맥에서 반복 관찰됨."

    return base

def _build_rule_based_metadata(
    candidate_id: int,
    canonical_name: str,
    raw_text: str,
    sample_titles: List[str],
    sample_snippets: List[str],
) -> Dict[str, Any]:
    related_keywords = _dedupe_keep_order([canonical_name, raw_text], max_n=10)

    cleaned_examples = []
    for x in sample_snippets:
        s = str(x or "").strip()
        if not s:
            continue
        s = re.sub(r"\s+", " ", s)
        cleaned_examples.append(s)

    examples = _dedupe_keep_order(cleaned_examples, max_n=5)

    return {
        "related_keywords": related_keywords,
        "examples": examples,
        "sample_titles": _dedupe_keep_order(sample_titles, max_n=5),
        "auto_generated": True,
        "source_candidate_id": candidate_id,
    }


def ensure_drafts_if_missing(row: Dict[str, Any]) -> Dict[str, Any]:
    candidate_id = int(row["candidate_id"])
    candidate_type = str(row.get("candidate_type") or "").strip()
    raw_text = str(row.get("raw_text") or "").strip()
    canonical_name = _pick_final_canonical(row) or raw_text

    sample_titles = _safe_json_loads(row.get("sample_titles_json"), [])
    sample_snippets = _safe_json_loads(row.get("sample_snippets_json"), [])

    draft_description = row.get("draft_description")
    draft_metadata_json = _safe_json_loads(row.get("draft_metadata_json"), None)

    if not draft_description:
        draft_description = _build_rule_based_description(
            candidate_type=candidate_type,
            canonical_name=canonical_name,
            raw_text=raw_text,
            snippets=sample_snippets,
        )

    if not isinstance(draft_metadata_json, dict):
        draft_metadata_json = _build_rule_based_metadata(
            candidate_id=candidate_id,
            canonical_name=canonical_name,
            raw_text=raw_text,
            sample_titles=sample_titles,
            sample_snippets=sample_snippets,
        )

    row = dict(row)
    row["draft_description"] = draft_description
    row["draft_metadata_json_obj"] = draft_metadata_json
    return row


# =========================
# TERM UPSERT
# =========================
def upsert_term(cur, term: Dict[str, Any]) -> int:
    sql_select = """
    SELECT term_id
    FROM term_dictionary
    WHERE term_type = %s
      AND scope = %s
      AND canonical_name = %s
    LIMIT 1
    """
    cur.execute(
        sql_select,
        (
            term["term_type"],
            term.get("scope", "all"),
            term["canonical_name"],
        ),
    )
    row = cur.fetchone()
    metadata_json = json.dumps(term.get("metadata") or {}, ensure_ascii=False)

    if row:
        term_id = int(row[0])
        sql_update = """
        UPDATE term_dictionary
        SET display_name = %s,
            description = %s,
            status = %s,
            is_verified = %s,
            metadata_json = %s,
            priority = %s,
            expand_to_aliases = %s,
            search_boost = %s
        WHERE term_id = %s
        """
        cur.execute(
            sql_update,
            (
                term.get("display_name") or term["canonical_name"],
                term.get("description") or "",
                term.get("status", "active"),
                int(term.get("is_verified", 0)),
                metadata_json,
                int(term.get("priority", 100)),
                int(term.get("expand_to_aliases", 1)),
                float(term.get("search_boost", 1.0)),
                term_id,
            ),
        )
        return term_id

    sql_insert = """
    INSERT INTO term_dictionary
    (
        term_type,
        canonical_name,
        display_name,
        description,
        scope,
        status,
        is_verified,
        metadata_json,
        priority,
        expand_to_aliases,
        search_boost
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cur.execute(
        sql_insert,
        (
            term["term_type"],
            term["canonical_name"],
            term.get("display_name") or term["canonical_name"],
            term.get("description") or "",
            term.get("scope", "all"),
            term.get("status", "active"),
            int(term.get("is_verified", 0)),
            metadata_json,
            int(term.get("priority", 100)),
            int(term.get("expand_to_aliases", 1)),
            float(term.get("search_boost", 1.0)),
        ),
    )
    return int(cur.lastrowid)


def upsert_alias(cur, term_id: int, alias: Dict[str, Any]) -> None:
    alias_text = str(alias["alias_text"]).strip()
    alias_normalized = normalize_alias_text(alias_text)
    if not alias_text or not alias_normalized:
        return

    sql_select = """
    SELECT alias_id
    FROM term_aliases
    WHERE term_id = %s
      AND alias_normalized = %s
    LIMIT 1
    """
    cur.execute(sql_select, (term_id, alias_normalized))
    row = cur.fetchone()

    if row:
        alias_id = int(row[0])
        sql_update = """
        UPDATE term_aliases
        SET alias_text = %s,
            match_type = %s,
            language_code = %s,
            is_preferred = %s,
            status = %s
        WHERE alias_id = %s
        """
        cur.execute(
            sql_update,
            (
                alias_text,
                alias.get("match_type", "contains"),
                alias.get("language_code"),
                int(alias.get("is_preferred", 0)),
                alias.get("status", "active"),
                alias_id,
            ),
        )
        return

    sql_insert = """
    INSERT INTO term_aliases
    (
        term_id,
        alias_text,
        alias_normalized,
        match_type,
        language_code,
        is_preferred,
        status
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    cur.execute(
        sql_insert,
        (
            term_id,
            alias_text,
            alias_normalized,
            alias.get("match_type", "contains"),
            alias.get("language_code"),
            int(alias.get("is_preferred", 0)),
            alias.get("status", "active"),
        ),
    )


# =========================
# QUEUE -> TERM
# =========================
def fetch_approved_candidates(cur) -> List[Dict[str, Any]]:
    sql = f"""
    SELECT *
    FROM term_candidate_queue
    WHERE review_status = 'approved'
      AND promoted_term_id IS NULL
    ORDER BY candidate_id ASC
    LIMIT {BATCH_SIZE}
    """
    cur.execute(sql)
    rows = cur.fetchall()
    columns = [d[0] for d in cur.description]
    return [dict(zip(columns, r)) for r in rows]


def update_drafts(cur, candidate_id: int, draft_description: str, draft_metadata: Dict[str, Any]) -> None:
    sql = """
    UPDATE term_candidate_queue
    SET draft_description = %s,
        draft_metadata_json = %s
    WHERE candidate_id = %s
    """
    cur.execute(
        sql,
        (
            draft_description,
            json.dumps(draft_metadata, ensure_ascii=False),
            candidate_id,
        ),
    )


def mark_promoted(cur, candidate_id: int, term_id: int) -> None:
    sql = """
    UPDATE term_candidate_queue
    SET review_status = 'promoted',
        promoted_term_id = %s,
        promoted_at = CURRENT_TIMESTAMP
    WHERE candidate_id = %s
    """
    cur.execute(sql, (term_id, candidate_id))


def ensure_promote_error_column(conn) -> None:
    """승격 실패 원인을 남길 promote_error 컬럼이 없으면 추가합니다."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = 'term_candidate_queue'
              AND column_name = 'promote_error'
            LIMIT 1
            """
        )
        if cur.fetchone() is None:
            cur.execute("ALTER TABLE term_candidate_queue ADD COLUMN promote_error TEXT NULL")
        conn.commit()
    finally:
        cur.close()


def mark_promote_failed(cur, candidate_id: int, error: str) -> None:
    """
    승격 중 예외가 난 후보만 promote_failed로 표시합니다.
    원인을 고친 뒤 review_status='approved'로 되돌리면 다시 승격됩니다.
    """
    sql = """
    UPDATE term_candidate_queue
    SET review_status = 'promote_failed',
        promote_error = %s
    WHERE candidate_id = %s
    """
    cur.execute(sql, ((error or "")[:4000], candidate_id))


def mark_needs_edit(cur, candidate_id: int) -> None:
    sql = """
    UPDATE term_candidate_queue
    SET review_status = 'needs_edit'
    WHERE candidate_id = %s
    """
    cur.execute(sql, (candidate_id,))


def build_term_payload_from_candidate(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    term_type = _pick_final_term_type(row)
    canonical_name = _pick_final_canonical(row)

    if not term_type or not canonical_name:
        return None

    display_name = _pick_final_display_name(row, canonical_name)
    scope = _pick_final_scope(row)
    is_verified = _pick_final_is_verified(row)
    priority = _pick_final_priority(row)
    expand_to_aliases = _pick_final_expand_to_aliases(row)
    search_boost = _pick_final_search_boost(row)

    approved_description = str(row.get("approved_description") or "").strip()
    if approved_description:
        description = approved_description
    else:
        description = str(row.get("draft_description") or "").strip()

    approved_metadata = _safe_json_loads(row.get("approved_metadata_json"), None)
    if isinstance(approved_metadata, dict):
        metadata = approved_metadata
    else:
        metadata = row.get("draft_metadata_json_obj") or {}

    return {
        "term_type": term_type,
        "canonical_name": canonical_name,
        "display_name": display_name,
        "description": description,
        "scope": scope,
        "status": "active",
        "is_verified": is_verified,
        "priority": priority,
        "expand_to_aliases": expand_to_aliases,
        "search_boost": search_boost,
        "metadata": metadata,
    }


def build_aliases_from_candidate(row: Dict[str, Any], canonical_name: str) -> List[Dict[str, Any]]:
    raw_text = str(row.get("raw_text") or "").strip()

    proposed_aliases = _safe_json_loads(row.get("proposed_aliases_json"), [])
    if not isinstance(proposed_aliases, list):
        proposed_aliases = []

    alias_candidates = _dedupe_keep_order(
        [canonical_name, raw_text] + [str(x) for x in proposed_aliases]
    )

    out = []
    for alias_text in alias_candidates:
        out.append({
            "alias_text": alias_text,
            "match_type": "contains",
            "language_code": None,
            "is_preferred": 1 if alias_text == canonical_name else 0,
            "status": "active",
        })

    return out

def promote_alias_for_existing_term(cur, row: Dict[str, Any]) -> Optional[int]:
    """
    기존 term_dictionary row에는 손대지 않고,
    target_term_id에 alias만 추가하는 승인 처리.
    """
    target_term_id = row.get("target_term_id")
    if target_term_id is None:
        return None

    try:
        target_term_id = int(target_term_id)
    except Exception:
        return None

    raw_text = str(row.get("raw_text") or "").strip()

    proposed_aliases = _safe_json_loads(row.get("proposed_aliases_json"), [])
    if not isinstance(proposed_aliases, list):
        proposed_aliases = []

    alias_candidates = _dedupe_keep_order(
        [raw_text] + [str(x) for x in proposed_aliases]
    )

    if not alias_candidates:
        return None

    for alias_text in alias_candidates:
        upsert_alias(cur, target_term_id, {
            "alias_text": alias_text,
            "match_type": "contains",
            "language_code": None,
            "is_preferred": 0,
            "status": "active",
        })

    return target_term_id

def promote_once() -> int:
    conn = get_mysql_conn()
    ensure_promote_error_column(conn)
    cur = conn.cursor()

    try:
        rows = fetch_approved_candidates(cur)
        if not rows:
            conn.commit()
            return 0

        promoted_count = 0

        for row in rows:
            candidate_id = int(row["candidate_id"])

            # 행 단위 SAVEPOINT: 한 후보의 실패가 같은 배치의 다른 후보 승격을 막지 않게 함
            cur.execute("SAVEPOINT promote_candidate")
            try:
                if _promote_row(cur, row, candidate_id):
                    promoted_count += 1
                cur.execute("RELEASE SAVEPOINT promote_candidate")
            except Exception as e:
                cur.execute("ROLLBACK TO SAVEPOINT promote_candidate")
                mark_promote_failed(cur, candidate_id, repr(e))
                print(f"[promote-failed] candidate_id={candidate_id} err={e!r}")

        conn.commit()
        return promoted_count

    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def _promote_row(cur, row: Dict[str, Any], candidate_id: int) -> bool:
    """후보 1건을 승격합니다. 승격하면 True, needs_edit 처리하면 False."""
    row = ensure_drafts_if_missing(row)
    update_drafts(
        cur=cur,
        candidate_id=candidate_id,
        draft_description=row["draft_description"],
        draft_metadata=row["draft_metadata_json_obj"],
    )

    candidate_kind = str(row.get("candidate_kind") or "new_term").strip()

    if candidate_kind == "alias_for_existing_term":
        term_id = promote_alias_for_existing_term(cur, row)

        if not term_id:
            mark_needs_edit(cur, candidate_id)
            return False

        mark_promoted(cur, candidate_id, term_id)
        print(
            f"[promoted-alias] candidate_id={candidate_id} -> "
            f"term_id={term_id}"
        )
        return True

    # 기본값: 신규 용어 승격
    term_payload = build_term_payload_from_candidate(row)
    if not term_payload:
        mark_needs_edit(cur, candidate_id)
        return False

    term_id = upsert_term(cur, term_payload)

    aliases = build_aliases_from_candidate(
        row=row,
        canonical_name=term_payload["canonical_name"],
    )
    for alias in aliases:
        upsert_alias(cur, term_id, alias)

    mark_promoted(cur, candidate_id, term_id)
    print(
        f"[promoted] candidate_id={candidate_id} -> "
        f"term_id={term_id} type={term_payload['term_type']} "
        f"canonical={term_payload['canonical_name']}"
    )
    return True


def main():
    print("[promote-candidates] started")
    while True:
        try:
            n = promote_once()
            if n == 0:
                print("[promote-candidates] no approved candidates")
        except Exception as e:
            print(f"[promote-candidates-error] {e}")

        time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    main()