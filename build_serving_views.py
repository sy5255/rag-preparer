#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
from typing import Any, Dict, List, Tuple

from normalize_docs import (
    get_category_from_path,
    get_version_from_path,
    compute_out_paths_3,
    build_base_doc,
)
from enrich_case_metadata import (
    build_additional_lite,
    build_additional_full,
    deep_merge_dict,
)
from candidate_queue import collect_candidates_from_additional


class LLMEnrichmentError(Exception):
    """FULL 메타데이터 생성(LLM) 실패. strict 모드에서만 발생합니다."""


def build_doc_views(
    doc: Dict[str, Any],
    category: str,
    version_tag: str,
    *,
    strict_llm: bool = False,
) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any], bool]:
    """
    입력 문서 1건 → (raw_doc, full_doc, lite_doc, degraded)

    - raw: base_doc만 유지
    - full: base_doc.additionalField + full enrichment merge
    - lite: base_doc.additionalField + lite enrichment merge (+ FULL summary)
    - strict_llm=True 이면 LLM 실패 시 LLMEnrichmentError를 올립니다.
      False이면 기존처럼 lite 결과로 대체하고 degraded=True를 반환합니다.
    """
    base_doc = build_base_doc(doc, version_tag)
    raw_doc = json.loads(json.dumps(base_doc, ensure_ascii=False))

    title = base_doc.get("title", "") or ""
    content = base_doc.get("content", "") or ""

    # ---------- FULL ----------
    degraded = False
    try:
        additional_full = build_additional_full(title, content, category)
    except Exception as e:
        if strict_llm:
            raise LLMEnrichmentError(f"doc_id={doc.get('doc_id')} {e!r}") from e
        degraded = True
        additional_full = build_additional_lite(title, content, category=category)
        if isinstance(additional_full, dict):
            additional_full["doc_type"] = "unknown"
            additional_full["_debug"] = {"llm_error": str(e)}

    if isinstance(additional_full, dict):
        additional_full.pop("owner", None)

    doc_full = dict(base_doc)
    orig_af_full = doc_full.get("additionalField", {})
    if not isinstance(orig_af_full, dict):
        orig_af_full = {}

    merged_af_full = deep_merge_dict(orig_af_full, additional_full)
    merged_af_full["version_tag"] = version_tag
    doc_full["additionalField"] = merged_af_full

    # FULL summary를 LITE에 복사
    full_summary = merged_af_full.get("summary") if isinstance(merged_af_full, dict) else None

    # ---------- LITE ----------
    additional_lite = build_additional_lite(title, content, category=category)
    if isinstance(additional_lite, dict):
        additional_lite.pop("owner", None)

    doc_lite = dict(base_doc)
    orig_af_lite = doc_lite.get("additionalField", {})
    if not isinstance(orig_af_lite, dict):
        orig_af_lite = {}

    merged_af_lite = deep_merge_dict(orig_af_lite, additional_lite)

    if full_summary:
        merged_af_lite["summary"] = full_summary

    merged_af_lite["version_tag"] = version_tag
    doc_lite["additionalField"] = merged_af_lite

    return raw_doc, doc_full, doc_lite, degraded


def preprocess_jsonl_file(in_path: str) -> Tuple[str, str, str]:
    """
    (기존 upload_indices.py 경로용. 신규 운영은 run_pipeline.py를 사용합니다.)
    Returns (raw_out, full_out, lite_out)

    동작:
    - input jsonl -> base_doc(title/content/mail_meta 정제)
    - raw/full/lite 3종 생성
    - FULL 결과 기준으로 term_candidate_queue에 후보 upsert
    """
    category = get_category_from_path(in_path)
    version_tag = get_version_from_path(in_path)

    raw_out, full_out, lite_out = compute_out_paths_3(in_path)

    # 기존 정책 유지: 이미 세 파일이 있으면 재생성 안 함
    # (파일 변경 감지는 upload_indices.py에서 in_path signature로 관리)
    if os.path.exists(raw_out) and os.path.exists(full_out) and os.path.exists(lite_out):
        return raw_out, full_out, lite_out

    os.makedirs(os.path.dirname(full_out), exist_ok=True)

    raw_lines: List[str] = []
    full_lines: List[str] = []
    lite_lines: List[str] = []

    with open(in_path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue

            doc = json.loads(line)
            raw_doc, doc_full, doc_lite, _ = build_doc_views(doc, category, version_tag)

            raw_lines.append(json.dumps(raw_doc, ensure_ascii=False))
            full_lines.append(json.dumps(doc_full, ensure_ascii=False))
            lite_lines.append(json.dumps(doc_lite, ensure_ascii=False))

            # ✅ FULL 결과 기준으로 후보 적재
            try:
                collect_candidates_from_additional(
                    doc=doc_full,
                    additional=doc_full.get("additionalField") or {},
                    category=category,
                    source_stage="build_serving_views_full",
                )
            except Exception as e:
                # 후보 큐 적재 실패가 전체 파이프라인을 죽이지 않게 로그만 남김
                print(f"[candidate-queue-fail] doc_id={doc_full.get('doc_id')} err={e}")

    # 임시 파일에 쓴 뒤 rename → 중간에 끊겨도 "잘린 파일이 존재"하는 상태가 생기지 않음
    _atomic_write_lines(raw_out, raw_lines)
    _atomic_write_lines(lite_out, lite_lines)
    _atomic_write_lines(full_out, full_lines)

    return raw_out, full_out, lite_out


def _atomic_write_lines(path: str, lines: List[str]) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + ("\n" if lines else ""))
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)