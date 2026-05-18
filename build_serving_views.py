#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
from typing import List, Tuple

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


def preprocess_jsonl_file(in_path: str) -> Tuple[str, str, str]:
    """
    Returns (raw_out, full_out, lite_out)

    동작:
    - input jsonl -> base_doc(title/content/mail_meta 정제)
    - raw/full/lite 3종 생성
    - raw: base_doc만 유지
    - full: base_doc.additionalField + full enrichment merge
    - lite: base_doc.additionalField + lite enrichment merge

    추가:
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
            base_doc = build_base_doc(doc, version_tag)

            title = base_doc.get("title", "") or ""
            content = base_doc.get("content", "") or ""

            # ---------- RAW ----------
            raw_lines.append(json.dumps(base_doc, ensure_ascii=False))

            # ---------- FULL ----------
            try:
                additional_full = build_additional_full(title, content, category)
            except Exception as e:
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
            full_lines.append(json.dumps(doc_full, ensure_ascii=False))

            # ✅ FULL 결과 기준으로 후보 적재
            try:
                collect_candidates_from_additional(
                    doc=doc_full,
                    additional=merged_af_full,
                    category=category,
                    source_stage="build_serving_views_full",
                )
            except Exception as e:
                # 후보 큐 적재 실패가 전체 파이프라인을 죽이지 않게 로그만 남김
                print(f"[candidate-queue-fail] doc_id={doc_full.get('doc_id')} err={e}")

            # FULL summary를 LITE에 복사
            full_summary = None
            try:
                af_full = doc_full.get("additionalField", {})
                if isinstance(af_full, dict):
                    full_summary = af_full.get("summary")
            except Exception:
                pass

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
            lite_lines.append(json.dumps(doc_lite, ensure_ascii=False))

    with open(raw_out, "w", encoding="utf-8") as f:
        f.write("\n".join(raw_lines) + ("\n" if raw_lines else ""))

    with open(lite_out, "w", encoding="utf-8") as f:
        f.write("\n".join(lite_lines) + ("\n" if lite_lines else ""))

    with open(full_out, "w", encoding="utf-8") as f:
        f.write("\n".join(full_lines) + ("\n" if full_lines else ""))

    return raw_out, full_out, lite_out