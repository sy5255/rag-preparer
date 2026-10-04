#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import time
import json
import hashlib
import requests
from typing import Dict, Any

from normalize_docs import WATCH_ROOT, OUT_ROOT, get_category_from_path, get_version_from_path
from build_serving_views import preprocess_jsonl_file

# =========================
# WATCH / STATE
# =========================
POLL_INTERVAL_SEC = 3.0
STATE_FILE = os.path.join(OUT_ROOT, "_state_processed.json")

# =========================
# RAG UPLOAD SETTINGS
# =========================
RAG_URL = "http://aw.ss.net:8000/llm_rag/2/llmrag/elastic/v2/insert-doc" 
PASS_KEY = "crew=="  
RAG_KEY = "rag-cn2"    

RAG_HEADERS = {
    "Content-Type": "application/json",
    "x-dep-ticket": PASS_KEY,
    "api-key": RAG_KEY,
}

CHUNK_FACTOR = {
    "logic": "fixed_size",
    "chunk_size": 100,
    "chunk_overlap": 50,
    "separator": " "
}

INDEX_BASE_MAP = {
    "inline_fa_report": "rp-ifa",
    "other": "rp-other-temp",
}

# =========================
# HELPERS
# =========================
def now_kst_iso() -> str:
    from datetime import datetime, timezone, timedelta
    kst = timezone(timedelta(hours=9))
    return datetime.now(kst).isoformat(timespec="milliseconds")

def category_to_base_index(category: str) -> str:
    if category in INDEX_BASE_MAP:
        return INDEX_BASE_MAP[category]
    return INDEX_BASE_MAP["other"]

def _empty_state() -> Dict[str, Any]:
    return {
        "processed_inputs": {},
        "uploaded_docs": {},
        "content_hash_by_doc": {},
        # 업로드 실패 문서: key=index::doc_id
        "failed_docs": {},
        # 파일 단위 처리 실패: key=입력 jsonl 경로
        "failed_inputs": {},
    }

def load_state() -> Dict[str, Any]:
    os.makedirs(OUT_ROOT, exist_ok=True)
    if not os.path.exists(STATE_FILE):
        return _empty_state()
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            st = json.load(f)
            for k, v in _empty_state().items():
                st.setdefault(k, v)
            return st
    except Exception:
        return _empty_state()

def save_state(state: Dict[str, Any]) -> None:
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())

    os.replace(tmp, STATE_FILE)

def _record_failure(bucket: Dict[str, Any], key: str, error: str, **extra: Any) -> None:
    prev = bucket.get(key) or {}
    bucket[key] = {
        "attempts": int(prev.get("attempts") or 0) + 1,
        "first_failed_at": prev.get("first_failed_at") or now_kst_iso(),
        "last_failed_at": now_kst_iso(),
        "last_error": (error or "")[:2000],
        **extra,
    }

def get_latest_version_dir(category_dir: str) -> str:
    max_ver = -1
    latest_dir = None

    for name in os.listdir(category_dir):
        full_path = os.path.join(category_dir, name)

        if not os.path.isdir(full_path):
            continue

        match = re.match(r"ver(\d+)$", name.lower())
        if match:
            num = int(match.group(1))
            if num > max_ver:
                max_ver = num
                latest_dir = full_path

    return latest_dir

def iter_jsonl_files(root: str):
    for category_name in os.listdir(root):
        category_dir = os.path.join(root, category_name)

        if not os.path.isdir(category_dir):
            continue

        # _state 같은 관리 폴더 제외
        if category_name.startswith("_"):
            continue

        latest_version_dir = get_latest_version_dir(category_dir)

        if latest_version_dir is None:
            continue

        for dirpath, _, filenames in os.walk(latest_version_dir):
            for fn in filenames:
                if fn.lower().endswith(".jsonl"):
                    yield os.path.join(dirpath, fn)

def file_signature(path: str) -> str:
    st = os.stat(path)
    return f"{st.st_size}:{int(st.st_mtime)}"

def stable_json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

def compute_payload_hash(payload: Dict[str, Any]) -> str:
    return hashlib.sha256(stable_json_dumps(payload).encode("utf-8")).hexdigest()

# =========================
# PAYLOAD BUILD
# =========================
def build_upload_data_obj(doc: Dict[str, Any], mode: str) -> Dict[str, Any]:
    created_time = doc.get("created_time") or now_kst_iso()

    data_obj = {
        "doc_id": doc.get("doc_id"),
        "title": doc.get("title", ""),
        "content": doc.get("content", ""),
        "permission_groups": doc.get("permission_groups", ["rag-public"]),
        "created_time": created_time,
    }

    if mode == "raw":
        af = doc.get("additionalField", {})
        if isinstance(af, dict):
            minimal = {}

            if "assets" in af:
                minimal["assets"] = af.get("assets")
            if "version_tag" in af:
                minimal["version_tag"] = af.get("version_tag")
            if "storage" in af:
                minimal["storage"] = af.get("storage")
            if "mail_from" in af:
                minimal["mail_from"] = af.get("mail_from")
            if "mail_date" in af:
                minimal["mail_date"] = af.get("mail_date")
            if "mail_date_iso" in af:
                minimal["mail_date_iso"] = af.get("mail_date_iso")

            if minimal:
                data_obj["additionalField"] = minimal
    else:
        data_obj["additionalField"] = doc.get("additionalField", {})

    return data_obj

# =========================
# UPLOAD
# =========================
def upload_jsonl_to_index(jsonl_path: str, index_name: str, state: Dict[str, Any], mode: str) -> int:
    """
    jsonl의 문서를 index에 업로드하고 실패 건수를 반환합니다.
    - 성공한 문서는 즉시 상태 파일에 저장(중단 시 재업로드 최소화)
    - 실패한 문서는 failed_docs에 기록
    """
    uploaded = state.setdefault("uploaded_docs", {})
    content_hash_by_doc = state.setdefault("content_hash_by_doc", {})
    failed_docs = state.setdefault("failed_docs", {})
    failed_count = 0

    with open(jsonl_path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue

            doc = json.loads(line)
            doc_id = doc.get("doc_id")

            if not doc_id:
                print(f"[upload-skip] missing doc_id in {jsonl_path}")
                _record_failure(
                    state.setdefault("failed_inputs", {}),
                    f"{jsonl_path}::missing_doc_id",
                    "document without doc_id",
                    index=index_name,
                )
                # 재시도해도 성공할 수 없으므로 파일 완료를 막지는 않음(기록만 남김)
                continue

            data_obj = build_upload_data_obj(doc, mode=mode)
            payload = {
                "index_name": index_name,
                "data": data_obj,
                "chunk_factor": CHUNK_FACTOR
            }

            key = f"{index_name}::{doc_id}"
            # created_time이 문서에 없으면 업로드 시각으로 채워지므로 hash에서 제외
            # (포함하면 매 실행마다 hash가 달라져 이미 올린 문서를 계속 재업로드함)
            hash_payload = dict(payload, data=dict(data_obj, created_time=doc.get("created_time")))
            payload_hash = compute_payload_hash(hash_payload)

            # ✅ 동일 doc_id + 동일 payload면 업로드 skip
            if uploaded.get(key) is True and content_hash_by_doc.get(key) == payload_hash:
                if key in failed_docs:
                    failed_docs.pop(key, None)
                    save_state(state)
                continue

            error = None
            try:
                resp = requests.post(
                    RAG_URL,
                    headers=RAG_HEADERS,
                    data=json.dumps(payload, ensure_ascii=False),
                    timeout=30
                )
                if 200 <= resp.status_code < 300:
                    uploaded[key] = True
                    content_hash_by_doc[key] = payload_hash
                    failed_docs.pop(key, None)
                    save_state(state)
                else:
                    error = f"status={resp.status_code} body={resp.text[:500]}"
                    print(f"[upload-fail] index={index_name} doc_id={doc_id} {error}")
            except Exception as e:
                error = repr(e)
                print(f"[upload-error] index={index_name} doc_id={doc_id} err={e}")

            if error is not None:
                failed_count += 1
                _record_failure(failed_docs, key, error, source_jsonl=jsonl_path)
                save_state(state)

    return failed_count

def upload_raw_full_lite_outputs(
    raw_out: str,
    full_out: str,
    lite_out: str,
    category: str,
    version_tag: str,
    state: Dict[str, Any]
) -> int:
    """업로드 실패 문서 수를 반환합니다."""
    base = category_to_base_index(category)

    raw_index = f"{base}-{version_tag}-raw"
    full_index = f"{base}-{version_tag}-full"
    lite_index = f"{base}-{version_tag}-lite"

    print(f"[upload] category={category}, version={version_tag} -> raw={raw_index}, full={full_index}, lite={lite_index}")

    failed = 0
    failed += upload_jsonl_to_index(raw_out, raw_index, state, mode="raw")
    failed += upload_jsonl_to_index(full_out, full_index, state, mode="full")
    failed += upload_jsonl_to_index(lite_out, lite_index, state, mode="lite")
    return failed

# =========================
# MAIN LOOP
# =========================
def main():
    state = load_state()
    processed_inputs = state.get("processed_inputs", {})

    print(f"[watch] root={WATCH_ROOT}")
    print(f"[out]   root={OUT_ROOT}")
    os.makedirs(OUT_ROOT, exist_ok=True)

    while True:
        try:
            for fp in iter_jsonl_files(WATCH_ROOT):
                sig = file_signature(fp)
                prev = processed_inputs.get(fp)
                if prev == sig:
                    continue

                time.sleep(0.2)
                sig2 = file_signature(fp)
                if sig2 != sig:
                    continue

                category = get_category_from_path(fp)
                version_tag = get_version_from_path(fp)
                print(f"[new] {fp} (category={category}, version={version_tag})")

                try:
                    raw_out, full_out, lite_out = preprocess_jsonl_file(fp)
                    print(f"[preprocess-done] raw={raw_out}")
                    print(f"[preprocess-done] full={full_out}")
                    print(f"[preprocess-done] lite={lite_out}")

                    failed = upload_raw_full_lite_outputs(raw_out, full_out, lite_out, category, version_tag, state)

                    if failed:
                        # 실패 문서가 남아 있으면 완료로 기록하지 않음 → 다음 주기에 다시 시도
                        # (이미 성공한 문서는 uploaded_docs/payload hash로 skip됨)
                        _record_failure(
                            state.setdefault("failed_inputs", {}),
                            fp,
                            f"{failed} document upload(s) failed",
                            signature=sig2,
                        )
                        save_state(state)
                        print(f"[partial-fail] {fp} failed_docs={failed} (will retry)")
                        continue

                    processed_inputs[fp] = sig2
                    state["processed_inputs"] = processed_inputs
                    state.setdefault("failed_inputs", {}).pop(fp, None)
                    save_state(state)
                    print(f"[done] preprocess+upload: {fp}")

                except Exception as e:
                    _record_failure(
                        state.setdefault("failed_inputs", {}),
                        fp,
                        repr(e),
                        signature=sig2,
                    )
                    save_state(state)
                    print(f"[fail] {fp} -> {e}")

        except Exception as loop_e:
            print(f"[loop_error] {loop_e}")

        time.sleep(POLL_INTERVAL_SEC)

if __name__ == "__main__":
    main()