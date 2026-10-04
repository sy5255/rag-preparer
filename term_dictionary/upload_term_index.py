#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import time
import hashlib
from typing import Dict, Any, List, Set

import mysql.connector
import requests

# =========================
# MYSQL SETTINGS
# =========================
MYSQL_HOST = os.getenv("MYSQL_HOST", "10.172.127.210")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DB = os.getenv("MYSQL_DB", "fspas")
MYSQL_USER = os.getenv("MYSQL_USER", "dbuser")
MYSQL_PASS = os.getenv("MYSQL_PASS", "separt123!")

# =========================
# OUTPUT / STATE
# =========================
OUT_ROOT = os.getenv("TERM_OUT_ROOT", "/config/work/sharedworkspace/preprocessed_terms")
TERM_JSONL_PATH = os.path.join(OUT_ROOT, "terms__full.jsonl")
STATE_FILE = os.path.join(OUT_ROOT, "_state_terms.json")
POLL_INTERVAL_SEC = float(os.getenv("TERM_POLL_INTERVAL_SEC", "300"))

# =========================
# RAG SETTINGS
# =========================
RAG_URL = os.getenv("TERM_RAG_URL", "http://aw.ss.net:8000/llm_rag/2/llmrag/elastic/v2/insert-doc")
DELETE_URL = os.getenv("TERM_RAG_DELETE_URL", "http://aw.ss.net:8000/llm_rag/2/llmrag/elastic/v2/delete-doc")

PASS_KEY = os.getenv("TERM_PASS_KEY", "crew==")   # API HUB key
RAG_KEY = os.getenv("TERM_RAG_KEY", "rag-cn2")
TERM_INDEX_NAME = os.getenv("TERM_INDEX_NAME", "rp-term-ver1")

RAG_HEADERS = {
    "Content-Type": "application/json",
    "x-dep-ticket": PASS_KEY,
    "api-key": RAG_KEY,
}

DEFAULT_PERMISSION_GROUPS = ["rag-public"]

CHUNK_FACTOR = {
    "logic": "fixed_size",
    "chunk_size": 100,
    "chunk_overlap": 50,
    "separator": " "
}

# =========================
# HELPERS
# =========================
def get_mysql_conn():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        database=MYSQL_DB,
        user=MYSQL_USER,
        password=MYSQL_PASS,
        autocommit=True,
    )

def ensure_dirs():
    os.makedirs(OUT_ROOT, exist_ok=True)

def now_kst_iso() -> str:
    from datetime import datetime, timezone, timedelta
    kst = timezone(timedelta(hours=9))
    return datetime.now(kst).isoformat(timespec="milliseconds")

def normalize_alias(s: str) -> str:
    s = str(s or "")
    s = s.replace("\u00A0", " ")
    s = s.lower()
    s = s.replace("-", " ").replace("_", " ").replace("/", " ")
    import re
    s = re.sub(r"\s+", " ", s).strip()
    return s

def stable_json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def compute_payload_hash(payload: Dict[str, Any]) -> str:
    return sha256_text(stable_json_dumps(payload))

def load_state() -> Dict[str, Any]:
    ensure_dirs()
    if not os.path.exists(STATE_FILE):
        return {
            "uploaded_docs": {},
            "content_hash_by_doc": {},
            "known_doc_ids": {},
        }
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        data.setdefault("uploaded_docs", {})
        data.setdefault("content_hash_by_doc", {})
        data.setdefault("known_doc_ids", {})
        return data
    except Exception:
        return {
            "uploaded_docs": {},
            "content_hash_by_doc": {},
            "known_doc_ids": {},
        }

def save_state(state: Dict[str, Any]) -> None:
    ensure_dirs()
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, STATE_FILE)

# =========================
# MYSQL READ
# =========================
def fetch_term_rows() -> List[Dict[str, Any]]:
    sql = """
    SELECT
        td.term_id,
        td.term_type,
        td.canonical_name,
        td.display_name,
        td.description,
        td.scope,
        td.status,
        td.is_verified,
        td.metadata_json,
        td.updated_at AS term_updated_at,

        ta.alias_id,
        ta.alias_text,
        ta.alias_normalized,
        ta.match_type,
        ta.language_code,
        ta.is_preferred,
        ta.status AS alias_status,
        ta.updated_at AS alias_updated_at
    FROM term_dictionary td
    LEFT JOIN term_aliases ta
      ON td.term_id = ta.term_id
     AND ta.status = 'active'
    WHERE td.status = 'active'
    ORDER BY td.term_type, td.term_id, ta.is_preferred DESC, ta.alias_id
    """
    conn = get_mysql_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(sql)
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()

def group_terms(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_term_id: Dict[int, Dict[str, Any]] = {}

    for r in rows:
        term_id = int(r["term_id"])
        if term_id not in by_term_id:
            meta = {}
            if r.get("metadata_json"):
                try:
                    meta = json.loads(r["metadata_json"])
                except Exception:
                    meta = {}

            by_term_id[term_id] = {
                "term_id": term_id,
                "term_type": r["term_type"],
                "canonical_name": r["canonical_name"],
                "display_name": r["display_name"] or r["canonical_name"],
                "description": r["description"] or "",
                "scope": r["scope"] or "all",
                "status": r["status"],
                "is_verified": bool(r["is_verified"]),
                "metadata": meta,
                "aliases": [],
            }

        alias_text = r.get("alias_text")
        if alias_text:
            by_term_id[term_id]["aliases"].append({
                "alias_id": r.get("alias_id"),
                "alias_text": alias_text,
                "alias_normalized": r.get("alias_normalized") or normalize_alias(alias_text),
                "match_type": r.get("match_type") or "contains",
                "language_code": r.get("language_code"),
                "is_preferred": bool(r.get("is_preferred")),
            })

    return list(by_term_id.values())

# =========================
# JSONL BUILD
# =========================
def build_term_doc(term: Dict[str, Any]) -> Dict[str, Any]:
    term_id = term["term_id"]
    term_type = term["term_type"]
    canonical_name = term["canonical_name"]
    display_name = term["display_name"]
    description = term["description"] or ""
    scope = term["scope"] or "all"
    is_verified = bool(term["is_verified"])
    aliases = term.get("aliases", [])

    alias_texts = []
    seen = set()

    all_aliases = [canonical_name] + [a["alias_text"] for a in aliases if a.get("alias_text")]
    for x in all_aliases:
        x = str(x).strip()
        if not x or x in seen:
            continue
        seen.add(x)
        alias_texts.append(x)

    metadata = term.get("metadata") or {}
    related_keywords = metadata.get("related_keywords") or []
    examples = metadata.get("examples") or []

    content_lines = [
        f"용어 유형: {term_type}",
        f"표준명: {canonical_name}",
        f"표시명: {display_name}",
        f"적용 범위: {scope}",
        f"검증 여부: {'verified' if is_verified else 'provisional'}",
    ]

    if alias_texts:
        content_lines.append("별칭: " + ", ".join(alias_texts))
    if description:
        content_lines.append("설명: " + description)
    if related_keywords:
        content_lines.append("관련 키워드: " + ", ".join(map(str, related_keywords)))
    if examples:
        content_lines.append("예시: " + " | ".join(map(str, examples[:5])))

    content = "\n".join(content_lines)

    return {
        "doc_id": f"term:{term_type}:{term_id}",
        "title": canonical_name,
        "content": content,
        "permission_groups": DEFAULT_PERMISSION_GROUPS,
        "created_time": now_kst_iso(),
        "additionalField": {
            "term_id": term_id,
            "term_type": term_type,
            "canonical_name": canonical_name,
            "display_name": display_name,
            "aliases": alias_texts,
            "scope": scope,
            "status": "active",
            "is_verified": is_verified,
            "description": description,
            "metadata": metadata,
        }
    }

def write_term_jsonl(docs: List[Dict[str, Any]]) -> None:
    ensure_dirs()
    with open(TERM_JSONL_PATH, "w", encoding="utf-8") as f:
        for d in docs:
            f.write(json.dumps(d, ensure_ascii=False) + "\n")

# =========================
# UPLOAD / DELETE
# =========================
def build_upload_payload(doc: Dict[str, Any]) -> Dict[str, Any]:
    data_obj = {
        "doc_id": doc["doc_id"],
        "title": doc.get("title", ""),
        "content": doc.get("content", ""),
        "permission_groups": doc.get("permission_groups", DEFAULT_PERMISSION_GROUPS),
        "created_time": doc.get("created_time") or now_kst_iso(),
        "additionalField": doc.get("additionalField", {}),
    }
    return {
        "index_name": TERM_INDEX_NAME,
        "data": data_obj,
        "chunk_factor": CHUNK_FACTOR
    }

def build_delete_payload(doc_id: str) -> Dict[str, Any]:
    return {
        "index_name": TERM_INDEX_NAME,
        "permission_groups": DEFAULT_PERMISSION_GROUPS,
        "doc_id": doc_id,
    }

def upload_term_docs(docs: List[Dict[str, Any]], state: Dict[str, Any]) -> Set[str]:
    uploaded = state.setdefault("uploaded_docs", {})
    content_hash_by_doc = state.setdefault("content_hash_by_doc", {})
    known_doc_ids = state.setdefault("known_doc_ids", {})

    current_keys: Set[str] = set()

    for doc in docs:
        doc_id = doc["doc_id"]
        key = f"{TERM_INDEX_NAME}::{doc_id}"
        current_keys.add(key)

        payload = build_upload_payload(doc)
        payload_hash = compute_payload_hash(payload)

        # 동일 index::doc_id + 동일 payload면 skip
        if uploaded.get(key) is True and content_hash_by_doc.get(key) == payload_hash:
            known_doc_ids[key] = True
            continue

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
                known_doc_ids[key] = True
                # 문서 단위로 즉시 저장 → 중간에 끊겨도 이미 올린 문서는 다시 올리지 않음
                save_state(state)
                print(f"[uploaded] {key}")
            else:
                print(f"[upload-fail] {key} status={resp.status_code} body={resp.text[:500]}")
        except Exception as e:
            print(f"[upload-error] {key} err={e}")

    return current_keys

def delete_stale_term_docs(current_keys: Set[str], state: Dict[str, Any]) -> None:
    uploaded = state.setdefault("uploaded_docs", {})
    content_hash_by_doc = state.setdefault("content_hash_by_doc", {})
    known_doc_ids = state.setdefault("known_doc_ids", {})

    prev_keys = set(known_doc_ids.keys())
    stale_keys = prev_keys - current_keys

    for key in sorted(stale_keys):
        try:
            _, doc_id = key.split("::", 1)
        except ValueError:
            continue

        payload = build_delete_payload(doc_id)

        try:
            resp = requests.post(
                DELETE_URL,
                headers=RAG_HEADERS,
                data=json.dumps(payload, ensure_ascii=False),
                timeout=30
            )
            if 200 <= resp.status_code < 300:
                print(f"[deleted] {key}")
                uploaded.pop(key, None)
                content_hash_by_doc.pop(key, None)
                known_doc_ids.pop(key, None)
                save_state(state)
            else:
                print(f"[delete-fail] {key} status={resp.status_code} body={resp.text[:500]}")
        except Exception as e:
            print(f"[delete-error] {key} err={e}")

# =========================
# MAIN SYNC
# =========================
def sync_once():
    rows = fetch_term_rows()
    terms = group_terms(rows)
    docs = [build_term_doc(t) for t in terms]
    write_term_jsonl(docs)

    state = load_state()

    current_keys = upload_term_docs(docs, state)
    delete_stale_term_docs(current_keys, state)

    save_state(state)
    print(f"[done] terms={len(terms)} docs={len(docs)} index={TERM_INDEX_NAME}")

def main():
    ensure_dirs()
    print(f"[term-sync] out={OUT_ROOT}")
    print(f"[term-sync] jsonl={TERM_JSONL_PATH}")
    print(f"[term-sync] index={TERM_INDEX_NAME}")

    while True:
        try:
            sync_once()
        except Exception as e:
            print(f"[term-sync-error] {e}")

        time.sleep(POLL_INTERVAL_SEC)

if __name__ == "__main__":
    main() 