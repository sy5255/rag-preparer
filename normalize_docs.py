#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime

# =========================
# PATH SETTINGS
# =========================
WATCH_ROOT = "/config/work/sharedworkspace/parsing_archive"
OUT_ROOT = "/config/work/sharedworkspace/preprocessed_jsonl"

# =========================
# REGEX
# =========================
ORIGINAL_MESSAGE_RE = re.compile(
    r"-{5,}\s*\*\*Original Message\*\*\s*-{5,}",
    re.IGNORECASE
)

TO_BLOCK_RE = re.compile(r"(?is)(?:^|\n)\s*(?:\*\*)?To:(?:\*\*)?\s*.*?\n\s*\n")
CC_BLOCK_RE = re.compile(r"(?is)(?:^|\n)\s*(?:\*\*)?Cc:(?:\*\*)?\s*.*?\n\s*\n")
PLACEHOLDER_RE = re.compile(r"\[placeholder\]", re.IGNORECASE)
TABLE_DATA_RE = re.compile(r"(?is)<table\s+data-[\s\S]*?</table>")

TITLE_ENRICHED_EML_SUFFIX_1 = re.compile(r"\.enriched\.eml\s*$", re.IGNORECASE)
TITLE_ENRICHED_EML_SUFFIX_2 = re.compile(r"~~enriched\.eml\s*$", re.IGNORECASE)
TITLE_EML_SUFFIX = re.compile(r"\.eml\s*$", re.IGNORECASE)

MAIL_META_BLOCK_RE = re.compile(r"(?s)\A\s*```[\s\S]*?```")
MAIL_META_FROM_RE = re.compile(
    r"(?is)(?:^|\[MAIL_META\]|\s)From\s*:\s*(.*?)(?=\s+Date\s*:|\s+\[EDM_LINKS\]|\n|$)"
)

MAIL_META_DATE_RE = re.compile(
    r"(?is)(?:^|\s)Date\s*:\s*(.*?)(?=\s+\[EDM_LINKS\]|\n|$)"
)

DISCLAIMER_BLOCK = (
    "SECRET\n\n"
 
    "주시기 바랍니다."
)

# =========================
# PATH HELPERS
# =========================
def get_category_from_path(in_path: str) -> str:
    rel = os.path.relpath(in_path, WATCH_ROOT)
    parts = rel.split(os.sep)
    return parts[0] if parts else ""

def get_version_from_path(in_path: str) -> str:
    rel = os.path.relpath(in_path, WATCH_ROOT)
    parts = rel.split(os.sep)

    max_ver = -1
    max_ver_str = "ver0"

    for p in parts:
        pl = (p or "").lower()

        match = re.match(r"ver(\d+)$", pl)
        if match:
            num = int(match.group(1))
            if num > max_ver:
                max_ver = num
                max_ver_str = p

    print('####################')
    print(max_ver_str)
    return max_ver_str

def compute_out_paths_3(in_path: str) -> Tuple[str, str, str]:
    rel = os.path.relpath(in_path, WATCH_ROOT)
    parts = rel.split(os.sep)
    parts = [p for p in parts if not p.startswith("export_")]

    filename = parts[-1]
    base, ext = os.path.splitext(filename)
    if ext.lower() != ".jsonl":
        raise ValueError("Not a jsonl file")

    out_dir = os.path.join(OUT_ROOT, *parts[:-1])
    raw_path = os.path.join(out_dir, f"{base}__raw.jsonl")
    full_path = os.path.join(out_dir, f"{base}__full.jsonl")
    lite_path = os.path.join(out_dir, f"{base}__lite.jsonl")
    return raw_path, full_path, lite_path

# =========================
# NORMALIZATION HELPERS
# =========================
def _parse_rfc2822_to_iso(dt_str: str) -> Optional[str]:
    if not dt_str:
        return None
    s = dt_str.strip()
    for fmt in ("%a, %d %b %Y %H:%M:%S %z", "%d %b %Y %H:%M:%S %z"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.isoformat(timespec="seconds")
        except Exception:
            pass
    return None

def extract_mail_meta_from_content(content: str) -> Dict[str, Optional[str]]:
    if not content:
        return {"mail_from": None, "mail_date": None, "mail_date_iso": None}

    c = content.replace("\r\n", "\n").replace("\r", "\n")
    m = MAIL_META_BLOCK_RE.match(c)
    head = m.group(0) if m else c[:3000]
    scope = head

    fm = MAIL_META_FROM_RE.search(scope)
    dm = MAIL_META_DATE_RE.search(scope)

    mail_from = fm.group(1).strip() if fm else None
    date_raw = dm.group(1).strip() if dm else None
    mail_date_iso = _parse_rfc2822_to_iso(date_raw) if date_raw else None

    mail_date = None
    if date_raw:
        mail_date = re.sub(r"\s*[+-]\d{4}\s*$", "", date_raw).strip()

    return {
        "mail_from": mail_from,
        "mail_date": mail_date,
        "mail_date_iso": mail_date_iso,
    }

def sanitize_title(title: str) -> str:
    if not title:
        return title
    t = title.strip()
    t = TITLE_ENRICHED_EML_SUFFIX_1.sub("", t)
    t = TITLE_ENRICHED_EML_SUFFIX_2.sub("", t)
    t = TITLE_EML_SUFFIX.sub("", t)
    t = t.rstrip(" ._-~")
    return t

def strip_trailing_disclaimer(content: str) -> str:
    if not content:
        return content
    c = content.replace("\r\n", "\n").replace("\r", "\n")
    d = DISCLAIMER_BLOCK
    c_rstrip = c.rstrip()
    if c_rstrip.endswith(d):
        return c_rstrip[:-len(d)].rstrip()
    if c_rstrip.endswith("\n\n" + d):
        return c_rstrip[:-len("\n\n" + d)].rstrip()
    return c

def strip_original_message_section(content: str) -> str:
    if not content:
        return content

    c = content.replace("\r\n", "\n").replace("\r", "\n")
    c = c.replace("\xa0", " ")

    m = ORIGINAL_MESSAGE_RE.search(c)
    if m:
        c = c[:m.start()].rstrip()

    return c

def clean_content(content: str) -> str:
    if not content:
        return content

    c = content.replace("\r\n", "\n").replace("\r", "\n")
    c = c.replace("\xa0", " ")

    c = strip_original_message_section(c)
    c = TO_BLOCK_RE.sub("\n", c)
    c = CC_BLOCK_RE.sub("\n", c)
    c = PLACEHOLDER_RE.sub("", c)
    c = TABLE_DATA_RE.sub("", c)
    c = strip_trailing_disclaimer(c)

    c = re.sub(r"\n{3,}", "\n\n", c).strip()
    return c

def drop_owner_key(af: Any) -> Dict[str, Any]:
    if not isinstance(af, dict):
        return {}
    out = dict(af)
    out.pop("owner", None)
    return out

def build_base_doc(doc: Dict[str, Any], version_tag: str) -> Dict[str, Any]:
    """
    build_serving_views.py 가 기대하는 핵심 함수
    """
    title_raw = doc.get("title", "") or ""
    content_raw = doc.get("content", "") or ""

    title = sanitize_title(title_raw)
    mail_meta = extract_mail_meta_from_content(content_raw)
    content = clean_content(content_raw)

    base_doc = dict(doc)
    base_doc["title"] = title
    base_doc["content"] = content

    orig_af = base_doc.get("additionalField", {})
    if not isinstance(orig_af, dict):
        orig_af = {}
    orig_af = drop_owner_key(orig_af)

    common_af = dict(orig_af)
    for k, v in (mail_meta or {}).items():
        if v is not None:
            common_af[k] = v
    common_af["version_tag"] = version_tag

    base_doc["additionalField"] = common_af
    return base_doc

def iter_base_docs_from_jsonl(in_path: str) -> List[Dict[str, Any]]:
    import json

    version_tag = get_version_from_path(in_path)
    docs: List[Dict[str, Any]] = []

    with open(in_path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            doc = json.loads(line)
            docs.append(build_base_doc(doc, version_tag))

    return docs