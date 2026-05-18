#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import re
import uuid
import time
import requests
from typing import Dict, Any, List, Optional, Tuple

import mysql.connector

# =========================
# LLM SETTINGS
# =========================
LLM_API_URL = os.getenv(
    "LLM_API_URL",
    "http://ag.ss.net:8000/gpt-oss/1/gpt-oss-120b/v1/chat/completions"
)
LLM_TICKET = os.getenv("LLM_TICKET", "creQ==")

LLM_HEADERS_BASE = {
    "x-dep-ticket": LLM_TICKET,
    "Send-System-Name": os.getenv("LLM_SEND_SYSTEM_NAME", "AutoMeasure"),
    "User-Id": os.getenv("LLM_USER_ID", "s.park"),
    "User-Type": os.getenv("LLM_USER_TYPE", "AD_ID"),
    "Accept": "text/event-stream; charset=utf-8",
    "Content-Type": "application/json",
}

# =========================
# MYSQL SETTINGS
# =========================
MYSQL_HOST = os.getenv("MYSQL_HOST", "10.111.111.111") 
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "1111")) 
MYSQL_DB = os.getenv("MYSQL_DB", "fspas")
MYSQL_USER = os.getenv("MYSQL_USER", "dbuser")
MYSQL_PASS = os.getenv("MYSQL_PASS", "12345") 

TERM_CACHE_TTL_SEC = int(os.getenv("TERM_CACHE_TTL_SEC", "300"))

# =========================
# REGEX
# =========================
URL_REGEX = re.compile(r"(https?://[^\s\]\)\"\'<>]+)", re.IGNORECASE)
ACRONYM_PATTERN = re.compile(r"^[A-Z0-9][A-Z0-9\-_/.]{1,20}$")
NODE_LIKE_PATTERN = re.compile(r"\bSF\d+\b", re.IGNORECASE)

LABEL_PATTERNS = [
    re.compile(r"(?:불량명|Defect\s*Name|Defect|Issue)\s*[:=]\s*([A-Za-z0-9\-_/.]{2,40})", re.IGNORECASE),
]

# chemistry formula fallback
CHEM_FORMULA_PATTERN = re.compile(
    r"\b(?:HF|HCl|H2SO4|NH4OH|NH4F|BOE|SPM|SC1|SC2|DHF)\b",
    re.IGNORECASE
)

# =========================
# GLOBAL CACHE
# =========================
_TERM_CACHE: Dict[str, Any] = {
    "loaded_at": 0.0,
    "data": None,
}

# =========================
# MYSQL LOAD
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

def _normalize_text(s: str) -> str:
    s = str(s or "")
    s = s.replace("\u00A0", " ")
    s = s.strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def _normalize_key_for_lookup(s: str) -> str:
    """
    alias lookup용 normalize.
    - 대소문자 무시
    - 연속 공백 축약
    - 하이픈/언더바/슬래시를 공백으로 간주
    """
    s = str(s or "")
    s = s.replace("\u00A0", " ")
    s = s.lower()
    s = s.replace("-", " ").replace("_", " ").replace("/", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _safe_json_loads(s: Any) -> Dict[str, Any]:
    if not s:
        return {}
    if isinstance(s, dict):
        return s
    try:
        return json.loads(s)
    except Exception:
        return {}

def _fetch_term_rows() -> List[Dict[str, Any]]:
    """
    active term + active alias 전체 조회
    """
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

def _build_term_cache(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    structure:
    {
      "by_type": {
        "product": {
          "terms": [...],
          "aliases": [...]
        },
        ...
      }
    }
    """
    by_term_id: Dict[int, Dict[str, Any]] = {}

    for r in rows:
        term_id = int(r["term_id"])
        if term_id not in by_term_id:
            by_term_id[term_id] = {
                "term_id": term_id,
                "term_type": r["term_type"],
                "canonical_name": r["canonical_name"],
                "display_name": r["display_name"] or r["canonical_name"],
                "description": r["description"] or "",
                "scope": r["scope"] or "all",
                "status": r["status"],
                "is_verified": bool(r["is_verified"]),
                "metadata": _safe_json_loads(r.get("metadata_json")),
                "aliases": [],
            }

        alias_text = r.get("alias_text")
        if alias_text:
            alias_normalized = r.get("alias_normalized") or _normalize_key_for_lookup(alias_text)
            by_term_id[term_id]["aliases"].append({
                "alias_id": r.get("alias_id"),
                "alias_text": alias_text,
                "alias_normalized": alias_normalized,
                "match_type": r.get("match_type") or "contains",
                "language_code": r.get("language_code"),
                "is_preferred": bool(r.get("is_preferred")),
                "status": r.get("alias_status"),
            })

    by_type: Dict[str, Dict[str, Any]] = {}
    for term in by_term_id.values():
        ttype = term["term_type"]
        slot = by_type.setdefault(ttype, {"terms": [], "aliases": []})
        slot["terms"].append(term)

        # canonical도 alias처럼 검색 가능하게 자동 추가
        canonical_aliases = [{
            "alias_id": None,
            "alias_text": term["canonical_name"],
            "alias_normalized": _normalize_key_for_lookup(term["canonical_name"]),
            "match_type": "contains",
            "language_code": None,
            "is_preferred": True,
            "status": "active",
        }]

        all_aliases = canonical_aliases + term["aliases"]
        seen = set()
        for a in all_aliases:
            key = (a["alias_normalized"], a["match_type"])
            if key in seen:
                continue
            seen.add(key)
            slot["aliases"].append({
                "term_id": term["term_id"],
                "term_type": term["term_type"],
                "canonical_name": term["canonical_name"],
                "display_name": term["display_name"],
                "description": term["description"],
                "scope": term["scope"],
                "is_verified": term["is_verified"],
                "metadata": term["metadata"],
                **a
            })

    return {"by_type": by_type}

def _get_term_cache(force_reload: bool = False) -> Dict[str, Any]:
    now = time.time()
    if (
        not force_reload
        and _TERM_CACHE["data"] is not None
        and (now - float(_TERM_CACHE["loaded_at"])) < TERM_CACHE_TTL_SEC
    ):
        return _TERM_CACHE["data"]

    rows = _fetch_term_rows()
    data = _build_term_cache(rows)
    _TERM_CACHE["loaded_at"] = now
    _TERM_CACHE["data"] = data
    return data

# =========================
# HELPERS
# =========================
def safe_json_from_llm(text: str) -> Dict[str, Any]:
    text = (text or "").strip()
    if not text:
        raise ValueError("Empty LLM content")

    i = text.find("{")
    j = text.rfind("}")
    if i == -1 or j == -1 or j <= i:
        raise ValueError("No JSON object braces found in LLM output")

    candidate = text[i:j+1].strip()
    return json.loads(candidate)

def deep_merge_dict(base: Dict[str, Any], extra: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base) if isinstance(base, dict) else {}
    if not isinstance(extra, dict):
        return out

    for k, v in extra.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = deep_merge_dict(out[k], v)
        else:
            out[k] = v
    return out

def extract_links(text: str) -> List[str]:
    if not text:
        return []
    return list(dict.fromkeys(URL_REGEX.findall(text)))

def _mk_candidate(
    raw: str,
    canonical: Optional[str],
    source: str,
    confidence: float,
    term_id: Optional[int] = None,
    description: Optional[str] = None,
    is_verified: Optional[bool] = None,
    scope: Optional[str] = None,
) -> Dict[str, Any]:
    out = {
        "raw": raw,
        "canonical": canonical or raw,
        "source": source,
        "confidence": round(float(confidence), 3),
    }
    if term_id is not None:
        out["term_id"] = term_id
    if description:
        out["description"] = description
    if is_verified is not None:
        out["is_verified"] = bool(is_verified)
    if scope:
        out["scope"] = scope
    return out

def _term_scope_match(scope: str, category: Optional[str]) -> bool:
    scope = (scope or "all").strip().lower()
    if scope in ("all", "", "*"):
        return True
    if not category:
        return False
    return scope == str(category).strip().lower()

def _find_alias_hits(
    text: str,
    term_type: str,
    category: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    dictionary alias 기반 후보 탐색
    """
    cache = _get_term_cache()
    by_type = cache.get("by_type", {})
    slot = by_type.get(term_type, {})
    aliases = slot.get("aliases", [])

    norm_text = _normalize_key_for_lookup(text)
    hits: List[Dict[str, Any]] = []

    for a in aliases:
        if not _term_scope_match(a.get("scope"), category):
            continue

        alias_text = str(a.get("alias_text") or "").strip()
        alias_norm = str(a.get("alias_normalized") or "").strip()
        if not alias_text or not alias_norm:
            continue

        match_type = (a.get("match_type") or "contains").lower()
        matched = False

        if match_type == "exact":
            matched = (alias_norm == norm_text)
        elif match_type == "regex":
            try:
                matched = re.search(alias_text, text, re.IGNORECASE) is not None
            except re.error:
                matched = False
        else:
            # contains
            matched = alias_norm in norm_text

        if not matched:
            continue

        base_conf = 0.96 if a.get("is_verified") else 0.82
        if match_type == "exact":
            base_conf += 0.02

        hits.append(_mk_candidate(
            raw=alias_text,
            canonical=a.get("canonical_name") or alias_text,
            source=f"dict_{term_type}",
            confidence=min(base_conf, 0.99),
            term_id=a.get("term_id"),
            description=a.get("description"),
            is_verified=a.get("is_verified"),
            scope=a.get("scope"),
        ))

    return dedupe_candidates(hits)

def normalize_node(node: Optional[str], category: Optional[str] = None) -> Optional[str]:
    if not node:
        return None

    node_up = node.strip().upper()
    if NODE_LIKE_PATTERN.fullmatch(node_up):
        return node_up

    cache = _get_term_cache()
    slot = cache.get("by_type", {}).get("node", {})
    terms = slot.get("terms", [])

    for t in terms:
        if not _term_scope_match(t.get("scope"), category):
            continue
        if node_up == str(t.get("canonical_name") or "").upper():
            return str(t["canonical_name"]).upper()

    return None

def detect_node_from_text(title: str, content: str, category: Optional[str] = None) -> Optional[str]:
    text = f"{title}\n{content}"

    # DB node alias 우선
    node_hits = _find_alias_hits(text, "node", category=category)
    if node_hits:
        return str(node_hits[0]["canonical"]).upper()

    # fallback
    m = NODE_LIKE_PATTERN.search(text)
    if m:
        return m.group(0).upper()
    return None

def _sanitize_owner_id(owner_obj: Any, category: Optional[str]) -> Optional[str]:
    if not owner_obj or not isinstance(owner_obj, dict):
        return None
    oid = owner_obj.get("id")
    if oid is None:
        return None
    oid_norm = str(oid).strip()
    if not oid_norm:
        return None

    cache = _get_term_cache()
    slot = cache.get("by_type", {}).get("owner", {})
    terms = slot.get("terms", [])

    for t in terms:
        if not _term_scope_match(t.get("scope"), category):
            continue
        candidates = {str(t.get("canonical_name") or "").strip()}
        for a in t.get("aliases", []):
            candidates.add(str(a.get("alias_text") or "").strip())

        for cand in candidates:
            if oid_norm == cand or _normalize_text(oid_norm) == _normalize_text(cand):
                return str(t.get("canonical_name") or cand)
    return None

# =========================
# RULE-BASED EXTRACTION
# =========================
def regex_defect_candidates(title: str, content: str) -> List[Dict[str, Any]]:
    cands = []
    t = title or ""
    c = content or ""

    for pat in LABEL_PATTERNS:
        for m in pat.finditer(c):
            tok = m.group(1).strip()
            cands.append({"token": tok, "source": "content_label", "evidence": m.group(0)[:200]})

    for tok in re.findall(r"\b[A-Z0-9][A-Z0-9\-_/.]{1,20}\b", t):
        if ACRONYM_PATTERN.match(tok):
            cands.append({"token": tok, "source": "title_acronym", "evidence": t[:200]})

    seen = set()
    out = []
    for x in cands:
        if x["token"] in seen:
            continue
        seen.add(x["token"])
        out.append(x)
    return out

def score_defect(token: str, candidates: List[Dict[str, Any]], title: str, content: str) -> Dict[str, Any]:
    score = 0
    signals = {
        "label_match": False,
        "in_title": False,
        "dict_match": False,
        "repeated": False,
        "looks_like_acronym": False,
    }

    for c in candidates:
        if c["token"] == token and c["source"] == "content_label":
            score += 50
            signals["label_match"] = True

    if token and token in (title or ""):
        score += 20
        signals["in_title"] = True

    # defect dict hit
    defect_hits = _find_alias_hits(token, "defect", category=None)
    if defect_hits:
        score += 30
        signals["dict_match"] = True

    if ACRONYM_PATTERN.match(token or ""):
        score += 10
        signals["looks_like_acronym"] = True

    cnt = (title or "").count(token) + (content or "").count(token)
    if cnt >= 2:
        score += 15
        signals["repeated"] = True

    if score >= 70:
        conf = "high"
        conf_num = 0.90
    elif score >= 40:
        conf = "medium"
        conf_num = 0.65
    else:
        conf = "low"
        conf_num = 0.35

    return {"score": score, "confidence": conf, "confidence_num": conf_num, "signals": signals}

def extract_product_candidates_rule(title: str, content: str, category: Optional[str] = None) -> List[Dict[str, Any]]:
    text = f"{title}\n{content}"
    return _find_alias_hits(text, "product", category=category)

def extract_process_candidates_rule(title: str, content: str, category: Optional[str] = None) -> List[Dict[str, Any]]:
    text = f"{title}\n{content}"
    return _find_alias_hits(text, "process", category=category)

def extract_chemistry_candidates_rule(title: str, content: str, category: Optional[str] = None) -> List[Dict[str, Any]]:
    text = f"{title}\n{content}"
    found = _find_alias_hits(text, "chemistry", category=category)

    # fallback formula scan
    for m in CHEM_FORMULA_PATTERN.finditer(text):
        tok = m.group(0)
        found.append(_mk_candidate(tok, tok.upper().replace("-", ""), "regex_chem_formula", 0.96))

    return dedupe_candidates(found)

def extract_equipment_candidates_rule(title: str, content: str, category: Optional[str] = None) -> List[Dict[str, Any]]:
    text = f"{title}\n{content}"
    return _find_alias_hits(text, "equipment", category=category)

def extract_analysis_candidates_rule(title: str, content: str, category: Optional[str] = None) -> List[Dict[str, Any]]:
    text = f"{title}\n{content}"
    return _find_alias_hits(text, "analysis", category=category)

def extract_defect_candidates_rule(title: str, content: str, category: Optional[str] = None) -> List[Dict[str, Any]]:
    out = []

    # 1) label regex
    rule_cands = regex_defect_candidates(title, content)
    for c in rule_cands:
        scored = score_defect(c["token"], rule_cands, title, content)
        dict_hits = _find_alias_hits(c["token"], "defect", category=category)
        canonical = dict_hits[0]["canonical"] if dict_hits else c["token"]
        term_id = dict_hits[0].get("term_id") if dict_hits else None
        is_verified = dict_hits[0].get("is_verified") if dict_hits else None
        description = dict_hits[0].get("description") if dict_hits else None

        out.append({
            "raw": c["token"],
            "canonical": canonical,
            "source": c["source"],
            "confidence": round(scored["confidence_num"], 3),
            "score": scored["score"],
            "signals": scored["signals"],
            "evidence": c.get("evidence", ""),
            **({"term_id": term_id} if term_id is not None else {}),
            **({"is_verified": bool(is_verified)} if is_verified is not None else {}),
            **({"description": description} if description else {}),
        })

    # 2) text 전체에서 defect alias hit
    text = f"{title}\n{content}"
    dict_hits = _find_alias_hits(text, "defect", category=category)
    for h in dict_hits:
        out.append({
            "raw": h["raw"],
            "canonical": h["canonical"],
            "source": h["source"],
            "confidence": h["confidence"],
            "evidence": "",
            **({"term_id": h["term_id"]} if "term_id" in h else {}),
            **({"is_verified": h["is_verified"]} if "is_verified" in h else {}),
            **({"description": h["description"]} if "description" in h else {}),
        })

    return dedupe_candidates(out)

def extract_acronym_candidates(title: str, content: str) -> List[str]:
    text = f"{title}\n{content}"
    toks = re.findall(r"\b[A-Z0-9][A-Z0-9\-_/.]{1,20}\b", text)
    out = []
    seen = set()
    for tok in toks:
        if not ACRONYM_PATTERN.match(tok):
            continue
        if tok in seen:
            continue
        seen.add(tok)
        out.append(tok)
    return out[:50]

def dedupe_candidates(cands: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    best: Dict[str, Dict[str, Any]] = {}
    for c in cands or []:
        raw = str(c.get("raw") or "").strip()
        canonical = str(c.get("canonical") or raw).strip()
        key = f"{canonical}::{raw}"
        prev = best.get(key)
        if prev is None or float(c.get("confidence", 0)) > float(prev.get("confidence", 0)):
            best[key] = c
    out = list(best.values())
    out.sort(
        key=lambda x: (
            -float(x.get("confidence", 0)),
            str(x.get("canonical", "")),
            str(x.get("raw", "")),
        )
    )
    return out

# =========================
# LLM EXTRACTION
# =========================
def _owner_choices_for_category(category: str) -> List[str]:
    cache = _get_term_cache()
    owners = cache.get("by_type", {}).get("owner", {}).get("terms", [])
    out = []
    for t in owners:
        if _term_scope_match(t.get("scope"), category):
            out.append(str(t.get("canonical_name") or "").strip())
    return [x for x in out if x]

def _term_examples_for_prompt(term_type: str, category: Optional[str], limit: int = 20) -> List[str]:
    cache = _get_term_cache()
    items = cache.get("by_type", {}).get(term_type, {}).get("terms", [])
    out = []
    for t in items:
        if not _term_scope_match(t.get("scope"), category):
            continue
        out.append(str(t.get("canonical_name") or "").strip())
        if len(out) >= limit:
            break
    return [x for x in out if x]

def call_llm_build_additional(title: str, content: str, category: str) -> Dict[str, Any]:
    owner_choices = _owner_choices_for_category(category)
    product_examples = _term_examples_for_prompt("product", category, limit=30)
    process_examples = _term_examples_for_prompt("process", category, limit=30)
    chemistry_examples = _term_examples_for_prompt("chemistry", category, limit=30)
    defect_examples = _term_examples_for_prompt("defect", category, limit=30)
    node_examples = _term_examples_for_prompt("node", category, limit=20)
    equipment_examples = _term_examples_for_prompt("equipment", category, limit=20)
    analysis_examples = _term_examples_for_prompt("analysis", category, limit=20)

    system_prompt = (
        "You are a strict information extraction engine. "
        "You must only use information present in the document. "
        "Do NOT guess unknown values. "
        "Do NOT expand abbreviations unless the expansion is explicitly present in the document "
        "or directly matches the provided dictionary hints. "
        "Return STRICT JSON only."
    )

    user_prompt = f"""
Return JSON with this schema:

{{
  "doc_type": "email|report|meeting_note|unknown",
  "summary": "1-2 sentences Korean summary" | null,

  "owner": {{
    "id": "..." | null
  }},

  "node": "..." | null,

  "product_candidates": [
    {{
      "raw": "..." ,
      "canonical": "..." | null
    }}
  ],

  "process_candidates": [
    {{
      "raw": "..." ,
      "canonical": "..." | null
    }}
  ],

  "chemistry_candidates": [
    {{
      "raw": "..." ,
      "canonical": "..." | null
    }}
  ],

  "defect_candidates": [
    {{
      "raw": "..." ,
      "canonical": "..." | null,
      "evidence": ["...","..."]
    }}
  ],

  "equipment_candidates": [
    {{
      "raw": "..." ,
      "canonical": "..." | null
    }}
  ],
  "analysis_candidates": [
    {{
      "raw": "..." ,
      "canonical": "..." | null
    }}
  ],

  "report_links": ["https://..."],
  "lot": "..." | null
}}

Rules:
- owner.id MUST be one of: {owner_choices} OR null.
- For product/process/chemistry/defect candidates, return only terms explicitly present in the document.
- If canonical form is uncertain, keep canonical as null or same as raw.
- Do NOT invent new abbreviations or expansions.
- node preference: choose from {node_examples} if present in text;
  BUT if text contains a node-like token 'SF<digits>', you may return that exact token instead.
- report_links: ONLY explicit URLs present in the text.
- summary must be concise Korean.
- Output JSON ONLY.

Dictionary hints:
- products: {product_examples}
- processes: {process_examples}
- chemistries: {chemistry_examples}
- defects: {defect_examples}
- equipments: {equipment_examples}
- analyses: {analysis_examples}

DOCUMENT:
TITLE: {title}

CONTENT:
{content}
""".strip()

    payload = {
        "model": "openai/gpt-oss-120b",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.0,
        "stream": False,
    }

    headers = dict(LLM_HEADERS_BASE)
    headers["Prompt-Msg-Id"] = str(uuid.uuid4())
    headers["Completion-Msg-Id"] = str(uuid.uuid4())

    resp = requests.post(LLM_API_URL, headers=headers, data=json.dumps(payload), timeout=60)
    resp.raise_for_status()
    recv_json = resp.json()
    text = recv_json["choices"][0]["message"]["content"]
    return safe_json_from_llm(text)

# =========================
# BUILD additionalField
# =========================
def build_additional_lite(title: str, content: str, category: Optional[str] = None) -> Dict[str, Any]:
    links = extract_links(title + "\n" + content)

    product_candidates = extract_product_candidates_rule(title, content, category=category)
    process_candidates = extract_process_candidates_rule(title, content, category=category)
    chemistry_candidates = extract_chemistry_candidates_rule(title, content, category=category)
    defect_candidates = extract_defect_candidates_rule(title, content, category=category)

    label_tok = defect_candidates[0]["raw"] if defect_candidates else None

    if label_tok:
        scored = score_defect(label_tok, regex_defect_candidates(title, content), title, content)
        defect_extraction = {
            "defect_name": label_tok,
            "confidence": scored["confidence"],
            "score": scored["score"],
            "signals": scored["signals"],
        }
        debug = {
            "defect_evidence": [defect_candidates[0].get("evidence", "")] if defect_candidates[0].get("evidence") else [],
            "acronym_candidates": extract_acronym_candidates(title, content),
        }
    else:
        defect_extraction = {
            "defect_name": None,
            "confidence": "low",
            "score": 0,
            "signals": {},
        }
        debug = {
            "defect_evidence": [],
            "acronym_candidates": extract_acronym_candidates(title, content),
        }

    return {
        "doc_type": None,
        "summary": None,
        "owner": None,
        "node": detect_node_from_text(title, content, category=category),
        "product": product_candidates[0]["canonical"] if product_candidates else None,
        "product_candidates": product_candidates,
        "process_candidates": process_candidates,
        "chemistry_candidates": chemistry_candidates,
        "defect_candidates": defect_candidates,
        "report_links": links,
        "lot": None,
        "defect_extraction": defect_extraction,
        "_debug": debug,
    }

def _normalize_llm_candidate_list(
    items: Any,
    source: str,
    default_conf: float
) -> List[Dict[str, Any]]:
    out = []
    if not isinstance(items, list):
        return out

    for x in items:
        if not isinstance(x, dict):
            continue
        raw = str(x.get("raw") or "").strip()
        canonical = x.get("canonical")
        canonical = str(canonical).strip() if canonical else raw
        if not raw:
            continue
        out.append(_mk_candidate(raw, canonical, source, default_conf))
    return dedupe_candidates(out)

def build_additional_full(title: str, content: str, category: str) -> Dict[str, Any]:
    rule_links = extract_links(title + "\n" + content)

    rule_product = extract_product_candidates_rule(title, content, category=category)
    rule_process = extract_process_candidates_rule(title, content, category=category)
    rule_chem = extract_chemistry_candidates_rule(title, content, category=category)
    rule_defect = extract_defect_candidates_rule(title, content, category=category)
    rule_equip = extract_equipment_candidates_rule(title, content, category=category)
    rule_anal = extract_analysis_candidates_rule(title, content, category=category)

    add = call_llm_build_additional(title, content, category)
    merged_links = list(dict.fromkeys(rule_links + (add.get("report_links") or [])))

    llm_product = _normalize_llm_candidate_list(add.get("product_candidates"), "llm_product", 0.72)
    llm_process = _normalize_llm_candidate_list(add.get("process_candidates"), "llm_process", 0.72)
    llm_chem = _normalize_llm_candidate_list(add.get("chemistry_candidates"), "llm_chemistry", 0.75)
    llm_equip = _normalize_llm_candidate_list(add.get("equipment_candidates"), "llm_equipment", 0.72)
    llm_anal = _normalize_llm_candidate_list(add.get("analysis_candidates"), "llm_analysis", 0.72)

    llm_defect_raw = add.get("defect_candidates")
    llm_defect = []
    if isinstance(llm_defect_raw, list):
        for x in llm_defect_raw:
            if not isinstance(x, dict):
                continue
            raw = str(x.get("raw") or "").strip()
            canonical = str(x.get("canonical") or raw).strip()
            if not raw:
                continue
            llm_defect.append({
                "raw": raw,
                "canonical": canonical,
                "source": "llm_defect",
                "confidence": 0.72,
                "evidence": (x.get("evidence") or [])[:3],
            })

    product_candidates = dedupe_candidates(rule_product + llm_product)
    process_candidates = dedupe_candidates(rule_process + llm_process)
    chemistry_candidates = dedupe_candidates(rule_chem + llm_chem)
    defect_candidates = dedupe_candidates(rule_defect + llm_defect)
    equipment_candidates = dedupe_candidates(rule_equip + llm_equip)
    analysis_candidates = dedupe_candidates(rule_anal + llm_anal)

    primary = defect_candidates[0]["raw"] if defect_candidates else None
    if primary:
        scored = score_defect(primary, regex_defect_candidates(title, content), title, content)
        defect_extraction = {
            "defect_name": primary,
            "confidence": scored["confidence"],
            "score": scored["score"],
            "signals": scored["signals"],
        }
    else:
        defect_extraction = {
            "defect_name": None,
            "confidence": "low",
            "score": 0,
            "signals": {},
        }

    owner_str = _sanitize_owner_id(add.get("owner"), category=category)

    node = normalize_node(add.get("node"), category=category)
    if not node:
        node = detect_node_from_text(title, content, category=category)

    lot = add.get("lot")

    llm_debug_evidence = []
    for x in llm_defect_raw or []:
        if isinstance(x, dict):
            llm_debug_evidence.extend((x.get("evidence") or [])[:2])

    return {
        "doc_type": add.get("doc_type"),
        "summary": add.get("summary"),
        "owner": owner_str,
        "node": node,
        "product": product_candidates[0]["canonical"] if product_candidates else None,
        "product_candidates": product_candidates,
        "process_candidates": process_candidates,
        "chemistry_candidates": chemistry_candidates,
        "defect_candidates": defect_candidates,
        "equipment_candidates": equipment_candidates,
        "analysis_candidates": analysis_candidates,
        "report_links": merged_links,
        "lot": lot,
        "defect_extraction": defect_extraction,
        "_debug": {
            "defect_evidence": llm_debug_evidence[:3],
            "acronym_candidates": extract_acronym_candidates(title, content),
        }
    }

def force_reload_term_cache() -> None:
    _get_term_cache(force_reload=True)