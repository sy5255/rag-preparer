#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
rag-preparer 1시간 주기 잡 (DB 상태 추적).

doc-parser의 PARSE 결과를 받아 세 단계를 처리합니다.
- PREPROCESS : 메일 1건의 export jsonl → raw/full/lite jsonl 생성 (LLM 메타데이터 포함)
- CANDIDATE  : FULL 결과에서 용어 후보를 term_candidate_queue에 적재
- UPLOAD     : 문서 1건 × 인덱스 1개 단위로 RAG 인덱스에 업로드

모든 단계의 상태는 ae_llm_agent_pipeline_task / _attempt / _run 에 기록됩니다.

실행: python run_pipeline.py
"""

import hashlib
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

import normalize_docs
import pipeline_state as ps
import upload_indices
from build_serving_views import LLMEnrichmentError, build_doc_views
from candidate_queue import collect_candidates_from_additional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("rag_preparer")

COMPONENT = "rag-preparer"
STAGE_PARSE = "PARSE"
STAGE_PREPROCESS = "PREPROCESS"
STAGE_CANDIDATE = "CANDIDATE"
STAGE_UPLOAD = "UPLOAD"
STAGES = [STAGE_PREPROCESS, STAGE_CANDIDATE, STAGE_UPLOAD]

TIME_BUDGET_SEC = int(os.getenv("RAG_PREPARER_TIME_BUDGET_SEC", str(50 * 60)))
# 시간 예산 종료 전 새 작업을 가져오지 않는 여유 시간(초). PREPROCESS는 LLM 호출이 길어 더 크게 둡니다.
CLAIM_RESERVE_SEC = int(os.getenv("RAG_PREPARER_CLAIM_RESERVE_SEC", "60"))
PREPROCESS_RESERVE_SEC = int(os.getenv("RAG_PREPARER_PREPROCESS_RESERVE_SEC", "300"))
MAX_ATTEMPT = int(os.getenv("RAG_PREPARER_MAX_ATTEMPT", "5"))
UPLOAD_TIMEOUT_SEC = float(os.getenv("RAG_UPLOAD_TIMEOUT_SEC", "30"))

# 한 라운드에서 단계별로 처리할 최대 건수. 업로드가 LLM 처리 뒤에 밀려 굶지 않도록 번갈아 처리합니다.
ROUND_QUOTA = [
    (STAGE_UPLOAD, int(os.getenv("RAG_ROUND_UPLOAD", "50"))),
    (STAGE_CANDIDATE, int(os.getenv("RAG_ROUND_CANDIDATE", "10"))),
    (STAGE_PREPROCESS, int(os.getenv("RAG_ROUND_PREPROCESS", "1"))),
]

# 예전 upload_indices.py 상태 파일. 이미 처리/업로드된 결과를 재사용해 LLM 재호출·재업로드를 막습니다.
LEGACY_STATE_FILE = os.getenv(
    "RAG_LEGACY_STATE_FILE",
    os.path.join(normalize_docs.OUT_ROOT, "_state_processed.json"),
)
USE_LEGACY_STATE = os.getenv("RAG_USE_LEGACY_STATE", "true").strip().lower() in {
    "1", "true", "yes", "y", "on",
}

MODES = ("raw", "full", "lite")


# =========================
# 공통
# =========================
def load_legacy_state() -> Dict[str, Any]:
    if not USE_LEGACY_STATE or not os.path.exists(LEGACY_STATE_FILE):
        return {}
    try:
        with open(LEGACY_STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception as e:
        logger.warning("legacy state unreadable path=%s err=%s", LEGACY_STATE_FILE, e)
        return {}


def read_jsonl(path: str) -> List[Dict[str, Any]]:
    docs = []
    with open(path, "r", encoding="utf-8") as f:
        for lineno, raw in enumerate(f, 1):
            line = raw.strip()
            if not line:
                continue
            try:
                doc = json.loads(line)
            except json.JSONDecodeError as e:
                raise ps.PermanentError(f"invalid JSON at {path}:{lineno}: {e}")
            if not isinstance(doc, dict):
                raise ps.PermanentError(f"non-object JSON at {path}:{lineno}")
            docs.append(doc)
    return docs


def write_jsonl_atomic(path: str, docs: List[Dict[str, Any]]) -> None:
    text = "".join(json.dumps(d, ensure_ascii=False) + "\n" for d in docs)
    ps.atomic_write_text(Path(path), text)


def index_names(category: str, version_tag: str) -> Dict[str, str]:
    base = upload_indices.category_to_base_index(category)
    return {mode: f"{base}-{version_tag}-{mode}" for mode in MODES}


def payload_for(doc: Dict[str, Any], index_name: str, mode: str) -> Dict[str, Any]:
    return {
        "index_name": index_name,
        "data": upload_indices.build_upload_data_obj(doc, mode=mode),
        "chunk_factor": upload_indices.CHUNK_FACTOR,
    }


def payload_hash_for(doc: Dict[str, Any], index_name: str, mode: str) -> str:
    payload = payload_for(doc, index_name, mode)
    # 생성 시각(created_time 기본값)은 hash에서 제외 (upload_indices와 동일 규칙)
    payload["data"]["created_time"] = doc.get("created_time")
    return upload_indices.compute_payload_hash(payload)


# =========================
# PREPROCESS
# =========================
def _legacy_outputs_reusable(jsonl_path: str, outs: Tuple[str, str, str], legacy: Dict[str, Any]) -> bool:
    processed = legacy.get("processed_inputs") or {}
    if not all(os.path.exists(p) for p in outs):
        return False
    prev_sig = processed.get(jsonl_path)
    return bool(prev_sig) and prev_sig == upload_indices.file_signature(jsonl_path)


def process_preprocess(
    queue: ps.TaskQueue,
    run: ps.PipelineRun,
    task: ps.Task,
    legacy: Dict[str, Any],
) -> None:
    export_dir = Path(task.input_ref or "")
    if not export_dir.is_dir():
        raise ps.PermanentError(f"PARSE output dir does not exist: {export_dir}")
    jsonl_files = sorted(str(p) for p in export_dir.rglob("*.jsonl") if p.is_file())
    if not jsonl_files:
        raise ps.PermanentError(f"no jsonl in PARSE output: {export_dir}")

    # 마지막 시도가 아니면 LLM 실패를 일시 오류로 보고 재시도한다.
    strict_llm = not task.is_last_attempt

    outputs: List[Dict[str, Any]] = []
    upload_children: List[Dict[str, Any]] = []
    full_paths: List[str] = []
    degraded_docs = 0
    all_legacy = True
    categories = set()
    hasher = hashlib.sha256()

    for jsonl_path in jsonl_files:
        category = normalize_docs.get_category_from_path(jsonl_path)
        version_tag = normalize_docs.get_version_from_path(jsonl_path)
        categories.add(category)
        raw_out, full_out, lite_out = normalize_docs.compute_out_paths_3(jsonl_path)
        out_by_mode = {"raw": raw_out, "full": full_out, "lite": lite_out}

        if _legacy_outputs_reusable(jsonl_path, (raw_out, full_out, lite_out), legacy):
            views = {mode: read_jsonl(path) for mode, path in out_by_mode.items()}
            run.incr("legacy_preprocess_reused")
            logger.info("reuse legacy outputs task_id=%s jsonl=%s", task.id, jsonl_path)
        else:
            all_legacy = False
            views = {mode: [] for mode in MODES}
            for doc in read_jsonl(jsonl_path):
                try:
                    raw_doc, full_doc, lite_doc, degraded = build_doc_views(
                        doc, category, version_tag, strict_llm=strict_llm
                    )
                except LLMEnrichmentError as e:
                    raise ps.TransientError(str(e))
                degraded_docs += int(degraded)
                views["raw"].append(raw_doc)
                views["full"].append(full_doc)
                views["lite"].append(lite_doc)
                queue.heartbeat(task)

            os.makedirs(os.path.dirname(full_out), exist_ok=True)
            for mode in MODES:
                write_jsonl_atomic(out_by_mode[mode], views[mode])

        indexes = index_names(category, version_tag)
        for mode in MODES:
            hasher.update(ps.sha256_file(Path(out_by_mode[mode])).encode("ascii"))
            for doc in views[mode]:
                doc_id = doc.get("doc_id")
                if not doc_id:
                    logger.warning("document without doc_id skipped path=%s", out_by_mode[mode])
                    run.incr("missing_doc_id")
                    continue
                upload_children.append({
                    "stage": STAGE_UPLOAD,
                    "item_key": f"{indexes[mode]}::{doc_id}",
                    "input_ref": json.dumps({
                        "path": out_by_mode[mode],
                        "mode": mode,
                        "index": indexes[mode],
                        "doc_id": doc_id,
                    }, ensure_ascii=False),
                    "input_hash": payload_hash_for(doc, indexes[mode], mode),
                })

        full_paths.append(full_out)
        outputs.append({"jsonl": jsonl_path, **out_by_mode, "category": category,
                        "version_tag": version_tag})

    output_hash = hasher.hexdigest()
    legacy_flag = all_legacy and bool(legacy)
    candidate_child = {
        "stage": STAGE_CANDIDATE,
        "item_key": "",
        "input_ref": json.dumps({
            "full": full_paths,
            "category": sorted(categories)[0] if categories else "",
            # 예전 경로로 이미 후보 적재가 끝난 결과면 중복 집계를 막기 위해 건너뜀
            "legacy_reused": legacy_flag,
        }, ensure_ascii=False),
        "input_hash": output_hash,
    }
    if legacy_flag:
        for child in upload_children:
            info = json.loads(child["input_ref"])
            info["legacy_reused"] = True
            child["input_ref"] = json.dumps(info, ensure_ascii=False)

    queue.save_checkpoint(task, outputs=outputs, degraded_docs=degraded_docs)
    queue.complete(
        task,
        output_ref=os.path.dirname(full_paths[0]),
        output_hash=output_hash,
        quality="DEGRADED" if degraded_docs else ("LEGACY" if legacy_flag else "NORMAL"),
        children=[candidate_child, *upload_children],
    )
    if degraded_docs:
        logger.warning("preprocess completed with LLM fallback task_id=%s docs=%s",
                       task.id, degraded_docs)


# =========================
# CANDIDATE
# =========================
def process_candidate(queue: ps.TaskQueue, run: ps.PipelineRun, task: ps.Task) -> None:
    info = json.loads(task.input_ref or "{}")
    if info.get("legacy_reused"):
        queue.complete(task, quality="LEGACY")
        run.incr("legacy_candidate_skipped")
        return

    category = info.get("category") or ""
    done = set(task.checkpoint.get("done_doc_keys") or [])

    for full_path in info.get("full") or []:
        for i, doc in enumerate(read_jsonl(full_path)):
            key = f"{full_path}::{doc.get('doc_id') or i}"
            if key in done:
                continue
            collect_candidates_from_additional(
                doc=doc,
                additional=doc.get("additionalField") or {},
                category=category,
                source_stage="build_serving_views_full",
            )
            done.add(key)
            # 문서 단위로 진행 기록 → 재시도 시 detected_count 중복 집계 방지
            queue.save_checkpoint(task, done_doc_keys=sorted(done))

    queue.complete(task, quality="NORMAL")


# =========================
# UPLOAD
# =========================
def _find_doc(path: str, doc_id: str) -> Optional[Dict[str, Any]]:
    for doc in read_jsonl(path):
        if doc.get("doc_id") == doc_id:
            return doc
    return None


def process_upload(
    queue: ps.TaskQueue,
    run: ps.PipelineRun,
    task: ps.Task,
    legacy: Dict[str, Any],
) -> None:
    info = json.loads(task.input_ref or "{}")
    index_name = info["index"]
    mode = info["mode"]

    if info.get("legacy_reused") and (legacy.get("uploaded_docs") or {}).get(task.item_key) is True:
        queue.complete(task, output_hash=task.input_hash, quality="LEGACY")
        run.incr("legacy_upload_skipped")
        return

    if not os.path.exists(info["path"]):
        raise ps.TransientError(f"preprocessed file missing: {info['path']}")
    doc = _find_doc(info["path"], info["doc_id"])
    if doc is None:
        raise ps.PermanentError(f"doc_id={info['doc_id']} not found in {info['path']}")

    payload = payload_for(doc, index_name, mode)
    try:
        resp = requests.post(
            upload_indices.RAG_URL,
            headers=upload_indices.RAG_HEADERS,
            data=json.dumps(payload, ensure_ascii=False),
            timeout=UPLOAD_TIMEOUT_SEC,
        )
    except requests.RequestException as e:
        raise ps.TransientError(f"upload request error: {e!r}")

    if 200 <= resp.status_code < 300:
        queue.complete(task, output_hash=task.input_hash, quality="NORMAL")
        return

    message = f"upload status={resp.status_code} body={resp.text[:500]}"
    if 400 <= resp.status_code < 500 and resp.status_code not in (408, 429):
        raise ps.PermanentError(message)
    raise ps.TransientError(message)


# =========================
# 실행
# =========================
def seed_preprocess(queue: ps.TaskQueue) -> int:
    return queue.seed(
        STAGE_PREPROCESS,
        f"""
        SELECT mail_id, '' AS item_key, output_ref AS input_ref, output_hash AS input_hash
        FROM `{ps.TASK_TABLE}`
        WHERE stage=%s AND item_key='' AND status='COMPLETED' AND output_ref IS NOT NULL
        """,
        (STAGE_PARSE,),
    )


def _handle(queue: ps.TaskQueue, run: ps.PipelineRun, task: ps.Task, legacy: Dict[str, Any]) -> None:
    if task.stage == STAGE_PREPROCESS:
        process_preprocess(queue, run, task, legacy)
    elif task.stage == STAGE_CANDIDATE:
        process_candidate(queue, run, task)
    elif task.stage == STAGE_UPLOAD:
        process_upload(queue, run, task, legacy)
    else:
        raise ps.PermanentError(f"unknown stage {task.stage}")


def run_once(cfg: Optional[ps.DBConfig] = None) -> Dict[str, int]:
    cfg = cfg or ps.DBConfig.from_env()
    ps.ensure_schema(cfg)

    with ps.PipelineRun(cfg, COMPONENT, time_budget_sec=TIME_BUDGET_SEC) as run:
        if not run.acquired:
            logger.warning("another rag-preparer run is active; skipped")
            return {}

        queue = ps.TaskQueue(cfg, run, max_attempt=MAX_ATTEMPT)
        legacy = load_legacy_state()

        recovered = queue.recover_orphans(STAGES)
        run.incr("recovered", recovered)
        seeded = seed_preprocess(queue)
        run.incr("seeded", seeded)
        logger.info("rag-preparer started run_id=%s recovered=%s seeded=%s legacy_state=%s",
                    run.run_id, recovered, seeded, bool(legacy))

        stop = False
        preprocess_deferred = False
        while not stop:
            progressed = False
            for stage, quota in ROUND_QUOTA:
                reserve = PREPROCESS_RESERVE_SEC if stage == STAGE_PREPROCESS else CLAIM_RESERVE_SEC
                for _ in range(quota):
                    if run.deadline_reached(reserve):
                        if stage == STAGE_PREPROCESS:
                            preprocess_deferred = True
                        else:
                            stop = True
                            run.exit_reason = "TIME_BUDGET"
                        break
                    task = queue.claim_next(stage)
                    if task is None:
                        break
                    progressed = True
                    try:
                        _handle(queue, run, task, legacy)
                        run.incr(f"{stage.lower()}_completed")
                    except Exception as e:
                        status = queue.fail(task, e)
                        run.incr(f"{stage.lower()}_{status.lower()}")
                        logger.exception("%s failed task_id=%s mail_id=%s item=%s -> %s",
                                         stage, task.id, task.mail_id, task.item_key, status)
                if stop:
                    break
            if not progressed and not stop:
                run.exit_reason = "TIME_BUDGET" if preprocess_deferred else "DRAINED"
                break

        logger.info("rag-preparer finished run_id=%s reason=%s counters=%s",
                    run.run_id, run.exit_reason, run.counters)
        return dict(run.counters)


if __name__ == "__main__":
    run_once()
