#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sharedworkspace 파일을 기준으로 DB 상태를 채웁니다 (1회성 백필, rag-preparer 담당 부분).

먼저 doc-parser 의 backfill_from_files.py --apply 로 메일 행과 PARSE 작업을 채운 뒤 실행하세요.

PARSE 가 COMPLETED 인 메일마다
1. export 폴더의 jsonl → preprocessed_jsonl 의 __raw/__full/__lite.jsonl 이 모두 있으면
   PREPROCESS 를 COMPLETED(quality='LEGACY')로 등록 (LLM 재호출 없음)
2. CANDIDATE 를 COMPLETED(LEGACY)로 등록 (예전 경로에서 이미 후보 적재가 끝났으므로 중복 집계 방지)
3. 문서 × 업로드 대상 인덱스(RAG_UPLOAD_INDEXES, 기본 '*-full')마다 UPLOAD 작업 등록
   - 예전 상태 파일(_state_processed.json)에 업로드 성공 기록이 있으면 COMPLETED(LEGACY)
   - 없으면 PENDING → 다음 run_pipeline.py 실행에서 업로드 (예전에 실패해 빠진 문서 복구)
   - --mark-all-uploaded 를 주면 기록이 없어도 모두 COMPLETED(LEGACY)로 처리

출력 파일이 없는 메일은 건드리지 않습니다(run_pipeline.py 가 LLM으로 새로 처리).
이미 COMPLETED/FAILED 인 작업은 바꾸지 않습니다.
RAG_UPLOAD_INDEXES 에 없는 인덱스는 UPLOAD 작업을 만들지 않고 upload_skipped_disabled_index 로만 셉니다.

업로드 대상 인덱스를 나중에 추가했다면 (예: RAG_UPLOAD_INDEXES=rp-ifa-ver3-full,rp-ifa-ver3-lite)
--uploads-only 로 실행해 이미 전처리가 끝난 메일들에 새 인덱스용 UPLOAD 작업만 추가합니다.

사용:
    python backfill_from_files.py                  # dry-run (DB에 쓰지 않고 건수만)
    python backfill_from_files.py --apply          # 실제 반영
    python backfill_from_files.py --uploads-only [--apply]   # 업로드 대상 인덱스 추가 후
    옵션: --mark-all-uploaded, --report <파일> (기본 backfill_from_files_report.json), --verbose
"""

import argparse
import hashlib
import json
import os
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

import run_pipeline as rp  # .env 로딩 포함
import normalize_docs
import pipeline_state as ps

DEFAULT_REPORT_PATH = Path("backfill_from_files_report.json")
SAMPLES_PER_CATEGORY = 5
COMMIT_EVERY = 200


def _upsert_completed(cur, mail_id: int, stage: str, item_key: str, *, input_ref: str,
                      input_hash: Optional[str], output_ref: Optional[str] = None,
                      output_hash: Optional[str] = None,
                      checkpoint: Optional[Dict[str, Any]] = None) -> int:
    """없으면 COMPLETED(LEGACY)로 추가, PENDING/RETRY면 COMPLETED로 변경, 그 외는 그대로."""
    cur.execute(
        f"""
        INSERT INTO `{ps.TASK_TABLE}`(mail_id, stage, item_key, status, input_ref, input_hash,
            output_ref, output_hash, checkpoint_json, quality, completed_at, last_error)
        VALUES(%s,%s,%s,'COMPLETED',%s,%s,%s,%s,%s,'LEGACY',NOW(),'backfilled from files')
        ON DUPLICATE KEY UPDATE
            input_ref       = IF(status IN ('PENDING','RETRY'), VALUES(input_ref), input_ref),
            input_hash      = IF(status IN ('PENDING','RETRY'), VALUES(input_hash), input_hash),
            output_ref      = IF(status IN ('PENDING','RETRY'), VALUES(output_ref), output_ref),
            output_hash     = IF(status IN ('PENDING','RETRY'), VALUES(output_hash), output_hash),
            checkpoint_json = IF(status IN ('PENDING','RETRY'), VALUES(checkpoint_json), checkpoint_json),
            quality         = IF(status IN ('PENDING','RETRY'), VALUES(quality), quality),
            completed_at    = IF(status IN ('PENDING','RETRY'), NOW(), completed_at),
            last_error      = IF(status IN ('PENDING','RETRY'), VALUES(last_error), last_error),
            status          = IF(status IN ('PENDING','RETRY'), 'COMPLETED', status)
        """,
        (
            mail_id, stage, item_key, input_ref, input_hash, output_ref, output_hash,
            json.dumps(checkpoint, ensure_ascii=False) if checkpoint is not None else None,
        ),
    )
    return cur.rowcount


def _insert_pending(cur, mail_id: int, stage: str, item_key: str, *, input_ref: str,
                    input_hash: Optional[str]) -> int:
    cur.execute(
        f"""
        INSERT IGNORE INTO `{ps.TASK_TABLE}`(mail_id, stage, item_key, status, input_ref, input_hash)
        VALUES(%s,%s,%s,'PENDING',%s,%s)
        """,
        (mail_id, stage, item_key, input_ref, input_hash),
    )
    return cur.rowcount


def _add_upload_tasks(cur, mail_id, uploads, uploaded_docs, mark_all_uploaded,
                      counters, details) -> None:
    for up in uploads:
        if mark_all_uploaded or uploaded_docs.get(up["item_key"]) is True:
            rc = _upsert_completed(
                cur, mail_id, rp.STAGE_UPLOAD, up["item_key"],
                input_ref=up["input_ref"], input_hash=up["input_hash"],
                output_hash=up["input_hash"],
            )
            counters["upload_completed_created" if rc else "upload_already_present"] += 1
        else:
            rc = _insert_pending(cur, mail_id, rp.STAGE_UPLOAD, up["item_key"],
                                 input_ref=up["input_ref"], input_hash=up["input_hash"])
            if rc:
                counters["upload_pending_created"] += 1
                details["upload_pending_created"].append(
                    {"mail_id": mail_id, "item_key": up["item_key"]})
            else:
                counters["upload_already_present"] += 1


def _collect_outputs(export_dir: Path) -> Optional[Dict[str, Any]]:
    """
    run_pipeline.process_preprocess 와 같은 규칙으로 출력 파일·업로드 대상을 모읍니다.
    출력 파일이 하나라도 없으면 None.
    """
    jsonl_files = sorted(str(p) for p in export_dir.rglob("*.jsonl") if p.is_file())
    if not jsonl_files:
        return None

    outputs, uploads, full_paths, categories = [], [], [], set()
    skipped_disabled = 0
    hasher = hashlib.sha256()
    for jsonl_path in jsonl_files:
        category = normalize_docs.get_category_from_path(jsonl_path)
        version_tag = normalize_docs.get_version_from_path(jsonl_path)
        raw_out, full_out, lite_out = normalize_docs.compute_out_paths_3(jsonl_path)
        out_by_mode = {"raw": raw_out, "full": full_out, "lite": lite_out}
        if not all(os.path.exists(p) for p in out_by_mode.values()):
            return None
        categories.add(category)
        indexes = rp.index_names(category, version_tag)
        for mode in rp.MODES:
            hasher.update(ps.sha256_file(Path(out_by_mode[mode])).encode("ascii"))
            enabled = rp.upload_index_enabled(indexes[mode])
            for doc in rp.read_jsonl(out_by_mode[mode]):
                doc_id = doc.get("doc_id")
                if not doc_id:
                    continue
                if not enabled:
                    skipped_disabled += 1
                    continue
                uploads.append({
                    "item_key": f"{indexes[mode]}::{doc_id}",
                    "input_ref": json.dumps({
                        "path": out_by_mode[mode], "mode": mode, "index": indexes[mode],
                        "doc_id": doc_id, "legacy_reused": True,
                    }, ensure_ascii=False),
                    "input_hash": rp.payload_hash_for(doc, indexes[mode], mode),
                })
        full_paths.append(full_out)
        outputs.append({"jsonl": jsonl_path, **out_by_mode, "category": category,
                        "version_tag": version_tag})

    return {
        "outputs": outputs,
        "uploads": uploads,
        "full_paths": full_paths,
        "category": sorted(categories)[0] if categories else "",
        "output_hash": hasher.hexdigest(),
        "skipped_disabled": skipped_disabled,
    }


def backfill(
    cfg: ps.DBConfig,
    *,
    apply: bool,
    mark_all_uploaded: bool = False,
    uploads_only: bool = False,
    report_path: Optional[Path] = None,
    verbose: bool = False,
) -> Dict[str, int]:
    legacy = rp.load_legacy_state()
    uploaded_docs = legacy.get("uploaded_docs") or {}

    counters = {
        "parse_completed_mails": 0,
        "preprocess_completed_created": 0,
        "preprocess_already_present": 0,
        "preprocess_output_missing": 0,
        "candidate_completed_created": 0,
        "upload_completed_created": 0,
        "upload_pending_created": 0,
        "upload_already_present": 0,
        "upload_skipped_disabled_index": 0,
    }
    details: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    ps.ensure_schema(cfg)
    conn = ps.connect(cfg)
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            f"""
            SELECT p.mail_id, p.output_ref, p.output_hash, pp.status AS preprocess_status
            FROM `{ps.TASK_TABLE}` p
            LEFT JOIN `{ps.TASK_TABLE}` pp
                   ON pp.mail_id = p.mail_id AND pp.stage = %s AND pp.item_key = ''
            WHERE p.stage = %s AND p.item_key = '' AND p.status = 'COMPLETED'
              AND p.output_ref IS NOT NULL
            ORDER BY p.mail_id
            """,
            (rp.STAGE_PREPROCESS, rp.STAGE_PARSE),
        )
        parse_rows = cur.fetchall()

        for n, row in enumerate(parse_rows, 1):
            counters["parse_completed_mails"] += 1
            mail_id = int(row["mail_id"])
            preprocess_done = row.get("preprocess_status") not in (None, "PENDING", "RETRY")
            if uploads_only:
                # 전처리가 끝난 메일에 업로드 대상 인덱스용 작업만 추가
                if row.get("preprocess_status") != "COMPLETED":
                    continue
            elif preprocess_done:
                counters["preprocess_already_present"] += 1
                continue

            export_dir = Path(row["output_ref"])
            collected = _collect_outputs(export_dir) if export_dir.is_dir() else None
            if collected is None:
                counters["preprocess_output_missing"] += 1
                details["preprocess_output_missing"].append(
                    {"mail_id": mail_id, "export_dir": str(export_dir)})
                continue
            counters["upload_skipped_disabled_index"] += collected["skipped_disabled"]

            if uploads_only:
                _add_upload_tasks(cur, mail_id, collected["uploads"], uploaded_docs,
                                  mark_all_uploaded, counters, details)
                if apply and n % COMMIT_EVERY == 0:
                    conn.commit()
                continue

            _upsert_completed(
                cur, mail_id, rp.STAGE_PREPROCESS, "",
                input_ref=str(export_dir),
                input_hash=row.get("output_hash"),  # seed 규칙과 동일: PARSE output_hash
                output_ref=os.path.dirname(collected["full_paths"][0]),
                output_hash=collected["output_hash"],
                checkpoint={"outputs": collected["outputs"], "degraded_docs": 0},
            )
            counters["preprocess_completed_created"] += 1

            if _upsert_completed(
                cur, mail_id, rp.STAGE_CANDIDATE, "",
                input_ref=json.dumps({"full": collected["full_paths"],
                                      "category": collected["category"],
                                      "legacy_reused": True}, ensure_ascii=False),
                input_hash=collected["output_hash"],
            ):
                counters["candidate_completed_created"] += 1

            _add_upload_tasks(cur, mail_id, collected["uploads"], uploaded_docs,
                              mark_all_uploaded, counters, details)

            if apply and n % COMMIT_EVERY == 0:
                conn.commit()

        if apply:
            conn.commit()
        else:
            conn.rollback()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

    if report_path is not None:
        ps.atomic_write_text(
            Path(report_path),
            json.dumps({"apply": apply, "counters": counters, "details": details},
                       ensure_ascii=False, indent=2),
        )
    _print_summary(counters, details, apply, report_path, verbose, bool(legacy))
    return counters


def _print_summary(counters, details, apply, report_path, verbose, has_legacy) -> None:
    print(f"업로드 대상 인덱스(RAG_UPLOAD_INDEXES): {rp.UPLOAD_INDEXES}")
    print(("" if apply else "[DRY-RUN] ") + json.dumps(counters, ensure_ascii=False))
    for category in ("preprocess_output_missing", "upload_pending_created"):
        rows = details.get(category) or []
        if not rows:
            continue
        shown = rows if verbose else rows[:SAMPLES_PER_CATEGORY]
        print(f"\n[{category}] {len(rows)}건" + ("" if verbose else f" (예시 {len(shown)}건)"))
        for r in shown:
            print("  " + json.dumps(r, ensure_ascii=False))
    if not has_legacy:
        print(f"\n※ 예전 상태 파일이 없습니다({rp.LEGACY_STATE_FILE}). "
              "업로드 성공 여부를 알 수 없어 모든 업로드가 PENDING(다시 업로드)이 됩니다. "
              "이미 인덱스에 올라가 있다면 --mark-all-uploaded 를 검토하세요.")
    if counters["upload_pending_created"]:
        print("\n※ upload_pending_created: 다음 run_pipeline.py 실행에서 업로드됩니다. "
              "insert-doc 이 같은 doc_id를 덮어쓰는지(P0-2) 확인 후 진행하세요.")
    if counters["preprocess_output_missing"]:
        print("※ preprocess_output_missing: 전처리 출력이 없는 메일입니다. run_pipeline.py가 LLM으로 새로 처리합니다.")
    if not apply:
        print("\n실제 반영하려면 --apply 를 붙여 다시 실행하세요.")
    if report_path is not None:
        print(f"전체 목록: {report_path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--apply", action="store_true", help="DB에 실제 반영 (없으면 dry-run)")
    parser.add_argument("--mark-all-uploaded", action="store_true",
                        help="예전 상태 파일에 기록이 없어도 모든 업로드를 완료로 처리")
    parser.add_argument("--uploads-only", action="store_true",
                        help="전처리가 끝난 메일에 업로드 대상 인덱스용 UPLOAD 작업만 추가")
    parser.add_argument("--report", default=str(DEFAULT_REPORT_PATH))
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    backfill(
        ps.DBConfig.from_env(),
        apply=args.apply,
        mark_all_uploaded=args.mark_all_uploaded,
        uploads_only=args.uploads_only,
        report_path=Path(args.report) if args.report else None,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    main()
