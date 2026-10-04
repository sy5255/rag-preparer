import json
from pathlib import Path

import pytest

import build_serving_views
import normalize_docs
import pipeline_state as ps
import run_pipeline
from conftest import query


class _Resp:
    def __init__(self, status_code, text=""):
        self.status_code = status_code
        self.text = text


@pytest.fixture
def env(tmp_path, monkeypatch, db_cfg):
    parse_root = tmp_path / "parsing_archive"
    out_root = tmp_path / "preprocessed_jsonl"
    monkeypatch.setattr(normalize_docs, "WATCH_ROOT", str(parse_root))
    monkeypatch.setattr(normalize_docs, "OUT_ROOT", str(out_root))
    monkeypatch.setattr(run_pipeline, "LEGACY_STATE_FILE", str(out_root / "_state_processed.json"))

    llm = {"fail": 0, "calls": 0}

    def fake_full(title, content, category):
        llm["calls"] += 1
        if llm["fail"] > 0:
            llm["fail"] -= 1
            raise RuntimeError("llm down")
        return {"doc_type": "report", "summary": "S", "defect_candidates": []}

    monkeypatch.setattr(build_serving_views, "build_additional_full", fake_full)
    monkeypatch.setattr(build_serving_views, "build_additional_lite",
                        lambda title, content, category=None: {"doc_type": "lite"})

    candidates = []
    monkeypatch.setattr(run_pipeline, "collect_candidates_from_additional",
                        lambda **kw: candidates.append(kw["doc"]["doc_id"]))

    uploads = {"calls": [], "fail_ids": set(), "status": 500}

    def fake_post(url, headers=None, data=None, timeout=None):
        body = json.loads(data)
        key = f"{body['index_name']}::{body['data']['doc_id']}"
        uploads["calls"].append(key)
        if body["data"]["doc_id"] in uploads["fail_ids"]:
            return _Resp(uploads["status"], "err")
        return _Resp(200)

    monkeypatch.setattr(run_pipeline.requests, "post", fake_post)
    return {"cfg": db_cfg, "parse_root": parse_root, "out_root": out_root,
            "llm": llm, "candidates": candidates, "uploads": uploads}


def _parse_done(env, mail_id=1, docs=("d1",), name="m1"):
    """doc-parser가 PARSE를 끝낸 상태를 만든다."""
    export_dir = env["parse_root"] / "inline_fa_report" / "ver3" / name / "export_r1"
    (export_dir / "doc").mkdir(parents=True)
    lines = "".join(
        json.dumps({"doc_id": d, "title": f"{d}.enriched.eml", "content": "body",
                    "additionalField": {"storage": {"mail_uid": "u"}}}) + "\n"
        for d in docs
    )
    (export_dir / "doc" / "report.jsonl").write_text(lines, encoding="utf-8")
    query(env["cfg"],
          "INSERT INTO ae_llm_agent_pipeline_task(mail_id, stage, item_key, status, output_ref,"
          " output_hash) VALUES(%s,'PARSE','','COMPLETED',%s,%s)",
          (mail_id, str(export_dir), "h1"))
    return export_dir


def _tasks(cfg, stage):
    return query(cfg, "SELECT * FROM ae_llm_agent_pipeline_task WHERE stage=%s ORDER BY item_key",
                 (stage,))


def test_full_flow_creates_outputs_and_uploads_each_doc(env):
    _parse_done(env, docs=("d1", "d2"))

    counters = run_pipeline.run_once(env["cfg"])

    pre = _tasks(env["cfg"], "PREPROCESS")
    assert [t["status"] for t in pre] == ["COMPLETED"]
    assert pre[0]["quality"] == "NORMAL"
    out_dir = env["out_root"] / "inline_fa_report" / "ver3" / "m1" / "doc"
    for mode in ("raw", "full", "lite"):
        assert (out_dir / f"report__{mode}.jsonl").exists()

    uploads = _tasks(env["cfg"], "UPLOAD")
    assert len(uploads) == 6  # 2 docs × 3 indexes
    assert all(t["status"] == "COMPLETED" for t in uploads)
    assert sorted(env["uploads"]["calls"]) == sorted(t["item_key"] for t in uploads)
    assert env["candidates"] == ["d1", "d2"]
    assert counters["upload_completed"] == 6

    # 다시 실행해도 아무것도 다시 하지 않음
    env["uploads"]["calls"].clear()
    run_pipeline.run_once(env["cfg"])
    assert env["uploads"]["calls"] == []


def test_failed_upload_is_tracked_and_retried_per_doc(env):
    _parse_done(env, docs=("d1", "d2"))
    env["uploads"]["fail_ids"] = {"d2"}

    run_pipeline.run_once(env["cfg"])
    rows = {t["item_key"]: t for t in _tasks(env["cfg"], "UPLOAD")}
    failed = [k for k, t in rows.items() if t["status"] == "RETRY"]
    assert len(failed) == 3 and all(k.endswith("::d2") for k in failed)
    assert all("status=500" in rows[k]["last_error"] for k in failed)

    # 다음 실행: 실패한 문서만 다시 전송
    env["uploads"]["fail_ids"] = set()
    env["uploads"]["calls"].clear()
    query(env["cfg"], "UPDATE ae_llm_agent_pipeline_task SET next_retry_at=NULL")
    run_pipeline.run_once(env["cfg"])
    assert sorted(env["uploads"]["calls"]) == sorted(failed)
    rows = _tasks(env["cfg"], "UPLOAD")
    assert all(t["status"] == "COMPLETED" for t in rows)
    assert sorted(t["attempt"] for t in rows) == [0, 0, 0, 1, 1, 1]


def test_client_error_is_permanent(env):
    _parse_done(env)
    env["uploads"]["fail_ids"] = {"d1"}
    env["uploads"]["status"] = 400
    run_pipeline.run_once(env["cfg"])
    rows = _tasks(env["cfg"], "UPLOAD")
    assert all(t["status"] == "FAILED" and t["error_class"] == "PERMANENT" for t in rows)


def test_llm_failure_retries_then_degrades_on_last_attempt(env, monkeypatch):
    monkeypatch.setattr(run_pipeline, "MAX_ATTEMPT", 2)
    _parse_done(env)
    env["llm"]["fail"] = 100

    run_pipeline.run_once(env["cfg"])
    pre = _tasks(env["cfg"], "PREPROCESS")[0]
    assert pre["status"] == "RETRY"
    assert "llm down" in pre["last_error"]
    assert _tasks(env["cfg"], "UPLOAD") == []  # 실패한 결과는 업로드되지 않음

    query(env["cfg"], "UPDATE ae_llm_agent_pipeline_task SET next_retry_at=NULL")
    run_pipeline.run_once(env["cfg"])
    pre = _tasks(env["cfg"], "PREPROCESS")[0]
    assert pre["status"] == "COMPLETED"
    assert pre["quality"] == "DEGRADED"  # 마지막 시도에서만 lite 대체 + 표시


def test_reparse_resets_preprocess_and_changed_uploads(env):
    export_dir = _parse_done(env)
    run_pipeline.run_once(env["cfg"])

    # doc-parser가 같은 메일을 다시 파싱 → 내용 변경
    (export_dir / "doc" / "report.jsonl").write_text(
        json.dumps({"doc_id": "d1", "title": "t", "content": "new body"}) + "\n", encoding="utf-8")
    query(env["cfg"], "UPDATE ae_llm_agent_pipeline_task SET output_hash='h2' WHERE stage='PARSE'")
    env["uploads"]["calls"].clear()

    run_pipeline.run_once(env["cfg"])
    assert _tasks(env["cfg"], "PREPROCESS")[0]["status"] == "COMPLETED"
    assert len(env["uploads"]["calls"]) == 3  # 바뀐 문서는 다시 업로드


def test_killed_run_orphans_are_recovered(env):
    _parse_done(env)
    with ps.PipelineRun(env["cfg"], "rag-preparer", time_budget_sec=60) as run:
        q = ps.TaskQueue(env["cfg"], run)
        run_pipeline.seed_preprocess(q)
        q.claim_next("PREPROCESS")  # 처리 도중 죽은 상황

    counters = run_pipeline.run_once(env["cfg"])
    assert counters["recovered"] == 1
    pre = _tasks(env["cfg"], "PREPROCESS")[0]
    assert pre["status"] == "COMPLETED" and pre["attempt"] == 1


def test_legacy_state_reuses_outputs_and_skips_uploaded_docs(env):
    export_dir = _parse_done(env, docs=("d1", "d2"))
    jsonl = export_dir / "doc" / "report.jsonl"

    # 예전 upload_indices.py가 이미 처리한 상태: 출력 존재, d1만 업로드 성공
    out_dir = env["out_root"] / "inline_fa_report" / "ver3" / "m1" / "doc"
    out_dir.mkdir(parents=True)
    for mode in ("raw", "full", "lite"):
        (out_dir / f"report__{mode}.jsonl").write_text(
            "".join(json.dumps({"doc_id": d, "title": "t", "content": "c"}) + "\n"
                    for d in ("d1", "d2")), encoding="utf-8")
    uploaded = {f"rp-ifa-ver3-{m}::d1": True for m in ("raw", "full", "lite")}
    (env["out_root"] / "_state_processed.json").write_text(json.dumps({
        "processed_inputs": {str(jsonl): __import__("upload_indices").file_signature(str(jsonl))},
        "uploaded_docs": uploaded,
    }), encoding="utf-8")

    run_pipeline.run_once(env["cfg"])

    assert env["llm"]["calls"] == 0  # LLM 재호출 없음
    assert env["candidates"] == []  # 후보 중복 적재 없음
    assert sorted(env["uploads"]["calls"]) == sorted(
        f"rp-ifa-ver3-{m}::d2" for m in ("raw", "full", "lite"))  # 예전에 빠진 d2만 업로드
    rows = {t["item_key"]: t["quality"] for t in _tasks(env["cfg"], "UPLOAD")}
    assert rows["rp-ifa-ver3-raw::d1"] == "LEGACY"
    assert rows["rp-ifa-ver3-raw::d2"] == "NORMAL"
