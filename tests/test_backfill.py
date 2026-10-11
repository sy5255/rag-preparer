import json

import pytest

import backfill_from_files as bf
import build_serving_views
import normalize_docs
import run_pipeline
from conftest import query


class _Resp:
    status_code = 200
    text = ""


@pytest.fixture
def env(tmp_path, monkeypatch, db_cfg):
    parse_root = tmp_path / "parsing_archive"
    out_root = tmp_path / "preprocessed_jsonl"
    monkeypatch.setattr(normalize_docs, "WATCH_ROOT", str(parse_root))
    monkeypatch.setattr(normalize_docs, "OUT_ROOT", str(out_root))
    monkeypatch.setattr(run_pipeline, "LEGACY_STATE_FILE", str(out_root / "_state_processed.json"))
    monkeypatch.setattr(run_pipeline, "UPLOAD_INDEXES", ["*"])  # 3개 인덱스 모두 대상

    calls = {"llm": 0, "upload": [], "candidates": 0}

    def no_llm(*a, **k):
        calls["llm"] += 1
        return {"doc_type": "x"}

    monkeypatch.setattr(build_serving_views, "build_additional_full", no_llm)
    monkeypatch.setattr(build_serving_views, "build_additional_lite", lambda *a, **k: {})
    monkeypatch.setattr(run_pipeline, "collect_candidates_from_additional",
                        lambda **kw: calls.__setitem__("candidates", calls["candidates"] + 1))

    def fake_post(url, headers=None, data=None, timeout=None):
        body = json.loads(data)
        calls["upload"].append(f"{body['index_name']}::{body['data']['doc_id']}")
        return _Resp()

    monkeypatch.setattr(run_pipeline.requests, "post", fake_post)
    return {"cfg": db_cfg, "parse_root": parse_root, "out_root": out_root, "calls": calls}


def _mail(env, mail_id, docs, with_outputs=True):
    name = f"m{mail_id}"
    export = env["parse_root"] / "inline_fa_report" / "ver3" / name / f"export_t{mail_id}"
    (export / "doc").mkdir(parents=True)
    lines = "".join(json.dumps({"doc_id": d, "title": "t", "content": "c"}) + "\n" for d in docs)
    (export / "doc" / "r.jsonl").write_text(lines, encoding="utf-8")
    if with_outputs:
        out = env["out_root"] / "inline_fa_report" / "ver3" / name / "doc"
        out.mkdir(parents=True)
        for mode in ("raw", "full", "lite"):
            (out / f"r__{mode}.jsonl").write_text(lines, encoding="utf-8")
    query(env["cfg"],
          "INSERT INTO ae_llm_agent_pipeline_task(mail_id, stage, item_key, status, output_ref,"
          " output_hash, quality) VALUES(%s,'PARSE','','COMPLETED',%s,%s,'LEGACY')",
          (mail_id, str(export), f"h{mail_id}"))


def _tasks(cfg, stage):
    return query(cfg, "SELECT * FROM ae_llm_agent_pipeline_task WHERE stage=%s ORDER BY id", (stage,))


def test_backfill_then_pipeline_does_no_llm_and_uploads_only_missing(env, tmp_path):
    _mail(env, 1, ["d1", "d2"])
    _mail(env, 2, ["d3"], with_outputs=False)  # 전처리 출력 없음 → 건드리지 않음
    env["out_root"].mkdir(parents=True, exist_ok=True)
    (env["out_root"] / "_state_processed.json").write_text(json.dumps({
        "uploaded_docs": {f"rp-ifa-ver3-{m}::d1": True for m in ("raw", "full", "lite")},
    }), encoding="utf-8")

    dry = bf.backfill(env["cfg"], apply=False, report_path=tmp_path / "r.json")
    assert dry["preprocess_completed_created"] == 1
    assert _tasks(env["cfg"], "PREPROCESS") == []  # dry-run은 반영 안 함

    c = bf.backfill(env["cfg"], apply=True)
    assert c["parse_completed_mails"] == 2
    assert c["preprocess_completed_created"] == 1
    assert c["preprocess_output_missing"] == 1
    assert c["candidate_completed_created"] == 1
    assert c["upload_completed_created"] == 3  # d1 × 3 인덱스 (예전 업로드 성공)
    assert c["upload_pending_created"] == 3    # d2 × 3 인덱스 (예전에 빠진 문서)

    pre = _tasks(env["cfg"], "PREPROCESS")
    assert [(p["mail_id"], p["status"], p["quality"]) for p in pre] == [(1, "COMPLETED", "LEGACY")]

    # 재실행해도 중복 없음
    again = bf.backfill(env["cfg"], apply=True)
    assert again["preprocess_already_present"] == 1 and again["upload_pending_created"] == 0

    # 파이프라인 실행: mail 1은 LLM·후보 적재 없이 d2만 업로드, mail 2는 정상 처리(LLM)
    run_pipeline.run_once(env["cfg"])
    calls = env["calls"]
    assert calls["llm"] == 1  # mail 2의 d3 한 건만
    uploads_mail1 = sorted(k for k in calls["upload"] if k.endswith(("::d1", "::d2")))
    assert uploads_mail1 == sorted(f"rp-ifa-ver3-{m}::d2" for m in ("raw", "full", "lite"))
    assert calls["candidates"] == 1  # mail 2의 d3 한 건만

    rows = _tasks(env["cfg"], "UPLOAD")
    assert all(r["status"] == "COMPLETED" for r in rows)
    pre = {p["mail_id"]: p for p in _tasks(env["cfg"], "PREPROCESS")}
    assert pre[1]["quality"] == "LEGACY" and pre[1]["attempt"] == 0
    assert pre[2]["status"] == "COMPLETED"


def test_mark_all_uploaded(env):
    _mail(env, 1, ["d1"])
    c = bf.backfill(env["cfg"], apply=True, mark_all_uploaded=True)
    assert c["upload_completed_created"] == 3 and c["upload_pending_created"] == 0
    run_pipeline.run_once(env["cfg"])
    assert env["calls"]["upload"] == [] and env["calls"]["llm"] == 0


def test_only_configured_indexes_are_counted_and_uploaded(env, monkeypatch):
    monkeypatch.setattr(run_pipeline, "UPLOAD_INDEXES", ["rp-ifa-ver3-full"])
    _mail(env, 1, ["d1", "d2"])
    env["out_root"].mkdir(parents=True, exist_ok=True)
    (env["out_root"] / "_state_processed.json").write_text(json.dumps({
        "uploaded_docs": {"rp-ifa-ver3-full::d1": True, "rp-ifa-ver3-lite::d1": True},
    }), encoding="utf-8")

    c = bf.backfill(env["cfg"], apply=True)
    assert c["upload_completed_created"] == 1   # full::d1
    assert c["upload_pending_created"] == 1     # full::d2
    assert c["upload_skipped_disabled_index"] == 4  # raw·lite × 2 문서
    keys = sorted(r["item_key"] for r in _tasks(env["cfg"], "UPLOAD"))
    assert keys == ["rp-ifa-ver3-full::d1", "rp-ifa-ver3-full::d2"]

    run_pipeline.run_once(env["cfg"])
    assert env["calls"]["upload"] == ["rp-ifa-ver3-full::d2"]

    # 나중에 lite 추가 → --uploads-only 로 lite 작업만 추가 (lite::d1은 예전 기록이 있어 완료)
    monkeypatch.setattr(run_pipeline, "UPLOAD_INDEXES", ["rp-ifa-ver3-full", "rp-ifa-ver3-lite"])
    c = bf.backfill(env["cfg"], apply=True, uploads_only=True)
    assert c["upload_completed_created"] == 1   # lite::d1
    assert c["upload_pending_created"] == 1     # lite::d2
    assert c["upload_already_present"] == 2    # full 두 건은 이미 있음
    assert c["preprocess_completed_created"] == 0

    env["calls"]["upload"].clear()
    run_pipeline.run_once(env["cfg"])
    assert env["calls"]["upload"] == ["rp-ifa-ver3-lite::d2"]


def test_existing_tasks_for_disabled_index_are_not_claimed(env, monkeypatch):
    _mail(env, 1, ["d1"])
    bf.backfill(env["cfg"], apply=True)  # 3개 인덱스 모두 PENDING
    monkeypatch.setattr(run_pipeline, "UPLOAD_INDEXES", ["rp-ifa-*-full"])
    run_pipeline.run_once(env["cfg"])
    assert env["calls"]["upload"] == ["rp-ifa-ver3-full::d1"]
    pending = sorted(r["item_key"] for r in _tasks(env["cfg"], "UPLOAD") if r["status"] == "PENDING")
    assert pending == ["rp-ifa-ver3-lite::d1", "rp-ifa-ver3-raw::d1"]


def test_one_broken_output_file_does_not_stop_backfill(env):
    _mail(env, 1, ["d1"])
    _mail(env, 2, ["d2"])
    _mail(env, 3, ["d3"])
    broken = env["out_root"] / "inline_fa_report" / "ver3" / "m2" / "doc" / "r__full.jsonl"
    broken.write_text("{not json\n", encoding="utf-8")

    c = bf.backfill(env["cfg"], apply=True)
    assert c["preprocess_read_error"] == 1
    assert c["preprocess_completed_created"] == 2  # 1, 3은 정상 처리
    done = sorted(t["mail_id"] for t in _tasks(env["cfg"], "PREPROCESS"))
    assert done == [1, 3]
