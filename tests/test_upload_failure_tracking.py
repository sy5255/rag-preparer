import json
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import upload_indices  # noqa: E402


class _Resp:
    def __init__(self, status_code, text=""):
        self.status_code = status_code
        self.text = text


@pytest.fixture
def state_file(tmp_path, monkeypatch):
    path = tmp_path / "_state_processed.json"
    monkeypatch.setattr(upload_indices, "STATE_FILE", str(path))
    monkeypatch.setattr(upload_indices, "OUT_ROOT", str(tmp_path))
    return path


def _write_jsonl(path, docs):
    path.write_text("".join(json.dumps(d) + "\n" for d in docs), encoding="utf-8")


def test_failed_upload_is_counted_and_recorded(tmp_path, state_file, monkeypatch):
    jsonl = tmp_path / "a__raw.jsonl"
    _write_jsonl(jsonl, [{"doc_id": "d1", "title": "t", "content": "c"},
                         {"doc_id": "d2", "title": "t", "content": "c"}])

    def fake_post(url, headers=None, data=None, timeout=None):
        doc_id = json.loads(data)["data"]["doc_id"]
        return _Resp(200) if doc_id == "d1" else _Resp(500, "boom")

    monkeypatch.setattr(upload_indices.requests, "post", fake_post)
    state = upload_indices.load_state()

    failed = upload_indices.upload_jsonl_to_index(str(jsonl), "idx", state, mode="raw")

    assert failed == 1
    saved = json.loads(state_file.read_text(encoding="utf-8"))
    assert saved["uploaded_docs"] == {"idx::d1": True}
    assert saved["failed_docs"]["idx::d2"]["attempts"] == 1
    assert "status=500" in saved["failed_docs"]["idx::d2"]["last_error"]


def test_retry_success_clears_failure_and_skips_done_docs(tmp_path, state_file, monkeypatch):
    jsonl = tmp_path / "a__raw.jsonl"
    _write_jsonl(jsonl, [{"doc_id": "d1"}, {"doc_id": "d2"}])
    calls = []

    def first(url, headers=None, data=None, timeout=None):
        doc_id = json.loads(data)["data"]["doc_id"]
        calls.append(doc_id)
        return _Resp(200) if doc_id == "d1" else _Resp(503)

    monkeypatch.setattr(upload_indices.requests, "post", first)
    state = upload_indices.load_state()
    assert upload_indices.upload_jsonl_to_index(str(jsonl), "idx", state, mode="raw") == 1

    calls.clear()
    monkeypatch.setattr(
        upload_indices.requests, "post",
        lambda url, headers=None, data=None, timeout=None: (calls.append(json.loads(data)["data"]["doc_id"]) or _Resp(200)),
    )
    state = upload_indices.load_state()
    assert upload_indices.upload_jsonl_to_index(str(jsonl), "idx", state, mode="raw") == 0
    assert calls == ["d2"]  # 이미 성공한 d1은 다시 보내지 않음
    saved = json.loads(state_file.read_text(encoding="utf-8"))
    assert saved["failed_docs"] == {}


def test_missing_doc_id_is_recorded_but_does_not_block(tmp_path, state_file, monkeypatch):
    jsonl = tmp_path / "a__raw.jsonl"
    _write_jsonl(jsonl, [{"title": "no id"}])
    monkeypatch.setattr(upload_indices.requests, "post", lambda *a, **k: _Resp(200))
    state = upload_indices.load_state()
    assert upload_indices.upload_jsonl_to_index(str(jsonl), "idx", state, mode="raw") == 0
    assert any(k.endswith("::missing_doc_id") for k in state["failed_inputs"])


def test_main_loop_does_not_mark_file_processed_when_upload_fails(tmp_path, state_file, monkeypatch):
    src = tmp_path / "in.jsonl"
    src.write_text("{}\n", encoding="utf-8")
    monkeypatch.setattr(upload_indices, "iter_jsonl_files", lambda root: [str(src)])
    monkeypatch.setattr(upload_indices, "preprocess_jsonl_file", lambda fp: ("r", "f", "l"))
    monkeypatch.setattr(upload_indices, "upload_raw_full_lite_outputs", lambda *a, **k: 2)
    monkeypatch.setattr(upload_indices, "get_category_from_path", lambda fp: "c")
    monkeypatch.setattr(upload_indices, "get_version_from_path", lambda fp: "ver3")
    monkeypatch.setattr(upload_indices.time, "sleep", lambda s: (_ for _ in ()).throw(KeyboardInterrupt) if s == upload_indices.POLL_INTERVAL_SEC else None)

    with pytest.raises(KeyboardInterrupt):
        upload_indices.main()

    saved = json.loads(state_file.read_text(encoding="utf-8"))
    assert str(src) not in saved["processed_inputs"]
    assert saved["failed_inputs"][str(src)]["attempts"] == 1
