import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "term_dictionary"))

import pipeline_state as ps  # noqa: E402
import promote_candidate_terms as promote  # noqa: E402
from conftest import query  # noqa: E402


@pytest.fixture
def queue_table(db_cfg, monkeypatch):
    query(db_cfg, """
        CREATE TABLE term_candidate_queue (
            candidate_id BIGINT PRIMARY KEY,
            review_status VARCHAR(30),
            promoted_term_id BIGINT NULL,
            promoted_at DATETIME NULL,
            draft_description TEXT NULL
        )""")
    query(db_cfg, "INSERT INTO term_candidate_queue(candidate_id, review_status) "
                  "VALUES(1,'approved'),(2,'approved'),(3,'approved')")
    monkeypatch.setattr(promote, "get_mysql_conn", lambda: ps.connect(db_cfg))
    return db_cfg


def test_one_bad_candidate_does_not_block_others(queue_table, monkeypatch):
    def fake_promote_row(cur, row, candidate_id):
        cur.execute("UPDATE term_candidate_queue SET draft_description='touched' "
                    "WHERE candidate_id=%s", (candidate_id,))
        if candidate_id == 2:
            raise ValueError("bad canonical")
        promote.mark_promoted(cur, candidate_id, 100 + candidate_id)
        return True

    monkeypatch.setattr(promote, "_promote_row", fake_promote_row)

    assert promote.promote_once() == 2

    rows = {r["candidate_id"]: r for r in query(queue_table, "SELECT * FROM term_candidate_queue")}
    assert rows[1]["review_status"] == "promoted" and rows[1]["promoted_term_id"] == 101
    assert rows[3]["review_status"] == "promoted"
    assert rows[2]["review_status"] == "promote_failed"
    assert "bad canonical" in rows[2]["promote_error"]
    assert rows[2]["draft_description"] is None  # 실패한 후보의 부분 변경은 롤백

    # 다음 주기: 실패 후보는 다시 승인되기 전까지 재시도하지 않음
    assert promote.promote_once() == 0
