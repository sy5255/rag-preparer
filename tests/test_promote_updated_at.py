"""승격이 사전·별칭을 쓸 때 updated_at도 함께 기록한다 (DB 없이 SQL만 확인)."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "term_dictionary"))

import promote_candidate_terms as promote  # noqa: E402


class FakeCursor:
    def __init__(self, existing_id=None):
        self.existing_id, self.sql, self.lastrowid = existing_id, [], 77

    def execute(self, sql, params=()):
        self.sql.append(" ".join(sql.split()))

    def fetchone(self):
        return (self.existing_id,) if self.existing_id else None


TERM = {"term_type": "defect", "canonical_name": "SF2", "scope": "all"}
ALIAS = {"alias_text": "SF2"}


def _writes(cur):
    return [s for s in cur.sql if s.startswith(("UPDATE", "INSERT"))]


def test_new_term_and_alias_record_updated_at():
    cur = FakeCursor()
    promote.upsert_term(cur, TERM)
    promote.upsert_alias(cur, 77, ALIAS)
    assert all("updated_at" in s and "CURRENT_TIMESTAMP" in s for s in _writes(cur))
    assert len(_writes(cur)) == 2


def test_existing_term_and_alias_record_updated_at():
    cur = FakeCursor(existing_id=5)
    promote.upsert_term(cur, TERM)
    promote.upsert_alias(cur, 5, ALIAS)
    assert all("updated_at = CURRENT_TIMESTAMP" in s for s in _writes(cur))
    assert len(_writes(cur)) == 2
