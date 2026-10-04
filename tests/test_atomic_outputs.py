import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import build_serving_views  # noqa: E402


def test_atomic_write_lines_leaves_no_tmp(tmp_path):
    target = tmp_path / "x__full.jsonl"
    build_serving_views._atomic_write_lines(str(target), ['{"a":1}', '{"b":2}'])
    assert target.read_text(encoding="utf-8") == '{"a":1}\n{"b":2}\n'
    assert not (tmp_path / "x__full.jsonl.tmp").exists()
