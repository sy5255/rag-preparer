import os
import sys
import uuid

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pipeline_state as ps  # noqa: E402


@pytest.fixture
def db_cfg():
    """
    PIPELINE_TEST_MYSQL_* 환경변수가 있을 때만 DB 테스트를 실행합니다.
    테스트마다 임시 database를 만들고 끝나면 삭제합니다.
    """
    host = os.getenv("PIPELINE_TEST_MYSQL_HOST")
    if not host:
        pytest.skip("PIPELINE_TEST_MYSQL_HOST is not set")
    import mysql.connector

    port = int(os.getenv("PIPELINE_TEST_MYSQL_PORT", "3306"))
    user = os.getenv("PIPELINE_TEST_MYSQL_USER", "root")
    password = os.getenv("PIPELINE_TEST_MYSQL_PASSWORD", "")
    name = f"ragtest_{uuid.uuid4().hex[:8]}"

    admin = mysql.connector.connect(host=host, port=port, user=user, password=password)
    cur = admin.cursor()
    cur.execute(f"CREATE DATABASE `{name}` DEFAULT CHARSET utf8mb4")
    cur.close()
    cfg = ps.DBConfig(host, port, name, user, password)
    ps.ensure_schema(cfg)
    try:
        yield cfg
    finally:
        cur = admin.cursor()
        cur.execute(f"DROP DATABASE `{name}`")
        cur.close()
        admin.close()


def query(cfg, sql, params=()):
    conn = ps.connect(cfg)
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(sql, params)
        rows = cur.fetchall() if cur.with_rows else []
        conn.commit()
        return rows
    finally:
        cur.close()
        conn.close()
