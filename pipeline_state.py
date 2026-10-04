#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
파이프라인 단계 상태 추적 공통 모듈.

이 파일은 doc-parser / rag-preparer 저장소에 **동일한 사본**으로 들어 있습니다.
수정할 때는 두 저장소의 사본을 함께 갱신하세요. (PIPELINE_WORKPLAN.md 참고)

구성
- ae_llm_agent_pipeline_task    : 단계별 작업 단위의 현재 상태
- ae_llm_agent_pipeline_attempt : 시도 이력 (append-only)
- ae_llm_agent_pipeline_run     : 잡 실행 기록
- v_ae_llm_agent_pipeline_mail  : 메일 1건의 전체 단계 진행 현황 뷰

사용 예
    cfg = DBConfig.from_env()
    ensure_schema(cfg)
    with PipelineRun(cfg, "doc-parser", time_budget_sec=50 * 60) as run:
        if not run.acquired:
            return
        queue = TaskQueue(cfg, run)
        queue.recover_orphans(["PARSE"])
        queue.seed("PARSE", "SELECT id AS mail_id, '' AS item_key, "
                            "path AS input_ref, raw_hash AS input_hash FROM ...")
        while not run.deadline_reached():
            task = queue.claim_next("PARSE")
            if task is None:
                break
            try:
                ...
                queue.complete(task, output_ref=...)
            except PermanentError as e:
                queue.fail(task, e, permanent=True)
            except Exception as e:
                queue.fail(task, e)
"""

import hashlib
import json
import os
import shutil
import socket
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

import mysql.connector
from mysql.connector.constants import ClientFlag


TASK_TABLE = "ae_llm_agent_pipeline_task"
ATTEMPT_TABLE = "ae_llm_agent_pipeline_attempt"
RUN_TABLE = "ae_llm_agent_pipeline_run"
MAIL_TABLE = "ae_llm_agent_mail"
MAIL_VIEW = "v_ae_llm_agent_pipeline_mail"

DEFAULT_MAX_ATTEMPT = int(os.getenv("PIPELINE_MAX_ATTEMPT", "5"))
BACKOFF_BASE_SEC = int(os.getenv("PIPELINE_BACKOFF_BASE_SEC", str(5 * 60)))
BACKOFF_MAX_SEC = int(os.getenv("PIPELINE_BACKOFF_MAX_SEC", str(6 * 3600)))


# =========================
# 오류 분류
# =========================
class TransientError(Exception):
    """일시 오류: backoff 후 재시도합니다."""


class PermanentError(Exception):
    """영구 오류: 재시도하지 않고 FAILED로 확정합니다."""


# =========================
# DB 설정
# =========================
def _env_first(*names: str, default: Optional[str] = None) -> Optional[str]:
    for name in names:
        value = os.getenv(name)
        if value not in (None, ""):
            return value
    return default


@dataclass(frozen=True)
class DBConfig:
    host: str
    port: int
    database: str
    user: str
    password: str

    @classmethod
    def from_env(cls) -> "DBConfig":
        # email-ingestion(MYSQL_DATABASE/MYSQL_PASSWORD)과
        # rag-preparer(MYSQL_DB/MYSQL_PASS)의 환경변수 이름을 모두 지원합니다.
        password = _env_first("MYSQL_PASSWORD", "MYSQL_PASS")
        if not password:
            raise RuntimeError(
                "MySQL password is not set. Set MYSQL_PASSWORD (or MYSQL_PASS)."
            )
        return cls(
            host=_env_first("MYSQL_HOST", default="127.0.0.1"),
            port=int(_env_first("MYSQL_PORT", default="3306")),
            database=_env_first("MYSQL_DATABASE", "MYSQL_DB", default="fspas"),
            user=_env_first("MYSQL_USER", default="dbuser"),
            password=password,
        )


def connect(cfg: DBConfig, *, found_rows: bool = False):
    """
    found_rows=True 이면 UPDATE의 rowcount가 "변경된 행"이 아니라 "조건에 맞은 행" 수가 됩니다.
    (같은 값으로 갱신해도 1을 반환 → 소유권 확인용)
    """
    kwargs = {}
    if found_rows:
        kwargs["client_flags"] = [ClientFlag.FOUND_ROWS]
    return mysql.connector.connect(
        host=cfg.host,
        port=cfg.port,
        database=cfg.database,
        user=cfg.user,
        password=cfg.password,
        autocommit=False,
        **kwargs,
    )


# =========================
# 스키마
# =========================
_DDL = [
    f"""
    CREATE TABLE IF NOT EXISTS `{TASK_TABLE}` (
        id               BIGINT AUTO_INCREMENT PRIMARY KEY,
        mail_id          BIGINT NOT NULL,
        stage            VARCHAR(30) NOT NULL,
        item_key         VARCHAR(255) NOT NULL DEFAULT '',
        status           VARCHAR(20) NOT NULL DEFAULT 'PENDING',
        attempt          INT NOT NULL DEFAULT 0,
        max_attempt      INT NOT NULL DEFAULT {DEFAULT_MAX_ATTEMPT},
        next_retry_at    DATETIME NULL,
        run_id           VARCHAR(64) NULL,
        heartbeat_at     DATETIME NULL,
        input_ref        VARCHAR(2000) NULL,
        input_hash       CHAR(64) NULL,
        output_ref       VARCHAR(2000) NULL,
        output_hash      CHAR(64) NULL,
        external_id      VARCHAR(255) NULL,
        checkpoint_json  JSON NULL,
        quality          VARCHAR(20) NULL,
        last_error       TEXT NULL,
        error_class      VARCHAR(20) NULL,
        created_at       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                             ON UPDATE CURRENT_TIMESTAMP,
        completed_at     DATETIME NULL,
        UNIQUE KEY uq_ae_llm_agent_pipeline_task (mail_id, stage, item_key),
        INDEX idx_ae_llm_agent_pipeline_task_queue (stage, status, next_retry_at),
        INDEX idx_ae_llm_agent_pipeline_task_run (status, run_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    f"""
    CREATE TABLE IF NOT EXISTS `{ATTEMPT_TABLE}` (
        id          BIGINT AUTO_INCREMENT PRIMARY KEY,
        task_id     BIGINT NOT NULL,
        run_id      VARCHAR(64) NOT NULL,
        attempt_no  INT NOT NULL,
        started_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        ended_at    DATETIME NULL,
        result      VARCHAR(20) NULL,
        error       TEXT NULL,
        INDEX idx_ae_llm_agent_pipeline_attempt_task (task_id),
        INDEX idx_ae_llm_agent_pipeline_attempt_run (run_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    f"""
    CREATE TABLE IF NOT EXISTS `{RUN_TABLE}` (
        run_id        VARCHAR(64) PRIMARY KEY,
        component     VARCHAR(50) NOT NULL,
        host          VARCHAR(255) NULL,
        pid           INT NULL,
        started_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        finished_at   DATETIME NULL,
        exit_reason   VARCHAR(30) NULL,
        error         TEXT NULL,
        counters_json JSON NULL,
        INDEX idx_ae_llm_agent_pipeline_run_component (component, started_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
]

# 메일 1건의 단계별 진행 현황 (ae_llm_agent_mail이 있을 때만 생성)
_VIEW_DDL = f"""
CREATE OR REPLACE VIEW `{MAIL_VIEW}` AS
SELECT
    m.id AS mail_id,
    m.original_subject,
    m.route_case,
    m.status AS archive_status,
    m.sharedworkspace_path,
    p.status AS parse_status,
    p.attempt AS parse_attempt,
    p.last_error AS parse_error,
    pp.status AS preprocess_status,
    pp.quality AS preprocess_quality,
    pp.last_error AS preprocess_error,
    c.status AS candidate_status,
    (SELECT COUNT(*) FROM `{TASK_TABLE}` u
      WHERE u.mail_id = m.id AND u.stage = 'UPLOAD') AS upload_total,
    (SELECT COUNT(*) FROM `{TASK_TABLE}` u
      WHERE u.mail_id = m.id AND u.stage = 'UPLOAD' AND u.status = 'COMPLETED') AS upload_completed,
    (SELECT COUNT(*) FROM `{TASK_TABLE}` u
      WHERE u.mail_id = m.id AND u.stage = 'UPLOAD' AND u.status = 'FAILED') AS upload_failed,
    GREATEST(
        COALESCE(m.updated_at, '1970-01-01'),
        COALESCE(p.updated_at, '1970-01-01'),
        COALESCE(pp.updated_at, '1970-01-01')
    ) AS last_updated_at
FROM `{MAIL_TABLE}` m
LEFT JOIN `{TASK_TABLE}` p  ON p.mail_id = m.id  AND p.stage = 'PARSE'      AND p.item_key = ''
LEFT JOIN `{TASK_TABLE}` pp ON pp.mail_id = m.id AND pp.stage = 'PREPROCESS' AND pp.item_key = ''
LEFT JOIN `{TASK_TABLE}` c  ON c.mail_id = m.id  AND c.stage = 'CANDIDATE'  AND c.item_key = ''
WHERE m.route_type = 'FILE_ARCHIVE'
"""


def _table_exists(cur, database: str, table: str) -> bool:
    cur.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema=%s AND table_name=%s LIMIT 1",
        (database, table),
    )
    return cur.fetchone() is not None


def ensure_schema(cfg: DBConfig) -> None:
    conn = connect(cfg)
    cur = conn.cursor()
    try:
        for ddl in _DDL:
            cur.execute(ddl)
        if _table_exists(cur, cfg.database, MAIL_TABLE):
            cur.execute(_VIEW_DDL)
        conn.commit()
    finally:
        cur.close()
        conn.close()


# =========================
# 실행 컨텍스트
# =========================
class PipelineRun:
    """
    1시간 주기 잡 1회 실행.

    - MySQL GET_LOCK으로 같은 component의 동시 실행을 막습니다.
      (프로세스가 죽으면 연결이 끊기면서 lock이 자동 해제됩니다.)
    - run 테이블에 시작/종료/종료 사유/카운터를 기록합니다.
    - 이전 실행이 강제 종료되어 finished_at이 비어 있으면 exit_reason='KILLED'로 표시합니다.
    """

    def __init__(
        self,
        cfg: DBConfig,
        component: str,
        *,
        time_budget_sec: float,
        lock_name: Optional[str] = None,
    ):
        self.cfg = cfg
        self.component = component
        self.time_budget_sec = float(time_budget_sec)
        self.lock_name = lock_name or f"ae_llm_agent_pipeline:{component}"
        self.run_id = uuid.uuid4().hex
        self.acquired = False
        self.counters: Dict[str, int] = {}
        self.exit_reason: Optional[str] = None
        self._lock_conn = None
        self._started_monotonic = 0.0

    # ---- 시간 예산 ----
    def time_left(self) -> float:
        return self.time_budget_sec - (time.monotonic() - self._started_monotonic)

    def deadline_reached(self, reserve_sec: float = 0.0) -> bool:
        return self.time_left() <= reserve_sec

    # ---- 카운터 ----
    def incr(self, name: str, amount: int = 1) -> None:
        self.counters[name] = self.counters.get(name, 0) + amount

    # ---- lock ----
    def ensure_lock(self) -> None:
        """lock 연결이 살아 있고 lock을 보유 중인지 확인합니다."""
        if self._lock_conn is None:
            raise RuntimeError("pipeline lock is not held")
        cur = self._lock_conn.cursor()
        try:
            cur.execute("SELECT IS_USED_LOCK(%s) = CONNECTION_ID()", (self.lock_name,))
            row = cur.fetchone()
        finally:
            cur.close()
        if not row or row[0] != 1:
            raise RuntimeError(f"pipeline lock lost: {self.lock_name}")

    def _insert_run(self, exit_reason: Optional[str] = None) -> None:
        conn = connect(self.cfg)
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                INSERT INTO `{RUN_TABLE}`(run_id, component, host, pid, exit_reason,
                                          finished_at)
                VALUES(%s,%s,%s,%s,%s, IF(%s IS NULL, NULL, NOW()))
                """,
                (
                    self.run_id,
                    self.component,
                    socket.gethostname(),
                    os.getpid(),
                    exit_reason,
                    exit_reason,
                ),
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

    def _mark_killed_runs(self) -> None:
        conn = connect(self.cfg)
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                UPDATE `{RUN_TABLE}`
                SET exit_reason='KILLED'
                WHERE component=%s AND finished_at IS NULL
                  AND exit_reason IS NULL AND run_id<>%s
                """,
                (self.component, self.run_id),
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

    def __enter__(self) -> "PipelineRun":
        self._started_monotonic = time.monotonic()
        self._lock_conn = connect(self.cfg)
        cur = self._lock_conn.cursor()
        try:
            cur.execute("SELECT GET_LOCK(%s, 0)", (self.lock_name,))
            row = cur.fetchone()
        finally:
            cur.close()
        self.acquired = bool(row and row[0] == 1)

        if not self.acquired:
            self._lock_conn.close()
            self._lock_conn = None
            self._insert_run(exit_reason="LOCK_BUSY")
            return self

        self._insert_run()
        # lock을 쥐고 있으므로 같은 component의 미종료 실행은 모두 죽은 실행입니다.
        self._mark_killed_runs()
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if not self.acquired:
            return False

        if exc_type is not None:
            reason = "ERROR"
            error = repr(exc)[:4000]
        else:
            reason = self.exit_reason or "DRAINED"
            error = None

        try:
            conn = connect(self.cfg)
            cur = conn.cursor()
            try:
                cur.execute(
                    f"""
                    UPDATE `{RUN_TABLE}`
                    SET finished_at=NOW(), exit_reason=%s, error=%s, counters_json=%s
                    WHERE run_id=%s
                    """,
                    (
                        reason,
                        error,
                        json.dumps(self.counters, ensure_ascii=False),
                        self.run_id,
                    ),
                )
                conn.commit()
            finally:
                cur.close()
                conn.close()
        finally:
            try:
                cur = self._lock_conn.cursor()
                cur.execute("SELECT RELEASE_LOCK(%s)", (self.lock_name,))
                cur.fetchall()
                cur.close()
            except Exception:
                pass
            try:
                self._lock_conn.close()
            except Exception:
                pass
            self._lock_conn = None
        return False


# =========================
# 작업 큐
# =========================
@dataclass
class Task:
    id: int
    mail_id: int
    stage: str
    item_key: str
    status: str
    attempt: int
    max_attempt: int
    input_ref: Optional[str]
    input_hash: Optional[str]
    output_ref: Optional[str]
    output_hash: Optional[str]
    external_id: Optional[str]
    checkpoint: Dict[str, Any] = field(default_factory=dict)
    attempt_row_id: Optional[int] = None

    @property
    def is_last_attempt(self) -> bool:
        """이번 시도가 실패하면 FAILED가 되는지 여부."""
        return self.attempt + 1 >= self.max_attempt


def backoff_seconds(attempt: int) -> int:
    """attempt(실패 횟수, 1부터)에 대한 재시도 대기 시간."""
    exponent = max(0, int(attempt) - 1)
    return int(min(BACKOFF_BASE_SEC * (2 ** min(exponent, 20)), BACKOFF_MAX_SEC))


def _json_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    if isinstance(value, str) and value:
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


_TASK_COLUMNS = (
    "id, mail_id, stage, item_key, status, attempt, max_attempt, input_ref, "
    "input_hash, output_ref, output_hash, external_id, checkpoint_json"
)


def _row_to_task(row: Dict[str, Any]) -> Task:
    return Task(
        id=int(row["id"]),
        mail_id=int(row["mail_id"]),
        stage=str(row["stage"]),
        item_key=str(row["item_key"] or ""),
        status=str(row["status"]),
        attempt=int(row["attempt"]),
        max_attempt=int(row["max_attempt"]),
        input_ref=row.get("input_ref"),
        input_hash=row.get("input_hash"),
        output_ref=row.get("output_ref"),
        output_hash=row.get("output_hash"),
        external_id=row.get("external_id"),
        checkpoint=_json_dict(row.get("checkpoint_json")),
    )


# 같은 키가 이미 있으면 입력(input_hash)이 바뀐 경우에만 PENDING으로 되돌립니다.
# 처리 중(PROCESSING)인 행은 건드리지 않습니다.
# MySQL은 SET 절을 왼쪽부터 평가하므로 status/input_* 는 마지막에 둡니다.
_UPSERT_ON_DUPLICATE = """
ON DUPLICATE KEY UPDATE
    attempt       = IF(status<>'PROCESSING' AND NOT (input_hash <=> VALUES(input_hash)), 0, attempt),
    next_retry_at = IF(status<>'PROCESSING' AND NOT (input_hash <=> VALUES(input_hash)), NULL, next_retry_at),
    last_error    = IF(status<>'PROCESSING' AND NOT (input_hash <=> VALUES(input_hash)), NULL, last_error),
    error_class   = IF(status<>'PROCESSING' AND NOT (input_hash <=> VALUES(input_hash)), NULL, error_class),
    external_id   = IF(status<>'PROCESSING' AND NOT (input_hash <=> VALUES(input_hash)), NULL, external_id),
    checkpoint_json = IF(status<>'PROCESSING' AND NOT (input_hash <=> VALUES(input_hash)), NULL, checkpoint_json),
    completed_at  = IF(status<>'PROCESSING' AND NOT (input_hash <=> VALUES(input_hash)), NULL, completed_at),
    status        = IF(status<>'PROCESSING' AND NOT (input_hash <=> VALUES(input_hash)), 'PENDING', status),
    input_ref     = IF(status='PROCESSING', input_ref, VALUES(input_ref)),
    input_hash    = IF(status='PROCESSING', input_hash, VALUES(input_hash))
"""


class TaskQueue:
    def __init__(self, cfg: DBConfig, run: PipelineRun, max_attempt: Optional[int] = None):
        self.cfg = cfg
        self.run = run
        self.max_attempt = int(max_attempt or DEFAULT_MAX_ATTEMPT)

    # ---- 등록 ----
    def seed(self, stage: str, select_sql: str, params: Sequence[Any] = ()) -> int:
        """
        select_sql은 mail_id, item_key, input_ref, input_hash 라는 이름(별칭)의
        4개 컬럼을 반환해야 합니다.
        새 행은 PENDING으로 추가되고, 기존 행은 input_hash가 바뀐 경우에만 PENDING으로 리셋됩니다.
        반환값: 영향받은 행 수(MySQL 규칙: 신규 1, 변경 2)
        """
        sql = f"""
            INSERT INTO `{TASK_TABLE}`(mail_id, stage, item_key, input_ref, input_hash,
                                       max_attempt)
            SELECT src.s_mail_id, %s, src.s_item_key, src.s_input_ref, src.s_input_hash, %s
            FROM (
                SELECT x.mail_id AS s_mail_id, x.item_key AS s_item_key,
                       x.input_ref AS s_input_ref, x.input_hash AS s_input_hash
                FROM ({select_sql}) AS x
            ) AS src
            {_UPSERT_ON_DUPLICATE}
        """
        conn = connect(self.cfg)
        cur = conn.cursor()
        try:
            cur.execute(sql, (stage, self.max_attempt, *params))
            count = cur.rowcount
            conn.commit()
            return max(count, 0)
        finally:
            cur.close()
            conn.close()

    def _upsert_children(self, cur, mail_id: int, children: Iterable[Dict[str, Any]]) -> int:
        count = 0
        for child in children:
            cur.execute(
                f"""
                INSERT INTO `{TASK_TABLE}`(mail_id, stage, item_key, input_ref, input_hash,
                                           max_attempt)
                VALUES(%s,%s,%s,%s,%s,%s)
                {_UPSERT_ON_DUPLICATE}
                """,
                (
                    int(child.get("mail_id", mail_id)),
                    str(child["stage"]),
                    str(child.get("item_key") or ""),
                    child.get("input_ref"),
                    child.get("input_hash"),
                    int(child.get("max_attempt") or self.max_attempt),
                ),
            )
            count += 1
        return count

    # ---- 복구 ----
    def recover_orphans(self, stages: Sequence[str], grace_sec: int = 0) -> int:
        """
        이 component의 lock을 쥔 상태에서 호출합니다.
        다른(죽은) 실행이 남긴 PROCESSING 행을 RETRY(attempt+1) 또는 FAILED로 되돌립니다.
        """
        if not stages:
            return 0
        self.run.ensure_lock()
        placeholders = ",".join(["%s"] * len(stages))
        conn = connect(self.cfg)
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                SELECT id FROM `{TASK_TABLE}`
                WHERE stage IN ({placeholders})
                  AND status='PROCESSING'
                  AND (run_id IS NULL OR run_id<>%s)
                  AND (heartbeat_at IS NULL
                       OR heartbeat_at <= NOW() - INTERVAL %s SECOND)
                FOR UPDATE
                """,
                (*stages, self.run.run_id, int(grace_sec)),
            )
            ids = [int(r[0]) for r in cur.fetchall()]
            if not ids:
                conn.commit()
                return 0
            id_ph = ",".join(["%s"] * len(ids))
            cur.execute(
                f"""
                UPDATE `{ATTEMPT_TABLE}`
                SET ended_at=NOW(), result='CRASHED',
                    error='process ended while PROCESSING'
                WHERE task_id IN ({id_ph}) AND ended_at IS NULL
                """,
                ids,
            )
            cur.execute(
                f"""
                UPDATE `{TASK_TABLE}`
                SET status=IF(attempt+1 >= max_attempt, 'FAILED', 'RETRY'),
                    attempt=attempt+1,
                    next_retry_at=NOW(),
                    error_class='TRANSIENT',
                    last_error=CONCAT('recovered orphan PROCESSING (run_id=',
                                      COALESCE(run_id,'NULL'), ')'),
                    run_id=NULL
                WHERE id IN ({id_ph})
                """,
                ids,
            )
            conn.commit()
            return len(ids)
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

    # ---- 가져가기 ----
    def claim_next(self, stage: str, batch: int = 20) -> Optional[Task]:
        """
        처리 가능한 작업 1건을 PROCESSING으로 가져옵니다. 없으면 None.
        RETRY보다 PENDING을, 같은 상태에서는 id 순서를 우선합니다.
        """
        self.run.ensure_lock()
        conn = connect(self.cfg)
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                f"""
                SELECT id FROM `{TASK_TABLE}`
                WHERE stage=%s
                  AND status IN ('PENDING','RETRY')
                  AND (next_retry_at IS NULL OR next_retry_at <= NOW())
                ORDER BY (status='RETRY'), id
                LIMIT %s
                """,
                (stage, int(batch)),
            )
            candidate_ids = [int(r["id"]) for r in cur.fetchall()]
            conn.commit()

            for task_id in candidate_ids:
                cur.execute(
                    f"""
                    UPDATE `{TASK_TABLE}`
                    SET status='PROCESSING', run_id=%s, heartbeat_at=NOW()
                    WHERE id=%s
                      AND status IN ('PENDING','RETRY')
                      AND (next_retry_at IS NULL OR next_retry_at <= NOW())
                    """,
                    (self.run.run_id, task_id),
                )
                if cur.rowcount != 1:
                    conn.commit()
                    continue

                cur.execute(
                    f"SELECT {_TASK_COLUMNS} FROM `{TASK_TABLE}` WHERE id=%s",
                    (task_id,),
                )
                task = _row_to_task(cur.fetchone())
                cur.execute(
                    f"""
                    INSERT INTO `{ATTEMPT_TABLE}`(task_id, run_id, attempt_no)
                    VALUES(%s,%s,%s)
                    """,
                    (task_id, self.run.run_id, task.attempt + 1),
                )
                task.attempt_row_id = int(cur.lastrowid)
                task.status = "PROCESSING"
                conn.commit()
                return task
            return None
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

    # ---- 진행 중 갱신 ----
    def heartbeat(self, task: Task) -> None:
        self._update_own(task, "heartbeat_at=NOW()", ())

    def save_external_id(self, task: Task, external_id: Optional[str]) -> None:
        """외부 작업 ID(예: 파싱 API task_id)를 즉시 commit합니다."""
        self._update_own(task, "external_id=%s, heartbeat_at=NOW()", (external_id,))
        task.external_id = external_id

    def save_checkpoint(self, task: Task, **values: Any) -> None:
        task.checkpoint.update(values)
        self._update_own(
            task,
            "checkpoint_json=%s, heartbeat_at=NOW()",
            (json.dumps(task.checkpoint, ensure_ascii=False),),
        )

    def _update_own(self, task: Task, set_sql: str, params: Sequence[Any]) -> None:
        conn = connect(self.cfg, found_rows=True)
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                UPDATE `{TASK_TABLE}` SET {set_sql}
                WHERE id=%s AND status='PROCESSING' AND run_id=%s
                """,
                (*params, task.id, self.run.run_id),
            )
            owned = cur.rowcount == 1
            conn.commit()
        finally:
            cur.close()
            conn.close()
        if not owned:
            raise RuntimeError(f"task {task.id} is no longer owned by run {self.run.run_id}")

    # ---- 종료 ----
    def complete(
        self,
        task: Task,
        *,
        output_ref: Optional[str] = None,
        output_hash: Optional[str] = None,
        quality: Optional[str] = None,
        children: Iterable[Dict[str, Any]] = (),
    ) -> None:
        """
        작업을 COMPLETED로 확정합니다.
        children(하위 단계 작업)은 같은 트랜잭션에서 등록됩니다.
        """
        conn = connect(self.cfg)
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                UPDATE `{TASK_TABLE}`
                SET status='COMPLETED', output_ref=%s, output_hash=%s, quality=%s,
                    last_error=NULL, error_class=NULL, next_retry_at=NULL,
                    heartbeat_at=NOW(), completed_at=NOW()
                WHERE id=%s AND status='PROCESSING' AND run_id=%s
                """,
                (output_ref, output_hash, quality, task.id, self.run.run_id),
            )
            if cur.rowcount != 1:
                raise RuntimeError(
                    f"task {task.id} is no longer owned by run {self.run.run_id}"
                )
            self._upsert_children(cur, task.mail_id, children)
            self._end_attempt(cur, task, "COMPLETED", None)
            conn.commit()
            task.status = "COMPLETED"
            task.output_ref = output_ref
            task.output_hash = output_hash
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

    def fail(self, task: Task, error: Any, *, permanent: Optional[bool] = None) -> str:
        """
        실패를 기록합니다. 반환값은 새 상태('RETRY' 또는 'FAILED').
        permanent를 생략하면 PermanentError 인스턴스인지로 판단합니다.
        """
        if permanent is None:
            permanent = isinstance(error, PermanentError)
        message = error if isinstance(error, str) else repr(error)
        message = (message or "")[:4000]
        new_attempt = task.attempt + 1
        if permanent or new_attempt >= task.max_attempt:
            status = "FAILED"
            delay = 0
        else:
            status = "RETRY"
            delay = backoff_seconds(new_attempt)

        conn = connect(self.cfg)
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                UPDATE `{TASK_TABLE}`
                SET status=%s, attempt=%s,
                    next_retry_at=IF(%s='RETRY', NOW() + INTERVAL %s SECOND, NULL),
                    last_error=%s, error_class=%s, run_id=NULL
                WHERE id=%s AND status='PROCESSING' AND run_id=%s
                """,
                (
                    status,
                    new_attempt,
                    status,
                    delay,
                    message,
                    "PERMANENT" if permanent else "TRANSIENT",
                    task.id,
                    self.run.run_id,
                ),
            )
            self._end_attempt(cur, task, status, message)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()
        task.status = status
        task.attempt = new_attempt
        return status

    def release(self, task: Task, reason: str = "time budget exhausted") -> None:
        """시간 예산 소진 등 정상 양보. attempt를 늘리지 않고 즉시 재시도 가능 상태로 둡니다."""
        conn = connect(self.cfg)
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                UPDATE `{TASK_TABLE}`
                SET status='RETRY', next_retry_at=NULL, run_id=NULL
                WHERE id=%s AND status='PROCESSING' AND run_id=%s
                """,
                (task.id, self.run.run_id),
            )
            self._end_attempt(cur, task, "RELEASED", reason)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()
        task.status = "RETRY"

    def _end_attempt(self, cur, task: Task, result: str, error: Optional[str]) -> None:
        if task.attempt_row_id is None:
            return
        cur.execute(
            f"""
            UPDATE `{ATTEMPT_TABLE}`
            SET ended_at=NOW(), result=%s, error=%s
            WHERE id=%s AND ended_at IS NULL
            """,
            (result, error, task.attempt_row_id),
        )


# =========================
# 파일 유틸
# =========================
def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_dir(path: Path) -> str:
    """디렉터리 내 (상대경로, 파일 hash) 목록의 hash."""
    root = Path(path)
    h = hashlib.sha256()
    for p in sorted(x for x in root.rglob("*") if x.is_file()):
        rel = p.relative_to(root).as_posix()
        h.update(rel.encode("utf-8"))
        h.update(b"\0")
        h.update(sha256_file(p).encode("ascii"))
        h.update(b"\n")
    return h.hexdigest()


def atomic_write_bytes(path: Path, data: bytes) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with tmp.open("wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def atomic_write_text(path: Path, text: str, encoding: str = "utf-8") -> None:
    atomic_write_bytes(path, text.encode(encoding))


def partial_dir_for(final_dir: Path) -> Path:
    final_dir = Path(final_dir)
    return final_dir.with_name(final_dir.name + ".partial")


def publish_dir(partial_dir: Path, final_dir: Path) -> None:
    """
    완성된 partial_dir을 final_dir로 교체 게시합니다.
    final_dir이 이미 있으면 백업으로 옮긴 뒤 교체하고 백업을 삭제합니다.
    """
    partial_dir = Path(partial_dir)
    final_dir = Path(final_dir)
    backup = None
    if final_dir.exists():
        backup = final_dir.with_name(f"{final_dir.name}.old-{uuid.uuid4().hex[:8]}")
        os.replace(final_dir, backup)
    os.replace(partial_dir, final_dir)
    if backup is not None:
        shutil.rmtree(backup, ignore_errors=True)
