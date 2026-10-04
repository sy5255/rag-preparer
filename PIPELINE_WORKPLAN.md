# 파이프라인 상태 추적 작업계획서

> 이 문서는 4개 저장소(`email-ingestion`, `doc-parser`, `rag-preparer`, `request-pipeline`)에
> **동일한 내용**으로 들어 있습니다. 진행 상황이 바뀌면 4개 저장소의 사본을 함께 갱신합니다.
> 작업 브랜치: 모든 저장소 `claude/pipeline-state-tracking`

## 1. 목표

```
email-ingestion ──▶ doc-parser ──▶ rag-preparer
        └──────────▶ request-pipeline
```

모든 단계는 **1시간 주기 잡**으로 실행되며, 실행 도중 강제 종료될 수 있다.
다음 주기에 **끊긴 지점부터 누락 없이** 이어서 처리하고,
각 작업의 성공 / 실패 / 재시도 후 성공 여부를 **DB에서 조회**할 수 있어야 한다.

## 2. 현재 문제 요약

| 단계 | 상태 저장소 | 핵심 문제 |
|---|---|---|
| email-ingestion | MySQL `ae_llm_agent_mail` | 반쯤 저장된 폴더가 `exists_skip`으로 COMPLETED 처리됨, `.enriched.eml` 비원자적 쓰기, stale 복구 시 retry_count 미증가, 폴더 모드에서 RETRY 미재처리 |
| request-pipeline | MySQL `ae_llm_agent_mail` | 문제 없음 (기준 모델) |
| doc-parser | 로컬 `processed.json` | task_id를 완료 전까지 저장하지 않음(중단 시 원격 태스크 중복 생성), 깨진 zip·패치 실패도 DONE, 타임아웃 없음, 재시도 상한 없음, 상태 파일 손상 시 전체 재제출, 파일시스템 스캔 인계 |
| rag-preparer | 로컬 `_state_processed.json` | **업로드 실패해도 파일을 완료 처리**, LLM 실패를 조용히 lite로 대체, 출력 비원자적 쓰기 + "있으면 skip", 입력 변경 미반영, `export_` 제거로 이름 충돌, 최신 verN만 처리, 무한 재시도(LLM 비용), 용어 승격 poison pill |

## 3. 설계 요약

### 3.1 원칙
1. `ae_llm_agent_mail`(email-ingestion)을 **0단계**로 그대로 사용한다.
2. 하위 단계는 공통 테이블 `ae_llm_agent_pipeline_task`로 추적한다.
3. **단계 간 인계는 DB로만** 한다. 하류는 시작 시 `INSERT IGNORE … SELECT`로 상류 COMPLETED 건을 자기 작업으로 등록(pull/seed)한다.
4. `attempt`는 "실패 횟수"다. 시간 예산 소진으로 정상 양보(RELEASE)하면 증가하지 않고, 프로세스가 죽은 경우(stale 복구)는 증가한다.
5. 결과물은 임시 경로에 쓴 뒤 rename하고, 검증 후에만 COMPLETED로 기록한다.
6. 모든 잡은 `run_once` + MySQL `GET_LOCK` + 시간 예산 구조이며 `while True` 상주 루프를 쓰지 않는다.

### 3.2 테이블
- `ae_llm_agent_pipeline_task` : 단계별 작업 단위의 현재 상태. `UNIQUE(mail_id, stage, item_key)`
- `ae_llm_agent_pipeline_attempt` : 시도 이력(append-only). 재시도 후 성공 여부 확인용
- `ae_llm_agent_pipeline_run` : 잡 실행 기록. 강제 종료 여부(finished_at NULL) 확인용
- `v_ae_llm_agent_pipeline_mail` : 메일 1건의 전체 단계 진행 상황 뷰

### 3.3 단계(stage)
| stage | 담당 | 단위 | item_key | seed 조건 |
|---|---|---|---|---|
| `PARSE` | doc-parser | 메일 | `''` | `ae_llm_agent_mail.route_type='FILE_ARCHIVE' AND status='COMPLETED'` |
| `PREPROCESS` | rag-preparer | 메일 | `''` | `PARSE` COMPLETED |
| `CANDIDATE` | rag-preparer | 메일 | `''` | `PREPROCESS` 완료 트랜잭션에서 생성 |
| `UPLOAD` | rag-preparer | 문서 | `{index}::{doc_id}` | `PREPROCESS` 완료 트랜잭션에서 생성 |

### 3.4 상태 전이
```
PENDING ─claim─▶ PROCESSING ─성공──────────────▶ COMPLETED
                     ├─일시 오류, attempt < max ─▶ RETRY (next_retry_at = now + backoff)
                     ├─영구 오류 / attempt ≥ max ─▶ FAILED
                     ├─시간 예산 소진(RELEASE) ───▶ RETRY (attempt 유지, 즉시 재시도 가능)
                     └─heartbeat 만료(프로세스 사망) ▶ RETRY (attempt+1) / FAILED
RETRY ─claim─▶ PROCESSING
```
backoff = `min(5분 × 2^(attempt-1), 6시간)`

수동 재시도:
```sql
UPDATE ae_llm_agent_pipeline_task
SET status='RETRY', attempt=0, next_retry_at=NULL, last_error=NULL
WHERE id=…;
```

## 4. 작업 순서와 체크리스트

진행 표시: `[x]` 완료 · `[ ]` 미완료 · `[~]` 사용자 확인/결정 필요

### Phase 0. 준비
- [x] P0-1 작업계획서를 4개 저장소에 배포
- [~] P0-2 RAG `insert-doc` API가 같은 `doc_id` 재전송 시 덮어쓰는지(멱등) 확인 — **사용자 확인 필요**. 추가 적재 방식이면 재업로드 전 삭제 호출 필요
- [~] P0-3 코드에 하드코딩된 자격증명(POP3/MySQL 비밀번호, RAG 키, 파싱 API 키)을 환경변수 전용으로 전환 — **운영 배포 방식 확인 후 진행**

### Phase 1. rag-preparer 긴급 패치 (기존 구조 유지, 누락 차단)
- [x] P1-1 `upload_jsonl_to_index`가 실패 건수를 반환하고, 실패가 1건이라도 있으면 `processed_inputs`에 기록하지 않음
- [x] P1-2 실패 내역을 상태 파일 `failed_docs`에 기록(오류 메시지, 시도 횟수, 마지막 시각)하고 성공 시 제거
- [x] P1-3 raw/full/lite 출력 파일을 임시 파일 → rename으로 원자적 저장
- [x] P1-4 문서 1건 업로드 성공 시마다 상태 저장(중단 시 재업로드 최소화)
- [x] P1-5 파일 단위 처리 실패도 `failed_inputs`에 기록
- [x] P1-6 (추가 발견) `created_time` 없는 문서는 매 실행 업로드 시각이 payload hash에 섞여 "동일 payload skip"이 동작하지 않던 문제 수정

### Phase 2. 공통 상태 모듈 `pipeline_state.py`
- [x] P2-1 DB 설정(환경변수 `MYSQL_*`, 기존 이름 `MYSQL_DATABASE`/`MYSQL_DB`, `MYSQL_PASSWORD`/`MYSQL_PASS` 모두 지원)
- [x] P2-2 스키마 보장: task / attempt / run 테이블 + 진행 현황 뷰 (`CREATE … IF NOT EXISTS`)
- [x] P2-3 실행 컨텍스트: `GET_LOCK` 획득, run 행 기록, 종료 사유(DRAINED / TIME_BUDGET / LOCK_BUSY / ERROR), 카운터
- [x] P2-4 작업 API: `seed`, `claim_next`, `heartbeat`, `save_checkpoint`, `complete`, `fail`(일시/영구), `release`
- [x] P2-5 시도 이력 기록(`attempt` 테이블: 시작/종료/결과/오류)
- [x] P2-6 stale PROCESSING 복구(attempt+1, 상한 초과 시 FAILED, 이력에 CRASHED)
- [x] P2-7 지수 backoff, 오류 분류(`TransientError` / `PermanentError`)
- [x] P2-8 원자적 파일 쓰기(`atomic_write_bytes/text`)와 디렉터리 게시(`.partial` → rename)
- [x] P2-9 MySQL 호환 테스트(MySQL 8.4 / MariaDB 10.11 양쪽에서 실행)

### Phase 3. doc-parser 전환 (PARSE 단계)
- [x] P3-1 1시간 잡 구조: `run_once`, `GET_LOCK`, 시간 예산(기본 50분), `runner.lock` 파일 제거
- [x] P3-2 seed: `ae_llm_agent_mail` FILE_ARCHIVE COMPLETED → PARSE 작업 등록(파일 스캔·`TARGET_VERSION_TAG` 고정값 제거, 대상 version_tag는 설정값 목록)
- [x] P3-3 `sharedworkspace_path`에서 `*.enriched.eml` 확인, 없으면 영구 실패로 기록
- [x] P3-4 `create_task` 직후 `task_id`를 `external_id`에 즉시 저장, 재개 시 기존 태스크 polling 이어서 진행
- [x] P3-5 모든 HTTP 요청 timeout, polling 중 heartbeat, 시간 예산 소진 시 RELEASE
- [x] P3-6 export 검증(zip 유효성, jsonl ≥ 1개) 후 `.partial` 디렉터리에 해제 → rename
- [x] P3-7 jsonl 메타 주입 실패 시 예외 처리(완료로 기록하지 않음)
- [x] P3-8 원격 태스크 FAILED/ERROR 시 `external_id` 초기화 후 RETRY
- [x] P3-9 결과 경로(`output_ref`)·해시 기록, `.DONE` 마커 유지
- [x] P3-10 기존 `processed.json` → DB 이관 스크립트(DONE 항목을 PARSE COMPLETED로)
- [x] P3-11 테스트(가짜 파싱 API + MariaDB)
- [x] P3-12 README(실행·스케줄러 등록·모니터링)

### Phase 4. email-ingestion 보강
- [x] P4-1 메일 폴더를 `.partial` 디렉터리에 완성한 뒤 rename(원자적 게시), 남은 `.partial` 정리
- [x] P4-2 `exists_skip`은 최종 폴더에 `.enriched.eml`이 있을 때만 성공, 아니면 불완전 폴더로 보고 재생성
- [x] P4-3 stale FILE_ARCHIVE 복구 시 `retry_count` 증가, 상한 도달 시 FAILED
- [x] P4-4 `ingest_folder.py`에서 기존 ROUTED/RETRY 행 재처리
- [x] P4-5 테스트
- [x] P4-6 (추가 발견) `ingest_folder.py` 첫 줄에 파일명이 코드로 들어가 있어 실행 즉시 `NameError`로 종료되던 문제 수정

### Phase 5. rag-preparer DB 전환
- [x] P5-1 `pipeline_state.py` 반입
- [x] P5-2 PREPROCESS: PARSE 결과(`output_ref`) 기반 처리, `mail_id` 기준 출력 경로, `input_hash` 비교로 재사용 판단
- [x] P5-3 LLM 실패 시 일시 오류로 재시도, 마지막 시도에서만 lite 대체 + `quality='DEGRADED'`
- [x] P5-4 PREPROCESS 완료와 같은 트랜잭션에서 CANDIDATE·UPLOAD 작업 생성
- [x] P5-5 CANDIDATE 단계(후보 큐 적재 실패 추적·재시도, 중복 집계 방지)
- [x] P5-6 UPLOAD 단계(문서 단위 claim → 전송 → 완료, payload hash 기록)
- [x] P5-7 기존 상태 파일 이관(업로드 성공 기록은 COMPLETED, 나머지는 PENDING → 과거 누락분 자동 재업로드)
- [x] P5-8 1시간 잡 진입점 `run_pipeline.py`
- [x] P5-9 `promote_candidate_terms.py` 행 단위 SAVEPOINT, 실패 행만 `promote_failed` 표시
- [x] P5-10 `upload_term_index.py` 문서 단위 상태 저장
- [x] P5-11 테스트
- [x] P5-12 (추가 발견) 공통 모듈: 같은 초에 heartbeat를 두 번 갱신하면 "소유권 상실"로 오판하던 문제 수정(FOUND_ROWS), doc-parser 사본 동기화

### Phase 6. 모니터링·운영
- [x] P6-1 모니터링 SQL 모음(이 문서 **부록 C**): 단계별 현황, 재시도 후 성공, 누락 탐지, 강제 종료 실행, 실패 원인
- [x] P6-2 스케줄러 등록 가이드·전환 순서(이 문서 **부록 A, B**)
- [x] P6-3 request-pipeline 문서의 미구현 기능(`FILE_ARCHIVE_MODE=NIGHT`, `--archive-only`, `SOURCE_MISSING`) 정리 — 문서에 "미구현" 명시
- [x] P6-4 (추가 발견) 용어사전 잡(`promote_candidate_terms.py`, `upload_term_index.py`)도 상주 루프 → 기본 1회 실행 후 종료로 전환

## 5. 진행 기록
| 날짜 | 항목 | 저장소 | 비고 |
|---|---|---|---|
| 2026-10-04 | P0-1 | 전체 | 작업계획서 배포 |
| 2026-10-04 | P1-1~P1-6 | rag-preparer | 업로드 실패 추적·원자적 출력·hash 버그 수정, 테스트 `tests/` 추가 |
| 2026-10-04 | P6-1~P6-4 | 전체 | 운영 부록(스케줄러·전환 순서·모니터링 SQL, MySQL 8.4에서 실행 확인), 용어사전 잡 1회 실행화, request-pipeline 문서 정리 |
| 2026-10-04 | P5-1~P5-12 | rag-preparer, doc-parser | `run_pipeline.py`(PREPROCESS/CANDIDATE/UPLOAD), 예전 상태 재사용, 용어 승격 SAVEPOINT, 테스트 13건(rag-preparer)·22건(doc-parser) MySQL 8.4 통과 |
| 2026-10-04 | P4-1~P4-6 | email-ingestion | 메일 폴더 원자적 게시, 불완전 폴더 재생성, stale 복구 retry_count, 폴더 모드 재처리, 테스트 6건 |
| 2026-10-04 | P2-9 | doc-parser | MySQL 8.4에서 테스트 21건 통과 확인 |
| 2026-10-04 | P3-1~P3-12 | doc-parser | `app.py` DB 기반 1시간 잡으로 전환, `migrate_processed_json.py`, README, 테스트 9건 추가(총 21건) |
| 2026-10-04 | P2-1~P2-9 | doc-parser | `pipeline_state.py` + MariaDB 테스트 12건 (rag-preparer에는 P5-1에서 반입) |

## 부록 A. 스케줄러 등록

모든 잡은 **1회 실행 후 종료**합니다. 실행 주기 1시간, 최대 실행 시간 59분 기준입니다.

| 저장소 | 작업 디렉터리 | 실행 명령 | 비고 |
|---|---|---|---|
| email-ingestion | `/config/work/email-ingestion` | `python ingest_pop3.py` | `RUN_ONCE=true`(기본) |
| request-pipeline | `/config/work/request-pipeline` | `python run_pipeline.py` | 55분 처리 + 3분 대기 후 종료 |
| doc-parser | `/config/work/doc-parser` | `python app.py` | 시간 예산 50분 |
| rag-preparer | `/config/work/rag-preparer` | `python run_pipeline.py` | 시간 예산 50분 |
| rag-preparer (용어 승격) | `/config/work/rag-preparer` | `python term_dictionary/promote_candidate_terms.py` | 1회 실행 |
| rag-preparer (용어 인덱스) | `/config/work/rag-preparer` | `python term_dictionary/upload_term_index.py` | 1회 실행 |

공통 환경변수(doc-parser, rag-preparer): `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_DATABASE`(또는 `MYSQL_DB`),
`MYSQL_USER`, `MYSQL_PASSWORD`(또는 `MYSQL_PASS`). 비밀번호는 필수입니다.

doc-parser와 rag-preparer는 `GET_LOCK`으로 중복 실행을 막습니다. 이전 실행이 아직 돌고 있으면
새 실행은 `exit_reason='LOCK_BUSY'`로 기록하고 바로 종료합니다.

## 부록 B. 전환 순서 (운영 반영 시)

1. **email-ingestion** 새 버전 배포 (스키마 변경 없음)
2. **doc-parser**
   1. 기존 상주 프로세스(예전 `app.py`) 중지
   2. 환경변수 설정 후 `python migrate_processed_json.py --dry-run` → 건수 확인 → `python migrate_processed_json.py`
   3. 스케줄러에 `python app.py` 등록 (테이블·뷰는 첫 실행 시 자동 생성)
3. **rag-preparer**
   1. 기존 상주 프로세스(`upload_indices.py`, 용어 스크립트 `nohup`) 중지
   2. 스케줄러에 `python run_pipeline.py` 및 용어 잡 2개 등록
   3. 첫 실행에서 예전 상태 파일(`_state_processed.json`)을 읽어 이미 만든 결과·업로드는 재사용하고,
      **예전에 실패해 빠진 문서만 다시 업로드**합니다.
   4. ⚠️ P0-2(RAG `insert-doc` 멱등성) 확인 전에는 "예전 상태 파일에 없는 문서"가 다시 전송될 수 있습니다.
4. 부록 C의 A-5, A-6(누락 탐지) 쿼리가 0건인지 확인

## 부록 C. 모니터링 SQL

(MySQL 8.4에서 실행 확인)

```sql
-- A-1. 단계별 현황
SELECT stage, status, COUNT(*) AS cnt
FROM ae_llm_agent_pipeline_task
GROUP BY stage, status
ORDER BY stage, status;

-- A-2. 0단계(email-ingestion) FILE_ARCHIVE 현황
SELECT status, COUNT(*) AS cnt
FROM ae_llm_agent_mail
WHERE route_type = 'FILE_ARCHIVE'
GROUP BY status;

-- A-3. 재시도 끝에 성공한 작업
SELECT id, mail_id, stage, item_key, attempt, completed_at
FROM ae_llm_agent_pipeline_task
WHERE status = 'COMPLETED' AND attempt > 0
ORDER BY completed_at DESC
LIMIT 100;

-- A-4. 현재 실패/재시도 대기 작업과 원인
SELECT id, mail_id, stage, item_key, status, attempt, max_attempt,
       error_class, next_retry_at, LEFT(last_error, 300) AS last_error
FROM ae_llm_agent_pipeline_task
WHERE status IN ('RETRY', 'FAILED')
ORDER BY status, updated_at DESC;

-- A-5. 누락 탐지: 아카이브 완료됐는데 PARSE 작업이 없음 (doc-parser 실행 후 0이어야 정상)
--      DOC_PARSER_VERSION_TAGS 대상 버전만 보려면 sharedworkspace_path 조건을 추가하세요.
SELECT m.id, m.sharedworkspace_path, m.saved_at
FROM ae_llm_agent_mail m
LEFT JOIN ae_llm_agent_pipeline_task t
       ON t.mail_id = m.id AND t.stage = 'PARSE' AND t.item_key = ''
WHERE m.route_type = 'FILE_ARCHIVE' AND m.status = 'COMPLETED'
  AND t.id IS NULL;

-- A-6. 누락 탐지: PARSE 완료됐는데 PREPROCESS 작업이 없음 (rag-preparer 실행 후 0이어야 정상)
SELECT p.mail_id, p.output_ref, p.completed_at
FROM ae_llm_agent_pipeline_task p
LEFT JOIN ae_llm_agent_pipeline_task pp
       ON pp.mail_id = p.mail_id AND pp.stage = 'PREPROCESS' AND pp.item_key = ''
WHERE p.stage = 'PARSE' AND p.status = 'COMPLETED' AND pp.id IS NULL;

-- A-7. 작업별 시도 이력 (id를 바꿔서 조회)
SELECT a.attempt_no, a.run_id, a.started_at, a.ended_at, a.result, LEFT(a.error, 300) AS error
FROM ae_llm_agent_pipeline_attempt a
WHERE a.task_id = 1
ORDER BY a.id;

-- A-8. 최근 잡 실행 기록 (KILLED = 강제 종료됨, LOCK_BUSY = 이전 실행이 아직 동작 중)
SELECT component, run_id, started_at, finished_at, exit_reason,
       TIMESTAMPDIFF(MINUTE, started_at, COALESCE(finished_at, NOW())) AS minutes,
       counters_json
FROM ae_llm_agent_pipeline_run
ORDER BY started_at DESC
LIMIT 50;

-- A-9. 최근 2시간 동안 한 번도 실행되지 않은 잡 (스케줄러 이상 탐지)
SELECT c.component, MAX(r.started_at) AS last_started_at
FROM (SELECT 'doc-parser' AS component UNION ALL SELECT 'rag-preparer') c
LEFT JOIN ae_llm_agent_pipeline_run r ON r.component = c.component
GROUP BY c.component
HAVING last_started_at IS NULL OR last_started_at < NOW() - INTERVAL 2 HOUR;

-- A-10. 메일별 전체 진행 현황
SELECT *
FROM v_ae_llm_agent_pipeline_mail
ORDER BY last_updated_at DESC
LIMIT 100;

-- A-11. LLM 대체(DEGRADED)로 처리된 메일 → 필요 시 재처리
SELECT mail_id, completed_at
FROM ae_llm_agent_pipeline_task
WHERE stage = 'PREPROCESS' AND quality = 'DEGRADED';
```

수동 재시도:

```sql
UPDATE ae_llm_agent_pipeline_task
SET status='RETRY', attempt=0, next_retry_at=NULL, last_error=NULL
WHERE id = …;
```
