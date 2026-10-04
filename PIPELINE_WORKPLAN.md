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
- [ ] P2-1 DB 설정(환경변수 `MYSQL_*`, 기존 이름 `MYSQL_DATABASE`/`MYSQL_DB`, `MYSQL_PASSWORD`/`MYSQL_PASS` 모두 지원)
- [ ] P2-2 스키마 보장: task / attempt / run 테이블 + 진행 현황 뷰 (`CREATE … IF NOT EXISTS`)
- [ ] P2-3 실행 컨텍스트: `GET_LOCK` 획득, run 행 기록, 종료 사유(DRAINED / TIME_BUDGET / LOCK_BUSY / ERROR), 카운터
- [ ] P2-4 작업 API: `seed`, `claim_next`, `heartbeat`, `save_checkpoint`, `complete`, `fail`(일시/영구), `release`
- [ ] P2-5 시도 이력 기록(`attempt` 테이블: 시작/종료/결과/오류)
- [ ] P2-6 stale PROCESSING 복구(attempt+1, 상한 초과 시 FAILED, 이력에 CRASHED)
- [ ] P2-7 지수 backoff, 오류 분류(`TransientError` / `PermanentError`)
- [ ] P2-8 원자적 파일 쓰기(`atomic_write_bytes/text`)와 디렉터리 게시(`.partial` → rename)
- [ ] P2-9 MySQL 호환 테스트(MariaDB로 실행)

### Phase 3. doc-parser 전환 (PARSE 단계)
- [ ] P3-1 1시간 잡 구조: `run_once`, `GET_LOCK`, 시간 예산(기본 50분), `runner.lock` 파일 제거
- [ ] P3-2 seed: `ae_llm_agent_mail` FILE_ARCHIVE COMPLETED → PARSE 작업 등록(파일 스캔·`TARGET_VERSION_TAG` 고정값 제거, 대상 version_tag는 설정값 목록)
- [ ] P3-3 `sharedworkspace_path`에서 `*.enriched.eml` 확인, 없으면 영구 실패로 기록
- [ ] P3-4 `create_task` 직후 `task_id`를 `external_id`에 즉시 저장, 재개 시 기존 태스크 polling 이어서 진행
- [ ] P3-5 모든 HTTP 요청 timeout, polling 중 heartbeat, 시간 예산 소진 시 RELEASE
- [ ] P3-6 export 검증(zip 유효성, jsonl ≥ 1개) 후 `.partial` 디렉터리에 해제 → rename
- [ ] P3-7 jsonl 메타 주입 실패 시 예외 처리(완료로 기록하지 않음)
- [ ] P3-8 원격 태스크 FAILED/ERROR 시 `external_id` 초기화 후 RETRY
- [ ] P3-9 결과 경로(`output_ref`)·해시 기록, `.DONE` 마커 유지
- [ ] P3-10 기존 `processed.json` → DB 이관 스크립트(DONE 항목을 PARSE COMPLETED로)
- [ ] P3-11 테스트(가짜 파싱 API + MariaDB)
- [ ] P3-12 README(실행·스케줄러 등록·모니터링)

### Phase 4. email-ingestion 보강
- [ ] P4-1 메일 폴더를 `.partial` 디렉터리에 완성한 뒤 rename(원자적 게시), 남은 `.partial` 정리
- [ ] P4-2 `exists_skip`은 최종 폴더에 `.enriched.eml`이 있을 때만 성공, 아니면 불완전 폴더로 보고 재생성
- [ ] P4-3 stale FILE_ARCHIVE 복구 시 `retry_count` 증가, 상한 도달 시 FAILED
- [ ] P4-4 `ingest_folder.py`에서 기존 ROUTED/RETRY 행 재처리
- [ ] P4-5 테스트

### Phase 5. rag-preparer DB 전환
- [ ] P5-1 `pipeline_state.py` 반입
- [ ] P5-2 PREPROCESS: PARSE 결과(`output_ref`) 기반 처리, `mail_id` 기준 출력 경로, `input_hash` 비교로 재사용 판단
- [ ] P5-3 LLM 실패 시 일시 오류로 재시도, 마지막 시도에서만 lite 대체 + `quality='DEGRADED'`
- [ ] P5-4 PREPROCESS 완료와 같은 트랜잭션에서 CANDIDATE·UPLOAD 작업 생성
- [ ] P5-5 CANDIDATE 단계(후보 큐 적재 실패 추적·재시도, 중복 집계 방지)
- [ ] P5-6 UPLOAD 단계(문서 단위 claim → 전송 → 완료, payload hash 기록)
- [ ] P5-7 기존 상태 파일 이관(업로드 성공 기록은 COMPLETED, 나머지는 PENDING → 과거 누락분 자동 재업로드)
- [ ] P5-8 1시간 잡 진입점 `run_pipeline.py`
- [ ] P5-9 `promote_candidate_terms.py` 행 단위 SAVEPOINT, 실패 행만 `promote_failed` 표시
- [ ] P5-10 `upload_term_index.py` 문서 단위 상태 저장
- [ ] P5-11 테스트

### Phase 6. 모니터링·운영
- [ ] P6-1 모니터링 SQL 모음(`pipeline_monitoring.sql`): 단계별 현황, 재시도 후 성공, 누락 탐지, 강제 종료 실행, 장기 FAILED
- [ ] P6-2 스케줄러 등록 가이드(4개 잡 명령·최대 실행 시간)
- [ ] P6-3 request-pipeline 문서의 미구현 기능(`FILE_ARCHIVE_MODE=NIGHT`, `--archive-only`, `SOURCE_MISSING`) 정리

## 5. 진행 기록
| 날짜 | 항목 | 저장소 | 비고 |
|---|---|---|---|
| 2026-10-04 | P0-1 | 전체 | 작업계획서 배포 |
| 2026-10-04 | P1-1~P1-6 | rag-preparer | 업로드 실패 추적·원자적 출력·hash 버그 수정, 테스트 `tests/` 추가 |
