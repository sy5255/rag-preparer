`term_candidate_queue`, `term_dictionary`, `term_candidate_queue` 컬럼 설명표와 
`promote_candidate_terms.py` 실행/운영 방법

---
# 0. `term_dictionary`, `term_aliases` 컬럼 설명

## 1. `term_dictionary`

**역할**  
용어사전의 **대표 용어(canonical term) 마스터 테이블**.  
검색/정규화/확장 시 기준이 되는 표준 용어를 관리한다.

| 컬럼명                 | 타입 예시             | 설명                                                   | 예시                                                           |
| ------------------- | ----------------- | ---------------------------------------------------- | ------------------------------------------------------------ |
| `term_id`           | BIGINT / INT      | 용어의 고유 식별자(PK)                                       | `101`                                                        |
| `term_type`         | VARCHAR           | 용어 종류. 제품/공정/화학성분/불량명/노드/담당자 등 분류에 사용                | `chemistry`, `process`, `product`, `defect`, `node`, `owner` |
| `canonical_name`    | VARCHAR           | 해당 용어의 표준명(정규화 결과로 귀결되는 이름)                          | `HF`, `CMP`, `particle`                                      |
| `display_name`      | VARCHAR           | UI나 문서에서 표시할 이름                                      | `HF`, `CMP`                                                  |
| `description`       | TEXT / VARCHAR    | 용어 설명. 검색 품질 개선 및 관리 목적                              | `Hydrofluoric acid related chemistry`                        |
| `scope`             | VARCHAR           | 적용 범위. 특정 카테고리에서만 유효한 용어를 구분할 때 사용                   | `all`, `inline_fa_report`                                    |
| `status`            | VARCHAR / ENUM    | 용어 상태. 활성/비활성/임시 상태 구분                               | `active`, `inactive`                                         |
| `is_verified`       | TINYINT(1) / BOOL | 검증된 용어인지 여부. 운영자가 확정한 용어인지 표시                        | `1`, `0`                                                     |
| `metadata_json`     | JSON / TEXT       | 보조 정보 저장용 JSON. 관련 키워드, 예시 문장 등 확장 정보 저장             | `{"related_keywords":["HF"],"examples":["HF cleaning"]}`     |
| `priority`          | INT               | 동일 alias가 여러 용어와 충돌할 때 우선순위 판단용. **작을수록 우선**으로 운영 권장 | `30`, `50`, `200`                                            |
| `expand_to_aliases` | TINYINT(1) / BOOL | Query expansion 시 이 용어의 alias들까지 확장 검색에 포함할지 여부      | `1`                                                          |
| `search_boost`      | FLOAT             | 검색 확장 시 가중치 조정용 값                                    | `1.3`, `1.0`, `0.7`                                          |
| `created_at`        | DATETIME          | 행 생성 시각                                              | `2026-03-13 10:00:00`                                        |
| `updated_at`        | DATETIME          | 행 최종 수정 시각                                           | `2026-03-13 11:20:00`                                        |

### 운영 메모

- `canonical_name`은 시스템에서 최종 정규화 결과로 사용하는 이름이다.
    
- `priority`, `expand_to_aliases`, `search_boost`는 **Query normalization / expansion 품질**에 직접 영향이 있다.
    
- `metadata_json`에는 보통 아래 정보를 저장한다.
    
    - `related_keywords`
        
    - `examples`

## 2. `term_aliases`

**역할**  
대표 용어(`term_dictionary`)에 연결되는 **별칭(alias), 약어, 변형 표현**을 관리하는 테이블.  
문서/질문에 등장한 표현을 canonical term으로 매핑할 때 사용한다.

|컬럼명|타입 예시|설명|예시|
|---|---|---|---|
|`alias_id`|BIGINT / INT|alias 행의 고유 식별자(PK)|`5001`|
|`term_id`|BIGINT / INT|연결되는 대표 용어의 ID (`term_dictionary.term_id`)|`101`|
|`alias_text`|VARCHAR|실제 문서나 질문에서 등장하는 표현 원문|`DHF`, `hydrofluoric acid`|
|`alias_normalized`|VARCHAR|매칭 비교를 쉽게 하기 위해 정규화한 alias 값|`dhf`, `hydrofluoric acid`|
|`match_type`|VARCHAR|alias 매칭 방식|`contains`, `exact`, `regex`|
|`language_code`|VARCHAR|언어 코드|`en`, `ko`|
|`is_preferred`|TINYINT(1) / BOOL|대표 alias 여부|`1`, `0`|
|`status`|VARCHAR / ENUM|alias 상태|`active`, `inactive`|
|`created_at`|DATETIME|행 생성 시각|`2026-03-13 10:05:00`|
|`updated_at`|DATETIME|행 최종 수정 시각|`2026-03-13 11:30:00`|

### 운영 메모

- `alias_text`는 실제 텍스트에 등장하는 표현 그대로 저장한다.
    
- `alias_normalized`는 보통 아래 규칙으로 만든다.
    
    - 소문자 변환
        
    - `-`, `_`, `/` → 공백 치환
        
    - 연속 공백 정리
        
- 같은 `term_id` 아래에서 `alias_normalized`는 중복되지 않도록 관리한다.
    
- 예:
    
    - canonical: `HF`
        
    - aliases: `HF`, `DHF`, `hydrofluoric acid`, `dilute hf`
        
---

# 1. `term_candidate_queue` 컬럼 설명

## 기본 식별 / 후보 정보

| 컬럼명                   | 의미                                                               | 누가 채움    |
| --------------------- | ---------------------------------------------------------------- | -------- |
| `candidate_id`        | 후보 row 고유 ID                                                     | DB 자동    |
| `candidate_type`      | 후보 유형. 예: `chemistry`, `process`, `product`, `defect`, `acronym` | 파이프라인 자동 |
| `raw_text`            | 문서에서 실제로 잡힌 원문 후보 문자열                                            | 파이프라인 자동 |
| `normalized_text`     | 중복 판정용 정규화 문자열                                                   | 파이프라인 자동 |
| `suggested_canonical` | 파이프라인이 추천한 표준명 후보                                                | 파이프라인 자동 |
| `scope`               | 적용 범위. 예: `all`, `inline_fa_report`                              | 파이프라인 자동 |

---

## 후보 발생 근거 / 통계

| 컬럼명                    | 의미                                                     | 누가 채움    |
| ---------------------- | ------------------------------------------------------ | -------- |
| `source_stage`         | 어느 단계에서 후보가 생성됐는지. 예: `build_serving_views_full`       | 파이프라인 자동 |
| `source_rule`          | 어떤 룰/소스에서 잡혔는지. 예: `rule_process_choice`, `llm_defect` | 파이프라인 자동 |
| `confidence`           | 후보 신뢰도                                                 | 파이프라인 자동 |
| `detected_count`       | 지금까지 몇 번 탐지됐는지 누적 횟수                                   | 파이프라인 자동 |
| `sample_doc_ids_json`  | 샘플 문서 ID 목록(JSON 배열)                                   | 파이프라인 자동 |
| `sample_titles_json`   | 샘플 문서 제목 목록(JSON 배열)                                   | 파이프라인 자동 |
| `sample_snippets_json` | 샘플 문맥 스니펫 목록(JSON 배열)                                  | 파이프라인 자동 |
| `first_seen_at`        | 처음 탐지된 시각                                              | DB/자동    |
| `last_seen_at`         | 마지막 탐지된 시각                                             | 파이프라인 자동 |

---

## 검토 / 승인 상태

| 컬럼명             | 의미                                                                 | 누가 채움        |
| --------------- | ------------------------------------------------------------------ | ------------ |
| `status`        | 기존 운영 상태. 가능하면 앞으로는 `review_status` 중심으로 쓰는 걸 추천                   | 과거/혼용        |
| `review_status` | 검토 상태. `pending`, `approved`, `promoted`, `rejected`, `needs_edit` | 사람 + 승격 스크립트 |
| `reviewed_by`   | 누가 검토했는지                                                           | 사람           |
| `reviewed_at`   | 언제 검토했는지                                                           | 사람           |

---

## 사람이 입력하는 승인값

| 컬럼명                          | 의미                          | 누가 채움 |
| ---------------------------- | --------------------------- | ----- |
| `approved_term_type`         | 최종 승인된 용어 유형                | 사람    |
| `approved_canonical_name`    | 최종 승인된 표준명                  | 사람    |
| `approved_display_name`      | 화면 표시용 이름. 비우면 canonical 사용 | 사람    |
| `approved_scope`             | 최종 적용 범위                    | 사람    |
| `approved_is_verified`       | 검증 용어 여부                    | 사람    |
| `approved_priority`          | query normalization 우선순위    | 사람    |
| `approved_expand_to_aliases` | 검색 확장 시 alias 확장 여부         | 사람    |
| `approved_search_boost`      | 검색 가중치                      | 사람    |
| `approved_description`       | 최종 설명                       | 사람    |
| `approved_metadata_json`     | 최종 metadata JSON            | 사람    |

`approved_term_type`로 들어갈 수 있는 값 : chemistry, process, product, defect, node, owner, acronym

---

## draft 초안 정보

| 컬럼명                   | 의미                 | 누가 채움   |
| --------------------- | ------------------ | ------- |
| `draft_description`   | 자동 생성된 설명 초안       | 승격 스크립트 |
| `draft_metadata_json` | 자동 생성된 metadata 초안 | 승격 스크립트 |

두 컬럼은 중요한 역할!!
`term_candidate_queue`만으로는 완벽한 description/metadata가 안 나와도, 최소 초안을 자동으로 만들어주고, 사람이 필요하면 `approved_*`로 덮어쓰는 구조

---

## 승격 결과

| 컬럼명                | 의미                                   | 누가 채움   |
| ------------------ | ------------------------------------ | ------- |
| `promoted_term_id` | `term_dictionary.term_id`로 반영된 결과 ID | 승격 스크립트 |
| `promoted_at`      | 실제 승격 완료 시각                          | 승격 스크립트 |

---

# 2. 상태값 운영 규칙

추천 운영 규칙

| 값            | 의미                                     |
| ------------ | -------------------------------------- |
| `pending`    | 아직 검토 전                                |
| `approved`   | 사람이 승인했고, 승격 대기 중                      |
| `promoted`   | `term_dictionary / term_aliases` 반영 완료 |
| `rejected`   | 반려                                     |
| `needs_edit` | 승인 필수값이 부족하거나 재검토 필요                   |

실무적으로는 `review_status`만 보면 충분!!

---

# 3. 사람이 실제로 어떻게 운영하면 되는지?

## 1단계

문서 파이프라인이 돌아가면서 `term_candidate_queue`에 후보를 계속 쌓음.

## 2단계

DBeaver에서 `term_candidate_queue`를 보고 사람이 검토.

예를 들어 `buffered oxide etch`가 들어왔으면:

* `review_status = approved`
* `approved_term_type = chemistry`
* `approved_canonical_name = BOE`

이 정도만 넣어도 충분함.

## 3단계

`promote_candidate_terms.py`가 approved row를 감시해서:

* `term_dictionary` upsert
* `term_aliases` upsert
* `review_status = promoted`
* `promoted_term_id` 기록

## 4단계

`upload_term_index.py`가 변경된 term만 vector DB에 반영

---

# 4. `promote_candidate_terms.py` 실행 방법

## 실행 명령

```bash
python promote_candidate_terms.py
```

## 백그라운드 상시 실행 권장

리눅스 서버 기준

```bash
nohup python promote_candidate_terms.py > promote_candidate_terms.log 2>&1 &
```

로그 확인:

```bash
tail -f promote_candidate_terms.log
```

프로세스 확인:

```bash
ps -ef | grep promote_candidate_terms.py
```

---

# 5. `upload_term_index.py` 실행 방법

이것도 같이 상시 실행

```bash
nohup python upload_term_index.py > upload_term_index.log 2>&1 &
```

즉, 용어사전 계열은 두 프로세스가 같이 떠야함.

* `promote_candidate_terms.py`
* `upload_term_index.py`

---

# 6. 운영 시 권장 실행 구성

## 문서 계열

* email ingest
* doc parser
* build serving views
* upload indices

## 용어사전 계열

* candidate queue 적재
* promote candidate terms
* upload term index

즉, 용어사전 쪽에 별도 서브 파이프라인 하나 더 추가함.

---

# 7. 사람이 DBeaver에서 최소한 무엇만 수정하면 되는지?

가장 최소 입력 세트

| 컬럼                         | 필수 여부 |
| -------------------------- | ----- |
| `review_status='approved'` | 필수    |
| `approved_term_type`       | 필수    |
| `approved_canonical_name`  | 권장    |
| `reviewed_by`              | 권장    |
| `reviewed_at`              | 권장    |

나머지는 비워도 일단 돌아가게 설계함.

---

# 8. 예시 1: chemistry 승인

원문 후보:

* `raw_text = buffered oxide etch`

사람이 넣는 값:

* `review_status = approved`
* `approved_term_type = chemistry`
* `approved_canonical_name = BOE`
* `approved_is_verified = 1`
* `approved_priority = 55`
* `approved_expand_to_aliases = 1`
* `approved_search_boost = 1.20`

그럼 자동으로:

* `term_dictionary`에는 canonical `BOE`
* `term_aliases`에는 `BOE`, `buffered oxide etch`

가 들어감.

---

# 9. 예시 2: defect 승인

원문 후보:

* `raw_text = NOP`

사람이 넣는 값:

* `review_status = approved`
* `approved_term_type = defect`
* `approved_canonical_name = NOP`

그럼 자동으로:

* `term_dictionary` defect / NOP
* `term_aliases` NOP

가 들어감.

---

# 10. DBeaver에서 바로 추가되는지 다시 정확히 설명

## 현재 구조

DBeaver 수정 즉시 DB 반영은 **term_candidate_queue에만** 일어남.

그 다음:

* `promote_candidate_terms.py`가 주기적으로 polling
* approved 상태를 발견
* 그때 `term_dictionary / term_aliases`에 반영

즉, **즉시 트리거 방식이 아니라 polling 방식**

---

# 11. 추천 점검 SQL

## 아직 검토 안 된 후보 보기

```sql
SELECT candidate_id, candidate_type, raw_text, suggested_canonical, scope, detected_count, confidence
FROM term_candidate_queue
WHERE review_status = 'pending'
ORDER BY detected_count DESC, confidence DESC, candidate_id ASC;
```

## 승인됐지만 아직 승격 안 된 후보 보기

```sql
SELECT candidate_id, candidate_type, raw_text, approved_term_type, approved_canonical_name
FROM term_candidate_queue
WHERE review_status = 'approved'
  AND promoted_term_id IS NULL
ORDER BY candidate_id ASC;
```

## 승격 완료된 후보 보기

```sql
SELECT candidate_id, raw_text, approved_canonical_name, promoted_term_id, promoted_at
FROM term_candidate_queue
WHERE review_status = 'promoted'
ORDER BY promoted_at DESC;
```

---

# 12. 추천 운영 팁

운영시 DBeaver에서는 아래 순서로 체크

1. `pending`만 필터
2. `detected_count DESC` 정렬
3. 자주 나온 후보부터 승인
4. `promoted` 전환 확인
5. 이후 term index 로그 확인

---

# 13. 마지막 정리

## 사람이 하는 일

* `term_candidate_queue`에서 승인값 입력

## 시스템이 하는 일

* approved 후보를 `term_dictionary / term_aliases`로 승격
* 변경된 term만 vector DB에 업로드
* stale term은 vector DB에서 삭제
