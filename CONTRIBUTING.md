# 기여 가이드 — 커밋 메시지

## 형식

```
<타입>: <한글 설명한다>. (#이슈번호)
```

### 타입

| 타입 | 용도 |
|------|------|
| `FEAT` | 기능 추가 |
| `FIX` | 버그 수정 |
| `CHORE` | 설정·빌드·인프라 |
| `TEST` | 테스트 |
| `DOCUMENT` | 문서 |

### 이슈 번호

- 커밋 메시지 **맨 끝**에 `(#N)` 형태로 GitHub 이슈 번호를 붙입니다.

### 예시

```
FEAT: Kafka FdsEvent Pydantic 모델에 callDirection을 추가한다. (#8)
FEAT: FDS Kafka 워커에 DLQ·메트릭을 추가한다. (#9)
```

### 금지

- `Co-authored-by: Cursor <cursoragent@cursor.com>` 트레일러는 넣지 않습니다.

## FDS 이슈 매핑 (v1.1)

| 이슈 | 범위 |
|------|------|
| #1 | FDS REST 스코어링 API |
| #2 | Kafka FDS 이벤트 파이프라인 |
| #3 | DLQ·SESSION_ENDED |
| #8 | FdsEvent direction·campaignId |
| #9 | DLQ·consumer lag 메트릭 |
| #10 | direction별 Redis Feature Store |

https://github.com/WooriFDSAICC/WooriCardFDSGateway/issues
