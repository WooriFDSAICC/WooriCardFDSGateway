# WooriCardFDSGateway — 실행 가이드

FastAPI 기반 **FDS Gateway**입니다. REST/gRPC FDS 스코어링 API와 Kafka 이벤트 워커(`wooricard-fds-events` consume → scores/actions 발행)를 제공합니다.

## 사전 요구사항

| 항목 | 버전 |
|------|------|
| Python | 3.12 |
| Redis | 7.x |
| Kafka | 3.x (워커 사용 시) |
| Triton (또는 Mock) | HTTP `:8001` — FDS 모델 `fds_lgbm` |

---

## 1. Docker로 실행

### 전체 스택 (Relay compose 포함)

```powershell
cd MiddleWare\WooriCardCallBotRelayServer
docker compose up --build fds-gateway
```

의존: `redis`, `kafka`, `triton`은 compose에서 함께 기동됩니다.

### Gateway 이미지만

```powershell
cd MiddleWare\WooriCardFDSGateway
docker build -t woori-fds-gateway .
docker run --rm -p 8010:8010 `
  -e REDIS_HOST=host.docker.internal `
  -e KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:9092 `
  -e TRITON_HTTP_URL=http://host.docker.internal:8001 `
  woori-fds-gateway
```

---

## 2. 로컬 개발 실행

```powershell
cd MiddleWare\WooriCardFDSGateway
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Redis·Kafka·Triton Mock을 로컬에서 먼저 띄워야 합니다. (Relay compose의 `redis`/`kafka`/`triton`만 띄워도 됩니다)

```powershell
cd MiddleWare\WooriCardCallBotRelayServer
docker compose up -d redis kafka triton
```

환경 변수 예시:

```powershell
$env:REDIS_HOST = "localhost"
$env:REDIS_PORT = "6380"
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
$env:KAFKA_WORKER_ENABLED = "true"
$env:TRITON_HTTP_URL = "http://localhost:8001"
uvicorn run:app --host 0.0.0.0 --port 8010 --reload
```

---

## 3. API 엔드포인트

| 경로 | 설명 |
|------|------|
| `GET /health` | 헬스체크 (`triton_ready`, `kafka_fds_pipeline` 등) |
| `GET /metrics` | Prometheus 메트릭 |
| `POST /api/v1/fds/score` | FDS 스코어링 REST API |
| Internal 라우트 | 내부 Feature Store API (`app/routers/internal.py`) |

기본 포트: **8010**

Swagger UI: `http://localhost:8010/docs`

gRPC (선택):

```powershell
$env:GRPC_ENABLED = "true"
$env:GRPC_PORT = "50051"
```

---

## 4. Kafka 워커

`KAFKA_WORKER_ENABLED=true`(기본)이면 앱 기동 시 백그라운드에서 다음 파이프라인이 동작합니다.

```
wooricard-fds-events → Feature Store(Redis) → 스코어링
  → wooricard-fds-scores → Rule 판정 → wooricard-fds-actions
```

파싱 실패 시: `wooricard-fds-events-dlq`

### 주요 Kafka 환경 변수

| 변수 | 기본값 |
|------|--------|
| `KAFKA_BOOTSTRAP_SERVERS` | localhost:9092 |
| `KAFKA_EVENTS_TOPIC` | wooricard-fds-events |
| `KAFKA_SCORES_TOPIC` | wooricard-fds-scores |
| `KAFKA_ACTIONS_TOPIC` | wooricard-fds-actions |
| `KAFKA_DLQ_TOPIC` | wooricard-fds-events-dlq |
| `KAFKA_CONSUMER_GROUP` | woori-fds-gateway |
| `KAFKA_DLQ_ENABLED` | true |

---

## 5. 환경 변수 (전체)

`.env` 또는 셸에서 설정 (`app/config.py`).

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `TRITON_HTTP_URL` | http://localhost:8001 | Triton URL |
| `TRITON_FDS_MODEL` | fds_lgbm | FDS 모델명 |
| `TRITON_STARTUP_CHECK` | true | 기동 시 ready 검사 |
| `API_KEY_ENABLED` | false | REST API 키 |
| `INTERNAL_API_KEY` | woori-internal-dev-key | 내부 API 키 |
| `RATE_LIMIT_ENABLED` | true | Rate limit |
| `METRICS_ENABLED` | true | Prometheus |
| `OTEL_ENABLED` | false | OpenTelemetry |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | http://localhost:4318/v1/traces | Jaeger |

---

## 6. 동작 확인

```powershell
curl http://localhost:8010/health
curl http://localhost:8010/metrics
```

Kafka 이벤트가 consume되는지는 Relay 경유 통화 또는 Relay가 `wooricard-fds-events`에 발행한 뒤 메트릭 `fds_kafka_events_processed_total`로 확인합니다.

### 단위 테스트

```powershell
pytest
```

### 계약 동기화

```powershell
python ..\scripts\sync_integration_contracts.py
```

---

## 7. Relay 연동

Relay가 `wooricard-fds-events`에 `callDirection`, `campaignId` 포함 FdsEvent v1.0을 발행하면 FDS Gateway 워커가 consume합니다. Redis 세션 키: `wooricard:session:{inbound|outbound}:{sessionId}`.
