# WooriCardFDSGateway ???�행 가?�드

FastAPI 기반 **FDS Gateway**?�니?? REST/gRPC FDS ?�코?�링 API?� Kafka ?�벤???�커(`wooricard-fds-events` consume ??scores/actions 발행)�??�공?�니??

## ?�전 ?�구?�항

| ??�� | 버전 |
|------|------|
| Python | 3.12 |
| Redis | 7.x |
| Kafka | 3.x (?�커 ?�용 ?? |
| Triton (?�는 Mock) | HTTP `:8001` ??FDS 모델 `fds_lgbm` |

---

## 1. Docker�??�행

### ?�체 ?�택 (Relay compose ?�함)

```powershell
cd MiddleWare\WooriCardCallBotRelayServer
docker compose up --build fds-gateway
```

?�존: `redis`, `kafka`, `triton`??compose?�서 ?�께 기동?�니??

### Gateway ?��?지�?

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

## 2. 로컬 개발 ?�행

```powershell
cd MiddleWare\WooriCardFDSGateway
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Redis·Kafka·Triton Mock??로컬?�서 ???�어???�니?? (Relay compose??`redis`/`kafka`/`triton`�??�워???�니??)

```powershell
cd MiddleWare\WooriCardCallBotRelayServer
docker compose up -d redis kafka triton
```

?�경 변???�시:

```powershell
$env:REDIS_HOST = "localhost"
$env:REDIS_PORT = "6379"
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
$env:KAFKA_WORKER_ENABLED = "true"
$env:TRITON_HTTP_URL = "http://localhost:8001"
uvicorn run:app --host 0.0.0.0 --port 8010 --reload
```

---

## 3. API ?�드?�인??

| 경로 | ?�명 |
|------|------|
| `GET /health` | ?�스체크 (`triton_ready`, `kafka_fds_pipeline` ?? |
| `GET /metrics` | Prometheus 메트�?|
| `POST /api/v1/fds/score` | FDS ?�코?�링 REST API |
| Internal ?�우??| ?��? Feature Store API (`app/routers/internal.py`) |

기본 ?�트: **8010**

gRPC (?�택):

```powershell
$env:GRPC_ENABLED = "true"
$env:GRPC_PORT = "50051"
```

---

## 4. Kafka ?�커

`KAFKA_WORKER_ENABLED=true`(기본)?�면 ??기동 ??백그?�운?�에???�음 ?�이?�라?�이 ?�작?�니??

```
wooricard-fds-events ??Feature Store(Redis) ???�코?�링
  ??wooricard-fds-scores ??Rule ?��? ??wooricard-fds-actions
```

?�싱 ?�패 ?? `wooricard-fds-events-dlq`

### 주요 Kafka ?�경 변??

| 변??| 기본�?|
|------|--------|
| `KAFKA_BOOTSTRAP_SERVERS` | localhost:9092 |
| `KAFKA_EVENTS_TOPIC` | wooricard-fds-events |
| `KAFKA_SCORES_TOPIC` | wooricard-fds-scores |
| `KAFKA_ACTIONS_TOPIC` | wooricard-fds-actions |
| `KAFKA_DLQ_TOPIC` | wooricard-fds-events-dlq |
| `KAFKA_CONSUMER_GROUP` | woori-fds-gateway |
| `KAFKA_DLQ_ENABLED` | true |

---

## 5. ?�경 변??(?�체)

`.env` ?�는 ?�에???�정 (`app/config.py`).

| 변??| 기본�?| ?�명 |
|------|--------|------|
| `TRITON_HTTP_URL` | http://localhost:8001 | Triton URL |
| `TRITON_FDS_MODEL` | fds_lgbm | FDS 모델�?|
| `TRITON_STARTUP_CHECK` | true | 기동 ??ready 검??|
| `API_KEY_ENABLED` | false | REST API ??|
| `INTERNAL_API_KEY` | woori-internal-dev-key | ?��? API ??|
| `RATE_LIMIT_ENABLED` | true | Rate limit |
| `METRICS_ENABLED` | true | Prometheus |
| `OTEL_ENABLED` | false | OpenTelemetry |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | http://localhost:4318/v1/traces | Jaeger |

---

## 6. ?�작 ?�인

```powershell
curl http://localhost:8010/health
curl http://localhost:8010/metrics
```

Kafka ?�벤?��? consume?�는지??Relay 경유 ?�화 ?�는 Relay가 `wooricard-fds-events`??발행????메트�?`fds_kafka_events_processed_total`�??�인?�니??

### ?�위 ?�스??

```powershell
pytest
```

### 계약 ?�기??

```powershell
python ..\scripts\sync_integration_contracts.py
```

---

## 7. Relay ?�동

Relay가 `wooricard-fds-events`??`callDirection`, `campaignId` ?�함 FdsEvent v1.0??발행?�면 FDS Gateway ?�커가 consume?�니?? Redis ?�션 ?? `wooricard:session:{inbound|outbound}:{sessionId}`.
