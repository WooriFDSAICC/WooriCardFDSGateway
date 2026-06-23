from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator

from app.config import settings
from app import metrics as _metrics  # noqa: F401 — register Prometheus collectors
from app.constants.scoring_constants import ApiConstants
from app.metrics import APPLICATION, triton_up
from app.grpc.fds_scoring_servicer import start_grpc_server
from app.routers import fds_score, internal
from app.services.scoring.feature_preprocessor import FeaturePreprocessor
from app.services.scoring.feature_store_service import FeatureStoreService
from app.services.scoring.fds_scoring_pipeline import FdsScoringPipeline
from app.services.scoring.ml_inference_service import MlInferenceService
from app.services.scoring.rule_weight_engine import RuleWeightEngine
from app.workers.fds_event_pipeline import FdsEventPipelineWorker
from app.utils.log_context import configure_logging
from app.utils.otel import instrument_fastapi
from app.utils.triton_health import check_triton_ready

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
configure_logging()
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    if settings.triton_startup_check:
        ready = await check_triton_ready(settings.triton_http_url)
        triton_up.labels(application=APPLICATION).set(1 if ready else 0)
        if not ready:
            raise RuntimeError(f"Triton not ready: {settings.triton_http_url}")

    feature_store = FeatureStoreService()
    await feature_store.startup()

    ml_inference = MlInferenceService()
    await ml_inference.startup()

    fds_pipeline = FdsScoringPipeline(
        feature_store=feature_store,
        preprocessor=FeaturePreprocessor(),
        ml_inference=ml_inference,
        rule_engine=RuleWeightEngine(),
    )

    app.state.feature_store = feature_store
    app.state.fds_scoring_pipeline = fds_pipeline

    grpc_task = None
    if settings.grpc_enabled:
        grpc_task = asyncio.create_task(start_grpc_server(fds_pipeline, settings.grpc_port))

    fds_worker: FdsEventPipelineWorker | None = None
    fds_worker_task: asyncio.Task | None = None
    if settings.kafka_worker_enabled:
        fds_worker = FdsEventPipelineWorker(fds_pipeline=fds_pipeline)
        await fds_worker.startup()
        fds_worker_task = asyncio.create_task(fds_worker.run_forever())

    logger.info(
        "%s started port=%d triton_url=%s metrics=%s grpc=%s kafka_worker=%s",
        settings.app_name,
        settings.port,
        settings.triton_http_url,
        settings.metrics_enabled,
        settings.grpc_enabled,
        settings.kafka_worker_enabled,
    )

    yield

    if fds_worker_task:
        fds_worker_task.cancel()
        try:
            await fds_worker_task
        except asyncio.CancelledError:
            pass
    if fds_worker:
        await fds_worker.shutdown()
    if grpc_task:
        grpc_task.cancel()
    await ml_inference.shutdown()
    await feature_store.shutdown()
    logger.info("%s shutdown complete", settings.app_name)


app = FastAPI(
    title=settings.app_name,
    version="1.0.0",
    description="우리은행 FDS 게이트웨이 — 가변형 스코어링 + Kafka 이벤트 파이프라인",
    lifespan=lifespan,
)

instrument_fastapi(app)

if settings.metrics_enabled:
    Instrumentator().instrument(app).expose(app, endpoint="/metrics", include_in_schema=False)

app.include_router(fds_score.router)
app.include_router(internal.router)


@app.get(ApiConstants.HEALTH_PATH)
async def health_check():
    ready = await check_triton_ready(settings.triton_http_url)
    triton_up.labels(application=APPLICATION).set(1 if ready else 0)
    return {
        "status": "UP" if ready else "DEGRADED",
        "service": settings.app_name,
        "capabilities": {
            "fds_scoring": True,
            "kafka_fds_pipeline": settings.kafka_worker_enabled,
            "grpc_fds_scoring": settings.grpc_enabled,
        },
        "triton_http_url": settings.triton_http_url,
        "triton_fds_model": settings.triton_fds_model,
        "triton_ready": ready,
        "grpc_enabled": settings.grpc_enabled,
        "api_key_enabled": settings.api_key_enabled,
    }
