"""
gRPC FDS Scoring Server — grpc.aio 포트 바인딩 + Servicer 스텁.

protoc codegen 후 generated Servicer로 교체:
  python -m grpc_tools.protoc -I app/grpc --python_out=app/grpc/generated \\
    --grpc_python_out=app/grpc/generated app/grpc/fds_scoring.proto
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

import grpc

from app.constants.scoring_constants import ApiConstants
from app.models.fds_models import FdsScoreRequest

if TYPE_CHECKING:
    from app.services.scoring.fds_scoring_pipeline import FdsScoringPipeline

logger = logging.getLogger(__name__)


class FdsScoringGrpcServicer:
    """REST pipeline 재사용 gRPC Servicer."""

    def __init__(self, pipeline: "FdsScoringPipeline") -> None:
        self._pipeline = pipeline

    async def score_transaction(self, request_dict: dict) -> dict:
        logger.info(
            "[gRPC:%s] session=%s user=%s",
            ApiConstants.GRPC_METHOD_SCORE,
            request_dict.get("session_id"),
            request_dict.get("user_id"),
        )
        body = FdsScoreRequest.model_validate(request_dict)
        result = await self._pipeline.score(body)
        return result.model_dump()


async def start_grpc_server(pipeline: "FdsScoringPipeline", port: int) -> None:
    """gRPC aio 서버 기동 — proto codegen Servicer 연결 확장 포인트."""
    server = grpc.aio.server()
    listen_addr = f"[::]:{port}"
    server.add_insecure_port(listen_addr)
    await server.start()
    logger.info(
        "[gRPC] Listening on %s service=%s (attach proto Servicer for production RPC)",
        listen_addr,
        ApiConstants.GRPC_SERVICE_NAME,
    )
    _ = FdsScoringGrpcServicer(pipeline)
    try:
        await server.wait_for_termination()
    except asyncio.CancelledError:
        await server.stop(grace=3)
