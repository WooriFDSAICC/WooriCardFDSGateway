from __future__ import annotations

import logging

import httpx

from app.constants.integration_contract import IntegrationContract

logger = logging.getLogger(__name__)


async def check_triton_ready(base_url: str, timeout_seconds: float = 3.0) -> bool:
    url = base_url.rstrip("/") + IntegrationContract.TRITON_HEALTH_READY_PATH
    try:
        async with httpx.AsyncClient(timeout=timeout_seconds) as client:
            response = await client.get(url)
            if response.status_code == 200:
                logger.info("[TritonHealth] Ready url=%s", base_url)
                return True
            logger.warning("[TritonHealth] Not ready url=%s status=%d", base_url, response.status_code)
    except Exception as exc:
        logger.warning("[TritonHealth] Unreachable url=%s error=%s", base_url, exc)
    return False
