from __future__ import annotations

import ipaddress
import logging
from typing import Optional

from fastapi import Header, HTTPException, Request, status

from app.config import settings

logger = logging.getLogger(__name__)


async def verify_api_key(x_api_key: Optional[str] = Header(default=None, alias="X-API-Key")) -> None:
    """Scoring API 인증 — X-API-Key 헤더."""
    if not settings.api_key_enabled:
        return
    if not x_api_key or x_api_key != settings.api_key:
        logger.warning("[Security] Invalid API key")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")


async def verify_internal_api_key(
    x_internal_api_key: Optional[str] = Header(default=None, alias="X-Internal-API-Key"),
) -> None:
    if not settings.internal_api_key_enabled:
        return
    if not x_internal_api_key or x_internal_api_key != settings.internal_api_key:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid internal API key")


async def verify_internal_network(request: Request) -> None:
    """Internal API — 허용 CIDR 대역만 접근."""
    if not settings.internal_network_guard_enabled:
        return

    client_host = request.client.host if request.client else None
    if not client_host:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")

    allowed = client_host in ("127.0.0.1", "::1")
    for cidr in settings.internal_allowed_networks:
        try:
            if ipaddress.ip_address(client_host) in ipaddress.ip_network(cidr, strict=False):
                allowed = True
                break
        except ValueError:
            continue

    if not allowed:
        logger.warning("[Security] Internal API blocked from %s", client_host)
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Internal network only")
