from __future__ import annotations

import time
from collections import defaultdict
from threading import Lock

from fastapi import HTTPException, Request, status

from app.config import settings


class InMemoryRateLimiter:
    """/v1/fds/score IP 기준 분당 호출 제한."""

    def __init__(self) -> None:
        self._buckets: dict[str, list[float]] = defaultdict(list)
        self._lock = Lock()

    def check(self, client_key: str) -> None:
        if not settings.rate_limit_enabled:
            return

        now = time.time()
        window = 60.0
        limit = settings.rate_limit_per_minute

        with self._lock:
            timestamps = self._buckets[client_key]
            self._buckets[client_key] = [t for t in timestamps if now - t < window]
            if len(self._buckets[client_key]) >= limit:
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail="Rate limit exceeded",
                )
            self._buckets[client_key].append(now)


rate_limiter = InMemoryRateLimiter()


async def apply_rate_limit(request: Request) -> None:
    client_host = request.client.host if request.client else "unknown"
    rate_limiter.check(client_host)
