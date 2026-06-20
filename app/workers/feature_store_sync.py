from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import redis.asyncio as aioredis

from app.config import settings
from app.constants.kafka_constants import WorkerFeatureStoreConstants
from app.workers.event_mapper import EventMapper

logger = logging.getLogger(__name__)


class FeatureStoreSyncService:
    """Kafka FDS 이벤트 → Redis Feature Store 실시간 적재."""

    def __init__(self) -> None:
        self._redis: Optional[aioredis.Redis] = None

    async def startup(self) -> None:
        self._redis = aioredis.Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            password=settings.redis_password or None,
            db=settings.redis_db,
            decode_responses=True,
        )

    async def shutdown(self) -> None:
        if self._redis:
            await self._redis.aclose()
            self._redis = None

    async def sync_from_event(self, event: Dict[str, Any]) -> Optional[str]:
        entity_key = EventMapper.entity_key(event)
        if not entity_key or self._redis is None:
            return None

        redis_key = WorkerFeatureStoreConstants.KEY_PREFIX + entity_key
        existing = await self._redis.hgetall(redis_key)

        stt_text = event.get("sttText") or ""
        keyword_hits = EventMapper.count_phishing_keywords(stt_text)
        current_count = int(existing.get(WorkerFeatureStoreConstants.FIELD_STT_KEYWORD_COUNT, 0))
        new_count = current_count + keyword_hits if keyword_hits > 0 else current_count

        metadata = event.get("metadata") or {}
        malicious_app = 1 if metadata.get("malicious_app_active") else int(
            existing.get(WorkerFeatureStoreConstants.FIELD_MALICIOUS_APP_ACTIVE, 0)
        )

        if (event.get("fdsFlag") or "").upper() == "CRITICAL":
            malicious_app = 1

        mapping = {
            WorkerFeatureStoreConstants.FIELD_STT_KEYWORD_COUNT: str(new_count),
            WorkerFeatureStoreConstants.FIELD_MALICIOUS_APP_ACTIVE: str(malicious_app),
            WorkerFeatureStoreConstants.FIELD_LAST_STT_TEXT: stt_text or existing.get(
                WorkerFeatureStoreConstants.FIELD_LAST_STT_TEXT, ""
            ),
            WorkerFeatureStoreConstants.FIELD_TRANSFER_SUM_30M: str(
                metadata.get(
                    WorkerFeatureStoreConstants.FIELD_TRANSFER_SUM_30M,
                    existing.get(WorkerFeatureStoreConstants.FIELD_TRANSFER_SUM_30M, 0),
                )
            ),
        }

        await self._redis.hset(redis_key, mapping=mapping)
        await self._redis.expire(redis_key, settings.redis_feature_ttl_seconds)

        logger.info(
            "[FeatureStoreSync] entity=%s stt_keywords=%d malicious_app=%d",
            entity_key,
            new_count,
            malicious_app,
        )
        return entity_key

    async def finalize_session(self, entity_key: str, event: Dict[str, Any]) -> None:
        """통화 종료 피드백 — Feature Store 세션 상태 기록 및 TTL 단축."""
        if self._redis is None:
            return

        metadata = event.get("metadata") or {}
        redis_key = WorkerFeatureStoreConstants.KEY_PREFIX + entity_key
        mapping = {
            WorkerFeatureStoreConstants.FIELD_SESSION_STATUS: str(
                metadata.get("finalStatus", "CLOSED")
            ),
        }
        reason = event.get("reason")
        if reason:
            mapping["termination_reason"] = str(reason)

        await self._redis.hset(redis_key, mapping=mapping)
        await self._redis.expire(redis_key, WorkerFeatureStoreConstants.SESSION_END_TTL_SECONDS)
        logger.info("[FeatureStoreSync] Session finalized entity=%s status=%s", entity_key, mapping.get("session_status"))

    async def get_context_features(self, entity_key: str) -> Dict[str, Any]:
        if self._redis is None:
            return {}
        redis_key = WorkerFeatureStoreConstants.KEY_PREFIX + entity_key
        stored = await self._redis.hgetall(redis_key)
        if not stored:
            return {}
        return {
            WorkerFeatureStoreConstants.FIELD_TRANSFER_SUM_30M: float(
                stored.get(WorkerFeatureStoreConstants.FIELD_TRANSFER_SUM_30M, 0)
            ),
            WorkerFeatureStoreConstants.FIELD_STT_KEYWORD_COUNT: int(
                stored.get(WorkerFeatureStoreConstants.FIELD_STT_KEYWORD_COUNT, 0)
            ),
            WorkerFeatureStoreConstants.FIELD_MALICIOUS_APP_ACTIVE: int(
                stored.get(WorkerFeatureStoreConstants.FIELD_MALICIOUS_APP_ACTIVE, 0)
            ),
            WorkerFeatureStoreConstants.FIELD_LAST_STT_TEXT: stored.get(
                WorkerFeatureStoreConstants.FIELD_LAST_STT_TEXT, ""
            ),
        }
