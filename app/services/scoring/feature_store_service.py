from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import redis.asyncio as aioredis

from app.config import settings
from app.constants.scoring_constants import FeatureStoreConstants, TransactionConstants

logger = logging.getLogger(__name__)


class FeatureStoreService:
    """
    실시간 Feature Store(Redis) 매핑 서비스.

    금융 비즈니스 관점:
    - 거래 변수(400/200)만으로는 보이스피싱·원격앱 등 비금융 맥락 탐지가 불가
    - session_id/user_id 기준으로 STT 키워드, 30분 이체 합계, 악성앱 구동 여부를
      Redis Hash에서 비동기 조회하여 ML 입력 벡터에 결합한다.
    """

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
        logger.info(
            "[FeatureStore] Redis connected host=%s port=%d",
            settings.redis_host,
            settings.redis_port,
        )

    async def shutdown(self) -> None:
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None

    async def fetch_context_features(
        self,
        session_id: Optional[str],
        user_id: Optional[str],
    ) -> Dict[str, Any]:
        entity_key = self._resolve_entity_key(session_id, user_id)
        if not entity_key or self._redis is None:
            return self._default_context_features()

        redis_key = FeatureStoreConstants.KEY_PREFIX + entity_key
        try:
            stored = await self._redis.hgetall(redis_key)
            if not stored:
                # Mock: Redis 미적재 시 시뮬레이션 기본값 반환 (로컬/부하 테스트용)
                return self._mock_context_features(entity_key, session_id)

            return {
                FeatureStoreConstants.FIELD_TRANSFER_SUM_30M: float(
                    stored.get(FeatureStoreConstants.FIELD_TRANSFER_SUM_30M, 0)
                ),
                FeatureStoreConstants.FIELD_STT_KEYWORD_COUNT: int(
                    stored.get(FeatureStoreConstants.FIELD_STT_KEYWORD_COUNT, 0)
                ),
                FeatureStoreConstants.FIELD_MALICIOUS_APP_ACTIVE: int(
                    stored.get(FeatureStoreConstants.FIELD_MALICIOUS_APP_ACTIVE, 0)
                ),
                FeatureStoreConstants.FIELD_LAST_STT_TEXT: stored.get(
                    FeatureStoreConstants.FIELD_LAST_STT_TEXT, ""
                ),
            }
        except Exception as exc:
            logger.warning("[FeatureStore] Redis read failed entity=%s: %s", entity_key, exc)
            return self._mock_context_features(entity_key, session_id)

    async def upsert_context_features(
        self,
        entity_key: str,
        features: Dict[str, Any],
    ) -> None:
        """운영/테스트: Feature Store 수동 적재 API (내부 사용)."""
        if self._redis is None:
            return
        redis_key = FeatureStoreConstants.KEY_PREFIX + entity_key
        mapping = {k: str(v) for k, v in features.items()}
        await self._redis.hset(redis_key, mapping=mapping)
        await self._redis.expire(redis_key, settings.redis_feature_ttl_seconds)

    @staticmethod
    def _resolve_entity_key(
        session_id: Optional[str],
        user_id: Optional[str],
    ) -> Optional[str]:
        if user_id:
            return user_id
        if session_id:
            return session_id
        return None

    @staticmethod
    def _default_context_features() -> Dict[str, Any]:
        return {
            FeatureStoreConstants.FIELD_TRANSFER_SUM_30M: 0.0,
            FeatureStoreConstants.FIELD_STT_KEYWORD_COUNT: 0,
            FeatureStoreConstants.FIELD_MALICIOUS_APP_ACTIVE: 0,
            FeatureStoreConstants.FIELD_LAST_STT_TEXT: "",
        }

    def _mock_context_features(
        self,
        entity_key: str,
        session_id: Optional[str],
    ) -> Dict[str, Any]:
        """
        Feature Store Mock — entity_key 해시 기반 결정적 시뮬레이션.
        session_id가 'critical' 포함 시 고위험 맥락 피처 주입.
        """
        is_critical_hint = (
            (session_id and "critical" in session_id.lower())
            or "critical" in entity_key.lower()
        )
        if is_critical_hint:
            return {
                FeatureStoreConstants.FIELD_TRANSFER_SUM_30M: 8_500_000.0,
                FeatureStoreConstants.FIELD_STT_KEYWORD_COUNT: 4,
                FeatureStoreConstants.FIELD_MALICIOUS_APP_ACTIVE: 1,
                FeatureStoreConstants.FIELD_LAST_STT_TEXT: "검찰청 금융범죄수사과입니다",
            }
        seed = sum(ord(c) for c in entity_key) % 100
        return {
            FeatureStoreConstants.FIELD_TRANSFER_SUM_30M: float(seed * 50_000),
            FeatureStoreConstants.FIELD_STT_KEYWORD_COUNT: seed % 3,
            FeatureStoreConstants.FIELD_MALICIOUS_APP_ACTIVE: 1 if seed > 85 else 0,
            FeatureStoreConstants.FIELD_LAST_STT_TEXT: "",
        }
