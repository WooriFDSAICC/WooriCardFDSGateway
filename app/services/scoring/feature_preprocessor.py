from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from app.constants.scoring_constants import TransactionConstants
from app.models.fds_models import FdsScoreRequest
from app.models.internal_models import EnrichedFeatureVector

logger = logging.getLogger(__name__)


class FeaturePreprocessor:
    """
    데이터 전처리 영역.

    금융 비즈니스 관점:
    - 국내(400변수)/해외(200변수) 거래 피처는 스키마가 확장 가능해야 함
    - txn_type 자동 추론 및 변수 개수 검증으로 데이터 품질을 사전 통제
    - Feature Store 맥락 피처와 거래 피처를 단일 추론 벡터로 정규화
    """

    def build_enriched_vector(
        self,
        request: FdsScoreRequest,
        context_features: Dict[str, Any],
    ) -> EnrichedFeatureVector:
        raw_features = request.to_feature_dict()
        entity_key = self._resolve_entity_key(request)
        txn_type = self._resolve_txn_type(request, raw_features)

        # 식별 메타 필드는 ML 입력에서 분리 (피처 벡터 = 순수 거래 변수)
        transaction_features = {
            k: v
            for k, v in raw_features.items()
            if k
            not in {
                TransactionConstants.KEY_SESSION_ID,
                TransactionConstants.KEY_USER_ID,
                TransactionConstants.KEY_TXN_TYPE,
                TransactionConstants.KEY_TXN_CHANNEL,
            }
            and v is not None
        }

        self._validate_variable_count(txn_type, len(transaction_features))

        logger.debug(
            "[Preprocessor] entity=%s txn_type=%s txn_vars=%d context_vars=%d",
            entity_key,
            txn_type,
            len(transaction_features),
            len(context_features),
        )

        return EnrichedFeatureVector(
            entity_key=entity_key or "anonymous",
            txn_type=txn_type,
            transaction_features=transaction_features,
            context_features=context_features,
        )

    @staticmethod
    def _resolve_entity_key(request: FdsScoreRequest) -> Optional[str]:
        return request.user_id or request.session_id

    @staticmethod
    def _resolve_txn_type(request: FdsScoreRequest, raw: Dict[str, Any]) -> str:
        explicit = request.txn_type or raw.get(TransactionConstants.KEY_TXN_TYPE)
        if explicit:
            return str(explicit).upper()
        channel = request.txn_channel or raw.get(TransactionConstants.KEY_TXN_CHANNEL, "")
        if "OVERSEAS" in str(channel).upper() or "INTL" in str(channel).upper():
            return TransactionConstants.TYPE_OVERSEAS
        return TransactionConstants.TYPE_DOMESTIC

    @staticmethod
    def _validate_variable_count(txn_type: str, count: int) -> None:
        """
        변수 개수 검증 — 운영에서는 soft warning, 스키마 확장 시 hard limit 완화.
        0개도 허용(최소 식별자만으로 스코어링 요청 가능).
        """
        if txn_type == TransactionConstants.TYPE_DOMESTIC and count > TransactionConstants.DOMESTIC_VARIABLE_COUNT:
            logger.warning(
                "[Preprocessor] Domestic variable count %d exceeds expected %d",
                count,
                TransactionConstants.DOMESTIC_VARIABLE_COUNT,
            )
        elif txn_type == TransactionConstants.TYPE_OVERSEAS and count > TransactionConstants.OVERSEAS_VARIABLE_COUNT:
            logger.warning(
                "[Preprocessor] Overseas variable count %d exceeds expected %d",
                count,
                TransactionConstants.OVERSEAS_VARIABLE_COUNT,
            )
