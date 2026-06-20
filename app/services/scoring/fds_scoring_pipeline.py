from __future__ import annotations

import logging
from typing import Any, Dict

from app.constants.schema_constants import SchemaConstants
from app.constants.scoring_constants import ScoringConstants
from app.models.fds_models import FdsScoreRequest, FdsScoreResponse
from app.services.scoring.feature_preprocessor import FeaturePreprocessor
from app.services.scoring.feature_store_service import FeatureStoreService
from app.services.scoring.ml_inference_service import MlInferenceService
from app.services.scoring.rule_weight_engine import RuleWeightEngine

logger = logging.getLogger(__name__)


class FdsScoringPipeline:
    """
    FDS 스코어링 오케스트레이터.

    [1] Feature Store 결합 → [2] 전처리 → [3] ML 추론 → [4] 룰 결합 → [5] JSON 응답
    """

    def __init__(
        self,
        feature_store: FeatureStoreService,
        preprocessor: FeaturePreprocessor,
        ml_inference: MlInferenceService,
        rule_engine: RuleWeightEngine,
    ) -> None:
        self._feature_store = feature_store
        self._preprocessor = preprocessor
        self._ml_inference = ml_inference
        self._rule_engine = rule_engine

    async def score(self, request: FdsScoreRequest) -> FdsScoreResponse:
        # [1] Redis Feature Store — 비금융/대화 맥락 피처 비동기 결합
        context_features = await self._feature_store.fetch_context_features(
            session_id=request.session_id,
            user_id=request.user_id,
        )

        # [2] 가변형 거래 변수 전처리 및 벡터 정규화
        enriched_vector = self._preprocessor.build_enriched_vector(request, context_features)

        # [3] LightGBM/XGBoost Mock 실시간 추론
        ml_result = await self._ml_inference.infer(enriched_vector)

        # [4] 6축 사기 분류 룰 + ML 점수 결합
        rule_result = self._rule_engine.evaluate(enriched_vector, ml_result)

        # [5] Rule Engine / Kafka 후행 모듈 응답 규격
        return FdsScoreResponse(
            status=ScoringConstants.STATUS_SUCCESS,
            fds_score=rule_result.fds_score,
            fds_flag=rule_result.fds_flag,
            matched_rules=rule_result.matched_rules,
            metadata=self._build_metadata(enriched_vector, ml_result, rule_result),
        )

    @staticmethod
    def _build_metadata(enriched_vector, ml_result, rule_result) -> Dict[str, Any]:
        return {
            "schemaVersion": SchemaConstants.VERSION,
            "entity_key": enriched_vector.entity_key,
            "txn_type": enriched_vector.txn_type,
            "transaction_variable_count": enriched_vector.feature_count,
            "ml_raw_score": round(ml_result.raw_score, 4),
            "ml_model_version": ml_result.model_version,
            "critical_override": rule_result.is_critical_override,
        }
