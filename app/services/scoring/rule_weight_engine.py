from __future__ import annotations

import logging
from typing import Any, Dict, List

from app.config import settings
from app.constants.scoring_constants import FeatureStoreConstants, ScoringConstants, TransactionConstants
from app.models.internal_models import EnrichedFeatureVector, ModelInferenceResult, RuleEvaluationResult

logger = logging.getLogger(__name__)


class RuleWeightEngine:
    """
    룰 가중치 결합 영역.

    금융 비즈니스 관점:
    - ML 점수만으로는 설명 가능성(explainability) 및 규제 대응이 어려움
    - 6축 사기 분류 룰(보이스피싱·원격앱·다액이체 등)과 ML 점수를 결합하여
      matched_rules[]로 Rule Engine/Kafka 후행 처리에 전달
    - 특정 위험 키워드·악성앱 가중치 초과 시 CRITICAL 강제 승격(95점)
    """

    def evaluate(
        self,
        vector: EnrichedFeatureVector,
        ml_result: ModelInferenceResult,
    ) -> RuleEvaluationResult:
        merged = vector.merged
        matched_rules: List[str] = []

        fraud_keyword_flag = float(
            merged.get(ScoringConstants.KEY_FRAUD_KEYWORD_ACCUMULATION, 0) or 0
        )
        malicious_app_weight = float(
            merged.get(ScoringConstants.KEY_MALICIOUS_APP_WEIGHT, 0) or 0
        )
        transfer_count_30m = int(merged.get(ScoringConstants.KEY_TRANSFER_COUNT_30M, 0) or 0)
        transfer_sum_ctx = float(
            vector.context_features.get(FeatureStoreConstants.FIELD_TRANSFER_SUM_30M, 0)
        )
        stt_keyword_count = int(
            vector.context_features.get(FeatureStoreConstants.FIELD_STT_KEYWORD_COUNT, 0)
        )
        malicious_app_active = int(
            vector.context_features.get(FeatureStoreConstants.FIELD_MALICIOUS_APP_ACTIVE, 0)
        )

        # ── 시뮬레이션: 특정 위험 지표 초과 → CRITICAL 95점 강제 ──
        critical_override = (
            fraud_keyword_flag >= settings.fraud_keyword_threshold
            or malicious_app_weight >= settings.malicious_app_weight_threshold
        )

        if critical_override or malicious_app_active == 1:
            matched_rules.append(ScoringConstants.RULE_VOICE_PHISHING_REMOTE_APP)

        if stt_keyword_count >= settings.fraud_keyword_threshold:
            matched_rules.append(ScoringConstants.RULE_STT_PHISHING_KEYWORD)

        if (
            vector.txn_type == TransactionConstants.TYPE_DOMESTIC
            and (
                transfer_sum_ctx >= settings.transfer_sum_30m_threshold
                or transfer_count_30m >= settings.transfer_count_30m_threshold
            )
        ):
            matched_rules.append(ScoringConstants.RULE_DOMESTIC_LARGE_TRANSFER)

        if vector.txn_type == TransactionConstants.TYPE_OVERSEAS:
            overseas_risk = float(merged.get("overseas_country_risk_score", 0) or 0)
            if overseas_risk >= 0.8:
                matched_rules.append(ScoringConstants.RULE_OVERSEAS_HIGH_RISK_COUNTRY)

        fds_score = ml_result.normalized_score
        fds_flag = self._score_to_flag(fds_score)

        if critical_override:
            fds_score = settings.critical_score
            fds_flag = ScoringConstants.FLAG_CRITICAL
            if ScoringConstants.RULE_VOICE_PHISHING_REMOTE_APP not in matched_rules:
                matched_rules.append(ScoringConstants.RULE_VOICE_PHISHING_REMOTE_APP)

        # 중복 룰 제거 (순서 유지)
        matched_rules = list(dict.fromkeys(matched_rules))

        logger.info(
            "[RuleEngine] entity=%s score=%d flag=%s rules=%s override=%s",
            vector.entity_key,
            fds_score,
            fds_flag,
            matched_rules,
            critical_override,
        )

        return RuleEvaluationResult(
            fds_score=fds_score,
            fds_flag=fds_flag,
            matched_rules=matched_rules,
            is_critical_override=critical_override,
        )

    @staticmethod
    def _score_to_flag(score: int) -> str:
        if score >= 90:
            return ScoringConstants.FLAG_CRITICAL
        if score >= 70:
            return ScoringConstants.FLAG_WARNING
        return ScoringConstants.FLAG_NORMAL
