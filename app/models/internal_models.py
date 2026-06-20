from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class EnrichedFeatureVector:
    """
    전처리 완료 피처 벡터.

    transaction_features: Rule Engine/Kafka에서 유입된 가변 거래 변수
    context_features:     Feature Store(Redis)에서 결합된 비금융/대화 맥락
    """

    entity_key: str
    txn_type: str
    transaction_features: Dict[str, Any] = field(default_factory=dict)
    context_features: Dict[str, Any] = field(default_factory=dict)

    @property
    def merged(self) -> Dict[str, Any]:
        merged = dict(self.transaction_features)
        merged.update(self.context_features)
        return merged

    @property
    def feature_count(self) -> int:
        return len(self.transaction_features)


@dataclass
class ModelInferenceResult:
    """ML/SLM 추론 결과."""

    raw_score: float
    normalized_score: int
    model_version: str = "mock-lgbm-v1"
    feature_importance: Dict[str, float] = field(default_factory=dict)


@dataclass
class RuleEvaluationResult:
    """룰 가중치 결합 및 사기 분류 축 매칭 결과."""

    fds_score: int
    fds_flag: str
    matched_rules: List[str] = field(default_factory=list)
    is_critical_override: bool = False
