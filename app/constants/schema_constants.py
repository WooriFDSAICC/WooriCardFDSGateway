from dataclasses import dataclass


from app.constants.integration_contract import (
    FEATURE_STORE_KEY_PREFIX,
    SCHEMA_VERSION,
    TOPIC_FDS_ACTIONS,
    TOPIC_FDS_EVENTS,
    TOPIC_FDS_SCORES,
)


@dataclass(frozen=True)
class SchemaConstants:
    """Kafka / API 스키마 버전 관리."""

    VERSION: str = SCHEMA_VERSION
    HEADER_SCHEMA_VERSION: str = "X-Schema-Version"

    EVENTS_TOPIC: str = TOPIC_FDS_EVENTS
    SCORES_TOPIC: str = TOPIC_FDS_SCORES
    ACTIONS_TOPIC: str = TOPIC_FDS_ACTIONS


@dataclass(frozen=True)
class FeatureStorePolicy:
    """
    Feature Store 키/TTL 정책.
    운영 기준: user_id 우선, session_id는 통화 중 임시 키 → user_id로 merge.
    """

    PRIMARY_KEY_FIELD: str = "user_id"
    FALLBACK_KEY_FIELD: str = "session_id"
    REDIS_KEY_PREFIX: str = FEATURE_STORE_KEY_PREFIX
    TTL_SECONDS: int = 3600


@dataclass(frozen=True)
class DomesticTransactionSchema:
    """국내 거래 핵심 변수 (400개 중 대표 필드 — extra='allow'로 확장)."""

    EXPECTED_VARIABLE_COUNT: int = 400
    REQUIRED_META: tuple = ("txn_type",)

    # 대표 변수명 (명세서 v1.0)
    TRANSFER_AMOUNT: str = "transfer_amount"
    TRANSFER_COUNT_30M: str = "transfer_count_30m"
    FRAUD_KEYWORD_FLAG: str = "fraud_keyword_accumulation_flag"
    MALICIOUS_APP_WEIGHT: str = "malicious_app_weight"
    CARD_PRESENT: str = "card_present"
    MERCHANT_CATEGORY: str = "merchant_category"


@dataclass(frozen=True)
class OverseasTransactionSchema:
    """해외 거래 핵심 변수 (200개 중 대표 필드)."""

    EXPECTED_VARIABLE_COUNT: int = 200
    OVERSEAS_COUNTRY_RISK: str = "overseas_country_risk_score"
    OVERSEAS_MERCHANT_ID: str = "overseas_merchant_id"
    FX_AMOUNT: str = "fx_amount"
