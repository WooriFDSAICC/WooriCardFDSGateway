"""AI팀·백엔드 연동 계약 (스키마 v1.0). Relay IntegrationContracts.java 와 동기화."""
from dataclasses import dataclass


@dataclass(frozen=True)
class IntegrationContract:
    SCHEMA_VERSION: str = "1.0"

    TOPIC_FDS_EVENTS: str = "wooricard-fds-events"
    TOPIC_FDS_SCORES: str = "wooricard-fds-scores"
    TOPIC_FDS_ACTIONS: str = "wooricard-fds-actions"
    TOPIC_FDS_DLQ: str = "wooricard-fds-events-dlq"

    EVENT_STT_PARTIAL: str = "STT_PARTIAL"
    EVENT_AGENT_ESCALATION: str = "AGENT_ESCALATION"
    EVENT_SESSION_ENDED: str = "SESSION_ENDED"

    TRITON_MODEL_FDS: str = "fds_lgbm"
    TRITON_INFER_PATH: str = "/v2/models/{model}/infer"
    TRITON_HEALTH_READY_PATH: str = "/v2/health/ready"

    INPUT_FEATURES_JSON: str = "FEATURES_JSON"
    INPUT_CONTEXT_JSON: str = "CONTEXT_JSON"

    OUTPUT_RAW_SCORE: str = "RAW_SCORE"
    OUTPUT_NORMALIZED_SCORE: str = "NORMALIZED_SCORE"
    OUTPUT_MODEL_VERSION: str = "MODEL_VERSION"

    FEATURE_STORE_KEY_PREFIX: str = "fds:feature:"
