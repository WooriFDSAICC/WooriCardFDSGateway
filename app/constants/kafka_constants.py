from dataclasses import dataclass

from app.constants.integration_contract import (
    EVENT_AGENT_ESCALATION,
    EVENT_SESSION_ENDED,
    EVENT_STT_PARTIAL,
    FEATURE_STORE_KEY_PREFIX,
    SCHEMA_VERSION,
    TOPIC_FDS_ACTIONS,
    TOPIC_FDS_DLQ,
    TOPIC_FDS_EVENTS,
    TOPIC_FDS_SCORES,
)


@dataclass(frozen=True)
class KafkaConstants:
    DEFAULT_BOOTSTRAP: str = "localhost:9092"
    CONSUMER_GROUP: str = "woori-fds-gateway"

    TOPIC_FDS_EVENTS: str = TOPIC_FDS_EVENTS
    TOPIC_FDS_SCORES: str = TOPIC_FDS_SCORES
    TOPIC_FDS_ACTIONS: str = TOPIC_FDS_ACTIONS

    EVENT_STT_PARTIAL: str = EVENT_STT_PARTIAL
    EVENT_AGENT_ESCALATION: str = EVENT_AGENT_ESCALATION
    EVENT_SESSION_ENDED: str = EVENT_SESSION_ENDED
    SCHEMA_VERSION: str = SCHEMA_VERSION

    TOPIC_FDS_DLQ: str = TOPIC_FDS_DLQ


@dataclass(frozen=True)
class WorkerFeatureStoreConstants:
    KEY_PREFIX: str = FEATURE_STORE_KEY_PREFIX
    FIELD_TRANSFER_SUM_30M: str = "transfer_sum_30m"
    FIELD_STT_KEYWORD_COUNT: str = "stt_keyword_count"
    FIELD_MALICIOUS_APP_ACTIVE: str = "malicious_app_active"
    FIELD_LAST_STT_TEXT: str = "last_stt_text"
    FIELD_SESSION_STATUS: str = "session_status"
    FIELD_CALL_DIRECTION: str = "call_direction"
    FIELD_CAMPAIGN_ID: str = "campaign_id"
    TTL_SECONDS: int = 3600
    SESSION_END_TTL_SECONDS: int = 300


@dataclass(frozen=True)
class SessionRedisConstants:
    KEY_PREFIX: str = "wooricard:session:"
    FIELD_SESSION_ID: str = "session_id"
    FIELD_CALL_DIRECTION: str = "call_direction"
    FIELD_CAMPAIGN_ID: str = "campaign_id"
    FIELD_STATUS: str = "status"
    FIELD_FDS_FLAG: str = "fds_flag"
    FIELD_LAST_EVENT: str = "last_event"
    FIELD_LAST_STT_TEXT: str = "last_stt_text"
    FIELD_UPDATED_AT: str = "updated_at"


@dataclass(frozen=True)
class PhishingKeywords:
    KEYWORDS: tuple = (
        "검찰",
        "금융범죄",
        "안전계좌",
        "원격",
        "teamviewer",
        "anydesk",
        "송금",
        "이체",
        "계좌",
        "수사",
    )


@dataclass(frozen=True)
class RuleAction:
    BLOCK: str = "BLOCK"
    ALERT: str = "ALERT"
    APPROVE: str = "APPROVE"


@dataclass(frozen=True)
class RuleThresholds:
    CRITICAL_FLAG: str = "CRITICAL"
