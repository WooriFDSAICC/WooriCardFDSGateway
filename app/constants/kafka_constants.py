from dataclasses import dataclass


@dataclass(frozen=True)
class KafkaConstants:
    DEFAULT_BOOTSTRAP: str = "localhost:9092"
    CONSUMER_GROUP: str = "woori-fds-gateway"

    TOPIC_FDS_EVENTS: str = "wooricard-fds-events"
    TOPIC_FDS_SCORES: str = "wooricard-fds-scores"
    TOPIC_FDS_ACTIONS: str = "wooricard-fds-actions"

    EVENT_STT_PARTIAL: str = "STT_PARTIAL"
    EVENT_AGENT_ESCALATION: str = "AGENT_ESCALATION"
    EVENT_SESSION_ENDED: str = "SESSION_ENDED"
    SCHEMA_VERSION: str = "1.0"

    TOPIC_FDS_DLQ: str = "wooricard-fds-events-dlq"


@dataclass(frozen=True)
class WorkerFeatureStoreConstants:
    KEY_PREFIX: str = "fds:feature:"
    FIELD_TRANSFER_SUM_30M: str = "transfer_sum_30m"
    FIELD_STT_KEYWORD_COUNT: str = "stt_keyword_count"
    FIELD_MALICIOUS_APP_ACTIVE: str = "malicious_app_active"
    FIELD_LAST_STT_TEXT: str = "last_stt_text"
    FIELD_SESSION_STATUS: str = "session_status"
    TTL_SECONDS: int = 3600
    SESSION_END_TTL_SECONDS: int = 300


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
