from dataclasses import dataclass


@dataclass(frozen=True)
class TransactionConstants:
    """국내/해외 거래 변수 규격."""

    TYPE_DOMESTIC: str = "DOMESTIC"
    TYPE_OVERSEAS: str = "OVERSEAS"

    DOMESTIC_VARIABLE_COUNT: int = 400
    OVERSEAS_VARIABLE_COUNT: int = 200

    # 요청 바디에서 식별자로 사용하는 표준 키
    KEY_SESSION_ID: str = "session_id"
    KEY_USER_ID: str = "user_id"
    KEY_TXN_TYPE: str = "txn_type"
    KEY_TXN_CHANNEL: str = "txn_channel"


@dataclass(frozen=True)
class FeatureStoreConstants:
    """Redis Feature Store 키 및 피처 필드."""

    KEY_PREFIX: str = "fds:feature:"
    KEY_CONTEXT_PREFIX: str = "fds:context:"

    FIELD_TRANSFER_SUM_30M: str = "transfer_sum_30m"
    FIELD_STT_KEYWORD_COUNT: str = "stt_keyword_count"
    FIELD_MALICIOUS_APP_ACTIVE: str = "malicious_app_active"
    FIELD_LAST_STT_TEXT: str = "last_stt_text"

    TTL_SECONDS: int = 3600


@dataclass(frozen=True)
class ScoringConstants:
    """FDS 스코어링 임계값 및 출력 상수."""

    STATUS_SUCCESS: str = "SUCCESS"
    STATUS_FAILED: str = "FAILED"

    FLAG_NORMAL: str = "NORMAL"
    FLAG_WARNING: str = "WARNING"
    FLAG_CRITICAL: str = "CRITICAL"

    SCORE_MIN: int = 0
    SCORE_MAX: int = 100
    CRITICAL_SCORE: int = 95

    # 요청 변수 내 사기 시뮬레이션 트리거 키
    KEY_FRAUD_KEYWORD_ACCUMULATION: str = "fraud_keyword_accumulation_flag"
    KEY_MALICIOUS_APP_WEIGHT: str = "malicious_app_weight"
    KEY_TRANSFER_AMOUNT: str = "transfer_amount"
    KEY_TRANSFER_COUNT_30M: str = "transfer_count_30m"

    FRAUD_KEYWORD_THRESHOLD: float = 3.0
    MALICIOUS_APP_WEIGHT_THRESHOLD: float = 0.75
    TRANSFER_SUM_30M_THRESHOLD: float = 5_000_000.0
    TRANSFER_COUNT_30M_THRESHOLD: int = 5

    # 사기 분류 축(6축) 및 국내 룰 명칭
    RULE_VOICE_PHISHING_REMOTE_APP: str = "6축_보이스피싱_원격앱_탐지"
    RULE_DOMESTIC_LARGE_TRANSFER: str = "국내_단시간_다액이체"
    RULE_OVERSEAS_HIGH_RISK_COUNTRY: str = "해외_고위험국_거래"
    RULE_STT_PHISHING_KEYWORD: str = "6축_STT_피싱키워드_탐지"


@dataclass(frozen=True)
class ApiConstants:
    """REST / gRPC API 경로."""

    FDS_SCORE_PATH: str = "/v1/fds/score"
    HEALTH_PATH: str = "/health"
    GRPC_SERVICE_NAME: str = "woori.fds.v1.FdsScoringService"
    GRPC_METHOD_SCORE: str = "ScoreTransaction"
