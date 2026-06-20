from pydantic_settings import BaseSettings, SettingsConfigDict

from app.constants.kafka_constants import KafkaConstants
from app.constants.schema_constants import FeatureStorePolicy
from app.constants.scoring_constants import ScoringConstants
from app.constants.triton import TritonConstants


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "WooriFdsGateway"
    host: str = "0.0.0.0"
    port: int = 8010

    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_password: str = ""
    redis_db: int = 0
    redis_feature_ttl_seconds: int = FeatureStorePolicy.TTL_SECONDS

    triton_http_url: str = TritonConstants.DEFAULT_HTTP_URL
    triton_fds_model: str = TritonConstants.DEFAULT_FDS_MODEL

    fraud_keyword_threshold: float = ScoringConstants.FRAUD_KEYWORD_THRESHOLD
    malicious_app_weight_threshold: float = ScoringConstants.MALICIOUS_APP_WEIGHT_THRESHOLD
    transfer_sum_30m_threshold: float = ScoringConstants.TRANSFER_SUM_30M_THRESHOLD
    transfer_count_30m_threshold: int = ScoringConstants.TRANSFER_COUNT_30M_THRESHOLD
    critical_score: int = ScoringConstants.CRITICAL_SCORE

    grpc_enabled: bool = False
    grpc_port: int = 50051

    api_key_enabled: bool = False
    api_key: str = "woori-fds-dev-key"

    internal_api_key_enabled: bool = True
    internal_api_key: str = "woori-internal-dev-key"

    internal_network_guard_enabled: bool = False
    internal_allowed_networks: list[str] = ["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"]

    rate_limit_enabled: bool = True
    rate_limit_per_minute: int = 120

    metrics_enabled: bool = True

    kafka_worker_enabled: bool = True
    kafka_bootstrap_servers: str = KafkaConstants.DEFAULT_BOOTSTRAP
    kafka_consumer_group: str = KafkaConstants.CONSUMER_GROUP
    kafka_events_topic: str = KafkaConstants.TOPIC_FDS_EVENTS
    kafka_scores_topic: str = KafkaConstants.TOPIC_FDS_SCORES
    kafka_actions_topic: str = KafkaConstants.TOPIC_FDS_ACTIONS

    score_on_stt_every_n: int = 0
    block_score_threshold: int = 90
    alert_score_threshold: int = 70

    alert_webhook_enabled: bool = False
    alert_webhook_url: str = ""

    triton_startup_check: bool = True

    kafka_dlq_enabled: bool = True
    kafka_dlq_topic: str = KafkaConstants.TOPIC_FDS_DLQ


settings = Settings()
