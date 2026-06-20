from dataclasses import dataclass


@dataclass(frozen=True)
class TritonConstants:
    DEFAULT_HTTP_URL: str = "http://localhost:8001"
    DEFAULT_FDS_MODEL: str = "fds_lgbm"
    INFER_PATH_TEMPLATE: str = "/v2/models/{model}/infer"
    HTTP_TIMEOUT_SECONDS: float = 10.0
    HTTP_CONNECT_TIMEOUT_SECONDS: float = 2.0
