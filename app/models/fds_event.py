from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field


class FdsEvent(BaseModel):
    """Relay Kafka FdsEvent 스키마 v1.0."""

    model_config = ConfigDict(populate_by_name=True)

    session_id: str = Field(alias="sessionId")
    call_direction: str | None = Field(default=None, alias="callDirection")
    campaign_id: str | None = Field(default=None, alias="campaignId")
    event_type: str = Field(alias="eventType")
    fds_flag: str | None = Field(default=None, alias="fdsFlag")
    stt_text: str | None = Field(default=None, alias="sttText")
    fds_score: float | None = Field(default=None, alias="fdsScore")
    reason: str | None = None
    timestamp: datetime | None = None
    metadata: dict[str, Any] | None = None
    schema_version: str = Field(default="1.0", alias="schemaVersion")

    def to_event_dict(self) -> Dict[str, Any]:
        """기존 dict 기반 파이프라인 호환용 camelCase dict."""
        return self.model_dump(by_alias=True, exclude_none=True)
