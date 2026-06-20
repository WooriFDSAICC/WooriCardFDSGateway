from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class FdsScoreRequest(BaseModel):
    """
    가변형 FDS 스코어링 요청.

    국내(400변수) / 해외(200변수) 거래 피처가 고정 스키마 없이
    JSON Dict 형태로 유입될 수 있도록 extra='allow' 적용.
    """

    model_config = ConfigDict(extra="allow")

    session_id: Optional[str] = Field(default=None, description="통화/세션 식별자")
    user_id: Optional[str] = Field(default=None, description="고객 식별자")
    txn_type: Optional[str] = Field(default=None, description="DOMESTIC | OVERSEAS")
    txn_channel: Optional[str] = Field(default=None, description="거래 채널 코드")

    def to_feature_dict(self) -> Dict[str, Any]:
        """식별 메타를 포함한 전체 피처 딕셔너리 반환."""
        return self.model_dump(exclude_none=False)


class FdsScoreResponse(BaseModel):
    """FDS 스코어링 응답 — Rule Engine / Kafka 후행 모듈 연동 규격."""

    status: str
    fds_score: int = Field(ge=0, le=100)
    fds_flag: str
    matched_rules: List[str] = Field(default_factory=list)
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="부가 진단 정보(선택)")


class FdsScoreErrorResponse(BaseModel):
    status: str = "FAILED"
    message: str
    detail: Optional[str] = None
