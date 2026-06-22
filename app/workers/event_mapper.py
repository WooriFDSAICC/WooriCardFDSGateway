from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional, Tuple

from pydantic import ValidationError

from app.constants.integration_contract import (
    CALL_DIRECTION_INBOUND,
    CALL_DIRECTION_OUTBOUND,
    EVENT_AGENT_ESCALATION,
    EVENT_STT_PARTIAL,
    SCHEMA_VERSION,
)
from app.constants.kafka_constants import PhishingKeywords, WorkerFeatureStoreConstants
from app.models.fds_event import FdsEvent

logger = logging.getLogger(__name__)


class EventParseError(Exception):
    """FdsEvent 파싱/검증 실패."""


class EventMapper:
    """Kafka FdsEvent JSON → Feature Store / Scoring Request 매핑."""

    @staticmethod
    def parse_event(raw: str) -> Dict[str, Any]:
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise EventParseError(f"Invalid JSON: {exc}") from exc

        try:
            fds_event = FdsEvent.model_validate(payload)
        except ValidationError as exc:
            raise EventParseError(f"FdsEvent validation failed: {exc}") from exc

        if fds_event.schema_version != SCHEMA_VERSION:
            logger.warning(
                "[EventMapper] Unsupported schemaVersion=%s session=%s — proceeding with compat mode",
                fds_event.schema_version,
                fds_event.session_id,
            )

        if not fds_event.call_direction:
            raise EventParseError("callDirection is required")

        direction = fds_event.call_direction.upper()
        if direction not in (CALL_DIRECTION_INBOUND, CALL_DIRECTION_OUTBOUND):
            raise EventParseError(f"Invalid callDirection: {fds_event.call_direction}")

        return fds_event.to_event_dict()

    @staticmethod
    def parse_kafka_key(key: bytes | None) -> Tuple[Optional[str], Optional[str]]:
        """Relay partition key `{direction}:{sessionId}` 파싱."""
        if not key:
            return None, None
        try:
            text = key.decode("utf-8")
            if ":" not in text:
                return None, text
            direction_part, session_id = text.split(":", 1)
            return direction_part.upper(), session_id
        except Exception:
            return None, None

    @staticmethod
    def entity_key(event: Dict[str, Any]) -> Optional[str]:
        metadata = event.get("metadata") or {}
        return event.get("userId") or metadata.get("user_id") or event.get("sessionId")

    @staticmethod
    def session_registry_key(event: Dict[str, Any]) -> Optional[str]:
        session_id = event.get("sessionId")
        direction = (event.get("callDirection") or CALL_DIRECTION_INBOUND).lower()
        if not session_id:
            return None
        return f"wooricard:session:{direction}:{session_id}"

    @staticmethod
    def count_phishing_keywords(stt_text: Optional[str]) -> int:
        if not stt_text:
            return 0
        lowered = stt_text.lower()
        return sum(1 for kw in PhishingKeywords.KEYWORDS if kw.lower() in lowered)

    @staticmethod
    def outbound_score_multiplier(campaign_id: Optional[str]) -> float:
        """아웃바운드 캠페인별 FDS 민감도 — 캠페인 ID 기반 단순 정책."""
        if not campaign_id:
            return 1.0
        if campaign_id.upper().startswith("HIGH_RISK"):
            return 1.25
        return 1.0

    @staticmethod
    def to_scoring_request(
        event: Dict[str, Any],
        context_features: Dict[str, Any],
    ) -> Dict[str, Any]:
        session_id = event.get("sessionId")
        metadata = event.get("metadata") or {}
        user_id = metadata.get("user_id")
        call_direction = (event.get("callDirection") or CALL_DIRECTION_INBOUND).upper()
        campaign_id = event.get("campaignId")

        stt_keyword_hits = EventMapper.count_phishing_keywords(event.get("sttText"))
        malicious_app = int(context_features.get(WorkerFeatureStoreConstants.FIELD_MALICIOUS_APP_ACTIVE, 0))

        keyword_flag = float(
            context_features.get(WorkerFeatureStoreConstants.FIELD_STT_KEYWORD_COUNT, stt_keyword_hits)
        )
        if call_direction == CALL_DIRECTION_OUTBOUND:
            keyword_flag *= EventMapper.outbound_score_multiplier(campaign_id)

        body: Dict[str, Any] = {
            "session_id": session_id,
            "user_id": user_id,
            "txn_type": metadata.get("txn_type", "DOMESTIC"),
            "fraud_keyword_accumulation_flag": keyword_flag,
            "malicious_app_weight": 0.92 if malicious_app == 1 else float(metadata.get("malicious_app_weight", 0)),
            "transfer_count_30m": int(metadata.get("transfer_count_30m", 0)),
            "source_event_type": event.get("eventType"),
            "source_fds_flag": event.get("fdsFlag"),
            "call_direction": call_direction,
            "campaign_id": campaign_id,
        }

        fds_score = event.get("fdsScore")
        if fds_score is not None:
            body["voice_fds_score"] = fds_score
            if call_direction == CALL_DIRECTION_OUTBOUND:
                body["voice_fds_score"] = float(fds_score) * EventMapper.outbound_score_multiplier(campaign_id)

        return body

    @staticmethod
    def should_trigger_scoring(event: Dict[str, Any], stt_event_count: int, score_every_n: int) -> bool:
        event_type = (event.get("eventType") or "").upper()
        fds_flag = (event.get("fdsFlag") or "").upper()

        if event_type == EVENT_AGENT_ESCALATION or fds_flag == "CRITICAL":
            return True
        if event_type == EVENT_STT_PARTIAL and score_every_n > 0 and stt_event_count % score_every_n == 0:
            return True
        return False
