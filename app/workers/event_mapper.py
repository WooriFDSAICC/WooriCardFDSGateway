from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

from app.constants.kafka_constants import PhishingKeywords, WorkerFeatureStoreConstants

logger = logging.getLogger(__name__)


class EventMapper:
    """Kafka FdsEvent JSON → Feature Store / Scoring Request 매핑."""

    @staticmethod
    def parse_event(raw: str) -> Dict[str, Any]:
        return json.loads(raw)

    @staticmethod
    def entity_key(event: Dict[str, Any]) -> Optional[str]:
        metadata = event.get("metadata") or {}
        return event.get("userId") or metadata.get("user_id") or event.get("sessionId")

    @staticmethod
    def count_phishing_keywords(stt_text: Optional[str]) -> int:
        if not stt_text:
            return 0
        lowered = stt_text.lower()
        return sum(1 for kw in PhishingKeywords.KEYWORDS if kw.lower() in lowered)

    @staticmethod
    def to_scoring_request(
        event: Dict[str, Any],
        context_features: Dict[str, Any],
    ) -> Dict[str, Any]:
        session_id = event.get("sessionId")
        metadata = event.get("metadata") or {}
        user_id = metadata.get("user_id")

        stt_keyword_hits = EventMapper.count_phishing_keywords(event.get("sttText"))
        malicious_app = int(context_features.get(WorkerFeatureStoreConstants.FIELD_MALICIOUS_APP_ACTIVE, 0))

        body: Dict[str, Any] = {
            "session_id": session_id,
            "user_id": user_id,
            "txn_type": metadata.get("txn_type", "DOMESTIC"),
            "fraud_keyword_accumulation_flag": float(
                context_features.get(WorkerFeatureStoreConstants.FIELD_STT_KEYWORD_COUNT, stt_keyword_hits)
            ),
            "malicious_app_weight": 0.92 if malicious_app == 1 else float(metadata.get("malicious_app_weight", 0)),
            "transfer_count_30m": int(metadata.get("transfer_count_30m", 0)),
            "source_event_type": event.get("eventType"),
            "source_fds_flag": event.get("fdsFlag"),
        }

        fds_score = event.get("fdsScore")
        if fds_score is not None:
            body["voice_fds_score"] = fds_score

        return body

    @staticmethod
    def should_trigger_scoring(event: Dict[str, Any], stt_event_count: int, score_every_n: int) -> bool:
        event_type = (event.get("eventType") or "").upper()
        fds_flag = (event.get("fdsFlag") or "").upper()

        if event_type == "AGENT_ESCALATION" or fds_flag == "CRITICAL":
            return True
        if event_type == "STT_PARTIAL" and score_every_n > 0 and stt_event_count % score_every_n == 0:
            return True
        return False
