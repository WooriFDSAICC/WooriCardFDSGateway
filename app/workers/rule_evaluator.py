from __future__ import annotations

import logging
from typing import Any, Dict, List

from app.config import settings
from app.constants.kafka_constants import RuleAction, RuleThresholds

logger = logging.getLogger(__name__)


class RuleEvaluator:
    """FDS 스코어링 결과 → 최종 조치(BLOCK/ALERT/APPROVE) 판정."""

    def evaluate(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        scoring = payload.get("scoringResult") or {}
        fds_score = int(scoring.get("fds_score", 0))
        fds_flag = (scoring.get("fds_flag") or "").upper()
        matched_rules: List[str] = scoring.get("matched_rules") or []

        session_id = payload.get("sessionId", "")
        entity_key = payload.get("entityKey", "")

        action = RuleAction.APPROVE
        reason = "Score within normal range"

        if fds_flag == RuleThresholds.CRITICAL_FLAG or fds_score >= settings.block_score_threshold:
            action = RuleAction.BLOCK
            reason = "Critical fraud score — transaction blocked"
        elif fds_score >= settings.alert_score_threshold:
            action = RuleAction.ALERT
            reason = "Elevated fraud score — additional verification required"

        result = {
            "schemaVersion": "1.0",
            "sessionId": session_id,
            "entityKey": entity_key,
            "action": action,
            "reason": reason,
            "fdsScore": fds_score,
            "fdsFlag": fds_flag,
            "matchedRules": matched_rules,
            "sourceEventType": payload.get("sourceEventType"),
        }

        logger.info(
            "[RuleEvaluator] session=%s action=%s score=%d flag=%s rules=%s",
            session_id,
            action,
            fds_score,
            fds_flag,
            matched_rules,
        )
        return result
