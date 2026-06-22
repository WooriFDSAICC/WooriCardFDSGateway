"""FdsGateway — FdsEvent / EventMapper / DLQ 조건 tests."""
import json

import pytest

from app.constants.integration_contract import CALL_DIRECTION_INBOUND, EVENT_STT_PARTIAL
from app.models.fds_event import FdsEvent
from app.workers.event_mapper import EventMapper, EventParseError


def test_fds_event_parses_relay_payload():
    raw = json.dumps(
        {
            "sessionId": "call-001",
            "callDirection": "INBOUND",
            "campaignId": None,
            "eventType": "STT_PARTIAL",
            "fdsFlag": "NORMAL",
            "sttText": "카드 분실",
            "fdsScore": 0.12,
            "schemaVersion": "1.0",
            "timestamp": "2026-06-22T03:00:00Z",
        }
    )
    event = EventMapper.parse_event(raw)
    assert event["sessionId"] == "call-001"
    assert event["callDirection"] == CALL_DIRECTION_INBOUND


def test_fds_event_missing_call_direction_goes_to_dlq_path():
    raw = json.dumps(
        {
            "sessionId": "call-002",
            "eventType": EVENT_STT_PARTIAL,
            "schemaVersion": "1.0",
        }
    )
    with pytest.raises(EventParseError, match="callDirection"):
        EventMapper.parse_event(raw)


def test_kafka_key_parsing():
    direction, session_id = EventMapper.parse_kafka_key(b"inbound:call-003")
    assert direction == "INBOUND"
    assert session_id == "call-003"


def test_fds_event_model_aliases():
    model = FdsEvent.model_validate(
        {
            "sessionId": "s1",
            "callDirection": "OUTBOUND",
            "campaignId": "CAMP01",
            "eventType": EVENT_STT_PARTIAL,
            "schemaVersion": "1.0",
        }
    )
    assert model.session_id == "s1"
    assert model.call_direction == "OUTBOUND"
    assert model.campaign_id == "CAMP01"
