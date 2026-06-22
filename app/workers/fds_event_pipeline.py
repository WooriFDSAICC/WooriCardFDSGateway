from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Dict, Optional

import httpx
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from app.config import settings
from app.constants.integration_contract import CALL_DIRECTION_INBOUND, EVENT_AGENT_ESCALATION, SCHEMA_VERSION
from app.constants.kafka_constants import KafkaConstants
from app.metrics import (
    APPLICATION,
    fds_kafka_consumer_lag,
    fds_kafka_dlq_published_total,
    fds_kafka_events_processed_total,
    fds_kafka_scores_published_total,
)
from app.models.fds_models import FdsScoreRequest
from app.services.scoring.fds_scoring_pipeline import FdsScoringPipeline
from app.utils.log_context import bind_event_context, clear_event_context
from app.workers.event_mapper import EventMapper, EventParseError
from app.workers.feature_store_sync import FeatureStoreSyncService
from app.workers.rule_evaluator import RuleEvaluator

logger = logging.getLogger(__name__)


class FdsEventPipelineWorker:
    """
    Kafka FDS 이벤트 통합 파이프라인 (Integration Worker + Rule Engine).

    wooricard-fds-events → Feature Store → in-process FDS 스코어링
    → wooricard-fds-scores → Rule 평가 → wooricard-fds-actions
    """

    def __init__(self, fds_pipeline: FdsScoringPipeline) -> None:
        self._fds_pipeline = fds_pipeline
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._producer: Optional[AIOKafkaProducer] = None
        self._feature_store = FeatureStoreSyncService()
        self._rule_evaluator = RuleEvaluator()
        self._http: Optional[httpx.AsyncClient] = None
        self._stt_counters: Dict[str, int] = {}
        self._running = False
        self._lag_task: asyncio.Task | None = None

    async def startup(self) -> None:
        await self._feature_store.startup()

        self._consumer = AIOKafkaConsumer(
            settings.kafka_events_topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.kafka_consumer_group,
            enable_auto_commit=True,
            auto_offset_reset="latest",
            value_deserializer=lambda v: v.decode("utf-8"),
        )
        self._producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        )
        self._http = httpx.AsyncClient(timeout=5.0)

        await self._consumer.start()
        await self._producer.start()
        self._running = True
        self._lag_task = asyncio.create_task(self._report_lag_loop())

        logger.info(
            "[FdsEventPipeline] Started events=%s scores=%s actions=%s dlq=%s group=%s",
            settings.kafka_events_topic,
            settings.kafka_scores_topic,
            settings.kafka_actions_topic,
            settings.kafka_dlq_topic if settings.kafka_dlq_enabled else "disabled",
            settings.kafka_consumer_group,
        )

    async def shutdown(self) -> None:
        self._running = False
        if self._lag_task:
            self._lag_task.cancel()
            try:
                await self._lag_task
            except asyncio.CancelledError:
                pass
            self._lag_task = None
        if self._consumer:
            await self._consumer.stop()
            self._consumer = None
        if self._producer:
            await self._producer.stop()
            self._producer = None
        if self._http:
            await self._http.aclose()
            self._http = None
        await self._feature_store.shutdown()

    async def run_forever(self) -> None:
        if self._consumer is None:
            raise RuntimeError("FdsEventPipelineWorker not started")

        async for msg in self._consumer:
            if not self._running:
                break
            try:
                await self._process_message(msg.value, msg.key)
            except EventParseError as exc:
                logger.warning("[FdsEventPipeline] Parse error: %s", exc)
                await self._publish_dlq(msg.value, str(exc))
            except Exception as exc:
                logger.exception("[FdsEventPipeline] Message processing failed")
                await self._publish_dlq(msg.value, str(exc))

    async def _process_message(self, raw: str, key: bytes | None) -> None:
        event = EventMapper.parse_event(raw)
        session_id = event.get("sessionId") or "unknown"
        bind_event_context(session_id, event.get("callDirection"))
        try:
            await self._handle_parsed_event(event, key, raw)
        finally:
            clear_event_context()

    async def _handle_parsed_event(self, event: Dict[str, Any], key: bytes | None, raw: str) -> None:
        key_direction, key_session_id = EventMapper.parse_kafka_key(key)
        session_id = event.get("sessionId") or key_session_id or "unknown"

        if key_direction and event.get("callDirection"):
            body_direction = (event.get("callDirection") or "").upper()
            if key_direction != body_direction:
                logger.warning(
                    "[FdsEventPipeline] Kafka key/body direction mismatch key=%s body=%s session=%s",
                    key_direction,
                    body_direction,
                    session_id,
                )

        event_type = (event.get("eventType") or "").upper()
        self._record_event(event)

        if event_type == KafkaConstants.EVENT_SESSION_ENDED:
            entity_key = EventMapper.entity_key(event)
            if entity_key:
                await self._feature_store.finalize_session(entity_key, event)
                self._stt_counters.pop(entity_key, None)
            logger.info("[FdsEventPipeline] Session ended session=%s entity=%s", session_id, entity_key)
            return

        if event_type == EVENT_AGENT_ESCALATION:
            logger.warning(
                "[FdsEventPipeline] Agent escalation session=%s direction=%s campaign=%s",
                session_id,
                event.get("callDirection"),
                event.get("campaignId"),
            )

        entity_key = await self._feature_store.sync_from_event(event)
        if not entity_key:
            return

        if event_type == KafkaConstants.EVENT_STT_PARTIAL:
            self._stt_counters[entity_key] = self._stt_counters.get(entity_key, 0) + 1

        stt_count = self._stt_counters.get(entity_key, 0)
        if not EventMapper.should_trigger_scoring(event, stt_count, settings.score_on_stt_every_n):
            return

        context = await self._feature_store.get_context_features(entity_key)
        score_request_body = EventMapper.to_scoring_request(event, context)
        score_result = await self._score_in_process(score_request_body)

        score_payload = {
            "schemaVersion": SCHEMA_VERSION,
            "sessionId": session_id,
            "callDirection": event.get("callDirection"),
            "campaignId": event.get("campaignId"),
            "entityKey": entity_key,
            "sourceEventType": event.get("eventType"),
            "scoringResult": score_result,
        }
        await self._publish(settings.kafka_scores_topic, session_id, score_payload)

        action_result = self._rule_evaluator.evaluate(score_payload)
        await self._publish(settings.kafka_actions_topic, session_id, action_result)

        if settings.alert_webhook_enabled and action_result["action"] in ("BLOCK", "ALERT"):
            await self._send_alert_webhook(action_result)

    async def _score_in_process(self, request_body: Dict[str, Any]) -> Dict[str, Any]:
        request = FdsScoreRequest.model_validate(request_body)
        response = await self._fds_pipeline.score(request)
        result = response.model_dump()
        logger.info(
            "[FdsEventPipeline] In-process score session=%s score=%s flag=%s",
            request_body.get("session_id"),
            result.get("fds_score"),
            result.get("fds_flag"),
        )
        return result

    async def _publish(self, topic: str, session_id: str, payload: Dict[str, Any]) -> None:
        if self._producer is None:
            return
        await self._producer.send_and_wait(
            topic,
            key=session_id.encode("utf-8"),
            value=payload,
        )
        if topic == settings.kafka_scores_topic:
            fds_kafka_scores_published_total.labels(application=APPLICATION).inc()
        logger.info("[FdsEventPipeline] Published topic=%s session=%s", topic, session_id)

    async def _publish_dlq(self, raw: str, error: str) -> None:
        if not settings.kafka_dlq_enabled or self._producer is None:
            return
        try:
            original = json.loads(raw)
            session_id = original.get("sessionId", "unknown")
        except Exception:
            session_id = "unknown"
            original = {"raw": raw}

        payload = {
            "schemaVersion": SCHEMA_VERSION,
            "sessionId": session_id,
            "error": error,
            "originalEvent": original,
        }
        await self._producer.send_and_wait(
            settings.kafka_dlq_topic,
            key=session_id.encode("utf-8"),
            value=payload,
        )
        fds_kafka_dlq_published_total.labels(application=APPLICATION).inc()
        logger.warning("[FdsEventPipeline] Published DLQ session=%s error=%s", session_id, error)

    async def _report_lag_loop(self) -> None:
        while self._running:
            try:
                await self._update_consumer_lag()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("[FdsEventPipeline] Lag report failed: %s", exc)
            await asyncio.sleep(15)

    async def _update_consumer_lag(self) -> None:
        if self._consumer is None:
            return
        partitions = self._consumer.assignment()
        if not partitions:
            return
        end_offsets = await self._consumer.end_offsets(partitions)
        total_lag = 0
        for partition in partitions:
            position = await self._consumer.position(partition)
            end = end_offsets.get(partition, 0)
            total_lag += max(0, end - position)
        fds_kafka_consumer_lag.labels(
            application=APPLICATION,
            topic=settings.kafka_events_topic,
        ).set(total_lag)

    @staticmethod
    def _record_event(event: Dict[str, Any]) -> None:
        event_type = (event.get("eventType") or "UNKNOWN").upper()
        direction = (event.get("callDirection") or CALL_DIRECTION_INBOUND).upper()
        fds_kafka_events_processed_total.labels(
            event_type=event_type,
            call_direction=direction,
            application=APPLICATION,
        ).inc()

    async def _send_alert_webhook(self, action_result: Dict[str, Any]) -> None:
        if not self._http or not settings.alert_webhook_url:
            return
        try:
            await self._http.post(settings.alert_webhook_url, json=action_result)
        except Exception as exc:
            logger.warning("[FdsEventPipeline] Alert webhook failed: %s", exc)
