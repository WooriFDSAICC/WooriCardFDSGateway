from __future__ import annotations

from prometheus_client import Counter, Gauge

APPLICATION = "WooriFdsGateway"

fds_kafka_events_processed_total = Counter(
    "fds_kafka_events_processed_total",
    "FDS Kafka events processed by the worker",
    ["event_type", "call_direction", "application"],
)

fds_kafka_dlq_published_total = Counter(
    "fds_kafka_dlq_published_total",
    "FDS events published to DLQ after parse/processing failure",
    ["application"],
)

fds_kafka_scores_published_total = Counter(
    "fds_kafka_scores_published_total",
    "FDS score payloads published to Kafka",
    ["application"],
)

fds_kafka_consumer_lag = Gauge(
    "fds_kafka_consumer_lag",
    "Approximate Kafka consumer lag for FDS events topic",
    ["application", "topic"],
)

triton_up = Gauge(
    "triton_up",
    "Triton inference server readiness (1=ready)",
    ["application"],
)
