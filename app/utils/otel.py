"""OpenTelemetry — Jaeger OTLP export (OTEL_ENABLED=true 시 활성)."""
from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)


def instrument_fastapi(app) -> None:
    if os.getenv("OTEL_ENABLED", "false").lower() != "true":
        return

    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318/v1/traces")
    service_name = os.getenv("OTEL_SERVICE_NAME", "WooriFdsGateway")
    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)))
    trace.set_tracer_provider(provider)
    FastAPIInstrumentor.instrument_app(app)
    logger.info("[OTel] Tracing enabled service=%s endpoint=%s", service_name, endpoint)
