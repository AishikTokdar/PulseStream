from fastapi import FastAPI
from prometheus_client import make_asgi_app

from app.core.config import settings


def setup_observability(app: FastAPI) -> None:
    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.instrumentation.celery import CeleryInstrumentor
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        from opentelemetry.instrumentation.redis import RedisInstrumentor
        from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
        from opentelemetry.sdk.resources import SERVICE_NAME, Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        resource = Resource(attributes={SERVICE_NAME: settings.OTEL_SERVICE_NAME})
        provider = TracerProvider(resource=resource)

        processor = BatchSpanProcessor(
            OTLPSpanExporter(endpoint=settings.OTEL_EXPORTER_OTLP_ENDPOINT)
        )
        provider.add_span_processor(processor)
        trace.set_tracer_provider(provider)

        # Instrument FastAPI
        FastAPIInstrumentor.instrument_app(app, tracer_provider=provider)

        # Instrument Redis
        RedisInstrumentor().instrument(tracer_provider=provider)

        # Instrument SQLAlchemy
        SQLAlchemyInstrumentor().instrument(tracer_provider=provider)

        # Instrument Celery
        CeleryInstrumentor().instrument(tracer_provider=provider)

    except Exception as e:
        print(f"OpenTelemetry initialization warning: {e}")

    # Expose Prometheus metrics endpoint on FastAPI app
    try:
        metrics_app = make_asgi_app()
        app.mount("/metrics", metrics_app)
    except Exception as e:
        print(f"Prometheus metrics endpoint warning: {e}")

