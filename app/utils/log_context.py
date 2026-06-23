"""sessionId 기반 로그 correlation."""
from __future__ import annotations

import contextvars
import logging

session_id_ctx: contextvars.ContextVar[str] = contextvars.ContextVar("session_id", default="-")
call_direction_ctx: contextvars.ContextVar[str] = contextvars.ContextVar("call_direction", default="-")


class LogContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.session_id = session_id_ctx.get()
        record.call_direction = call_direction_ctx.get()
        return True


def bind_event_context(session_id: str, call_direction: str | None = None) -> None:
    session_id_ctx.set(session_id or "-")
    call_direction_ctx.set((call_direction or "-").upper())


def clear_event_context() -> None:
    session_id_ctx.set("-")
    call_direction_ctx.set("-")


def configure_logging() -> None:
    root = logging.getLogger()
    log_format = (
        "%(asctime)s [%(levelname)s] [session=%(session_id)s direction=%(call_direction)s] "
        "%(name)s - %(message)s"
    )
    for handler in root.handlers:
        handler.addFilter(LogContextFilter())
        handler.setFormatter(logging.Formatter(log_format))
