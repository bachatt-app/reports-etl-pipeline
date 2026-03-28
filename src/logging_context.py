"""
Async-safe request-id propagation via contextvars.

Usage::

    from src.logging_context import bind_request_id, new_request_id

    rid = new_request_id()
    token = bind_request_id(rid)
    try:
        ...  # all logging inside here includes [req_id=<rid>]
    finally:
        reset_request_id(token)

The custom logging filter injects ``request_id`` into every log record so it
appears in the format string ``%(request_id)s``.
"""

import logging
import uuid
from contextvars import ContextVar, Token

_request_id_var: ContextVar[str] = ContextVar("request_id", default="-")


def new_request_id() -> str:
    return uuid.uuid4().hex[:12]


def bind_request_id(rid: str) -> Token:
    return _request_id_var.set(rid)


def reset_request_id(token: Token) -> None:
    _request_id_var.reset(token)


def get_request_id() -> str:
    return _request_id_var.get("-")


class RequestIdFilter(logging.Filter):
    """Injects the current context's request_id into every log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = _request_id_var.get("-")
        return True


def configure_logging(level: int = logging.INFO) -> None:
    """Call once at startup to attach the filter and set the format."""
    root = logging.getLogger()
    root.setLevel(level)

    if not root.handlers:
        handler = logging.StreamHandler()
        root.addHandler(handler)

    fmt = "%(asctime)s [%(levelname)s] [req_id=%(request_id)s] %(name)s — %(message)s"
    formatter = logging.Formatter(fmt, datefmt="%Y-%m-%dT%H:%M:%S")

    for handler in root.handlers:
        handler.setFormatter(formatter)
        handler.addFilter(RequestIdFilter())
