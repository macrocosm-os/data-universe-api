from __future__ import annotations
import logging
import sys

import structlog

def _shared_processors(dev: bool):
    return [
        structlog.contextvars.merge_contextvars,      
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ] + (
        [structlog.dev.ConsoleRenderer()] if dev else [structlog.processors.JSONRenderer()]
    )

class _InterceptHandler(logging.Handler):
    """Route stdlib logs (including uvicorn) into structlog."""
    def emit(self, record: logging.LogRecord) -> None:
        try:
            logger = structlog.get_logger(record.name)
            level = record.levelno
            method = {
                logging.CRITICAL: "critical",
                logging.ERROR: "error",
                logging.WARNING: "warning",
                logging.INFO: "info",
                logging.DEBUG: "debug",
            }.get(level, "info")
            logger.bind(logger_name=record.name).log(method, record.getMessage())
        except Exception:
            sys.stderr.write(f"[logging fallback] {record.levelname} {record.name}: {record.getMessage()}\n")

def configure_logging(dev: bool = False) -> None:
    """Call once at app startup."""
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
    root.setLevel(logging.INFO)
    root.addHandler(_InterceptHandler())

    # Keep uvicorn access log INFO noise at bay; structlog will handle access in middleware
    logging.getLogger("uvicorn.access").handlers = [ _InterceptHandler() ]
    logging.getLogger("uvicorn").handlers = [ _InterceptHandler() ]

    # 2) Configure structlog
    structlog.reset_defaults()
    structlog.configure(
        processors=_shared_processors(dev),
        context_class=dict,                     
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        cache_logger_on_first_use=True,
    )

