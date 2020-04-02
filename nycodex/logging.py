import logging
import sys

import structlog

from .config import LOG_LEVEL

logging.basicConfig(
    format="%(module)s:%(lineno)d %(message)s",
    level=getattr(logging, LOG_LEVEL),
    stream=sys.stdout,
)

structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        # TODO(alan): Dev-only. Use JSON for production
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S.%f"),
        structlog.dev.ConsoleRenderer(),
    ],
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)


def get_logger(*args, **kwargs):
    return structlog.get_logger(*args, **kwargs)
