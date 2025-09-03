# app/logger.py
import logging
import structlog
from .config import cfg

_LEVELS = {
    "CRITICAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
    "NOTSET": logging.NOTSET,
}

def get_logger():
    min_level = _LEVELS.get(cfg.LOG_LEVEL.upper(), logging.INFO)

    # Optional: also set stdlib root logger to the same level so 3rd-party libs match
    logging.basicConfig(level=min_level)

    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer(),
        ],
        # filter at the wrapper level using stdlib numeric level
        wrapper_class=structlog.make_filtering_bound_logger(min_level),
        logger_factory=structlog.PrintLoggerFactory(),  # print to stdout
    )
    return structlog.get_logger()
