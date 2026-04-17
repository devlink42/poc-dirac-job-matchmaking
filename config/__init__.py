from __future__ import annotations

import logging
import sys

logger = logging.getLogger(__name__)


def configure_logger(log_level: str = "WARNING") -> None:
    """Configure CLI logging output and level."""
    level = getattr(logging, log_level.upper(), logging.WARNING)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(filename)s in %(funcName)s on line %(lineno)d] %(message)s",
        stream=sys.stdout,
        force=True,
    )
    logger.setLevel(level)
