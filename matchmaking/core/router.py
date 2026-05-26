#!/usr/bin/env python3
"""Strategy router for matchmaking implementation variants.

Centralizes the dispatch between matchmaking implementations (base Python,
``py_redis``, ...) so entry points such as the schedulers and the Locust
benchmark do not hardcode a single algorithm. Each mode contributes a
candidate-filtering strategy; the shared scheduling policy
(:func:`matchmaking.core.scheduler.select_job`) is then applied uniformly on
top of the filtered candidates.
"""

from __future__ import annotations

from enum import StrEnum


class MatchMode(StrEnum):
    """Available matchmaking implementation strategies."""

    PYTHON = "python"
    PYTHON_REDIS = "python_redis"
