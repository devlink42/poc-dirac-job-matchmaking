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

import redis

from matchmaking.core.py_redis.scheduler import select_job_from_redis
from matchmaking.core.scheduler import select_job
from matchmaking.models.config import SchedulingConfig
from matchmaking.models.job import Job
from matchmaking.models.node import Node


class MatchMode(StrEnum):
    """Available matchmaking implementation strategies."""

    PYTHON = "python"
    PYTHON_REDIS = "python_redis"


def select_job_router(
    node: Node,
    jobs: list[Job],
    config: SchedulingConfig | None = None,
    mode: MatchMode = MatchMode.PYTHON,
    redis_client: redis.Redis | None = None,
) -> Job | None:
    """Filter candidates according to the MatchMode and apply scheduling policy.

    Args:
        node: The node on which the job will be executed.
        jobs: List of job candidates.
        config: Scheduling configuration parameters.
        mode: The matchmaking implementation strategy to use based on MatchMode.
        redis_client: A connection to Redis for Redis-based matchmaking.

    Returns:
        The selected job or None if no suitable job is found.
    """
    match mode:
        case MatchMode.PYTHON:
            return select_job(node, jobs, config)
        case MatchMode.PYTHON_REDIS:
            try:
                return select_job_from_redis(redis_client, node, len(jobs), config)
            except Exception as e:
                raise ValueError(f"Redis error in matchmaking: {e}") from e
        case _:
            raise ValueError(f"Unsupported MatchMode: {mode}")
