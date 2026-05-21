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

from matchmaking.core.match_making import filter_compatible_jobs as base_filter_compatible_jobs
from matchmaking.core.scheduler import select_job as base_select_job
from matchmaking.models.config import SchedulingConfig
from matchmaking.models.job import Job
from matchmaking.models.node import Node


class MatchMode(StrEnum):
    """Available matchmaking implementation strategies."""

    PYTHON = "python"
    PYTHON_REDIS = "python_redis"


def select_job(
    node: Node,
    candidates: list[Job],
    config: SchedulingConfig | None = None,
    mode: MatchMode = MatchMode.PYTHON,
) -> Job | None:
    """Filter candidates according to the MatchMode and apply scheduling policy.

    Args:
        node: The node on which the job will be executed.
        candidates: List of job candidates.
        config: Scheduling configuration parameters.
        mode: The matchmaking implementation strategy to use based on MatchMode.

    Returns:
        The selected job or None if no suitable job is found.
    """
    if mode in (MatchMode.PYTHON, MatchMode.PYTHON_REDIS):
        compatible = base_filter_compatible_jobs(node, candidates)
    else:
        raise ValueError(f"Unknown match mode: {mode}")

    if not compatible:
        return None

    return base_select_job(node, compatible, config)
