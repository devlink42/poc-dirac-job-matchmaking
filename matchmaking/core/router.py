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

from collections.abc import Callable
from enum import Enum

from matchmaking.core.match_making import filter_compatible_jobs as filter_jobs_python
from matchmaking.core.py_redis.match_making import filter_compatible_jobs as filter_jobs_redis
from matchmaking.core.scheduler import select_job
from matchmaking.models.config import SchedulingConfig
from matchmaking.models.job import Job
from matchmaking.models.node import Node


class MatchMode(str, Enum):
    """Available matchmaking implementation strategies.

    The string values double as the CLI/benchmark argument tokens, so
    ``MatchMode("python")`` resolves a user-supplied flag to its member.
    """

    PYTHON = "python"
    PYTHON_REDIS = "python_redis"


# Registry mapping each mode to its candidate-filtering strategy. The shared
# scheduling policy is applied separately by ``select_job_for_node``.
_FILTER_STRATEGIES: dict[MatchMode, Callable[[Node, list[Job]], list[Job]]] = {
    MatchMode.PYTHON: filter_jobs_python,
    MatchMode.PYTHON_REDIS: filter_jobs_redis,
}


def get_filter_strategy(mode: MatchMode) -> Callable[[Node, list[Job]], list[Job]]:
    """Return the candidate-filtering function registered for ``mode``.

    Args:
        mode (MatchMode): The matchmaking implementation to route to.

    Returns:
        Callable[[Node, list[Job]], list[Job]]: The filtering function for the
        requested mode.

    Raises:
        ValueError: If no strategy is registered for ``mode``.
    """
    try:
        return _FILTER_STRATEGIES[mode]
    except KeyError as exc:
        raise ValueError(f"Unsupported matchmaking mode: {mode}") from exc


def select_job_for_node(
    mode: MatchMode,
    node: Node,
    candidates: list[Job],
    config: SchedulingConfig | None = None,
) -> Job | None:
    """Filter candidates with the mode's strategy, then apply scheduling policy.

    Args:
        mode (MatchMode): The matchmaking implementation to route to.
        node (Node): The node (pilot) requesting work.
        candidates (list[Job]): Pre-fetched job candidates to evaluate.
        config (SchedulingConfig | None): Scheduling policy configuration. When
            ``None``, the default config is loaded by
            :func:`matchmaking.core.scheduler.select_job`.

    Returns:
        Job | None: The selected job, or ``None`` when no compatible candidate
        is found.
    """
    compatible = get_filter_strategy(mode)(node, candidates)
    if not compatible:
        return None

    return select_job(node, compatible, config)
