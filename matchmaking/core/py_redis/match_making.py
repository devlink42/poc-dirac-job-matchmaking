#!/usr/bin/env python3

from __future__ import annotations

from matchmaking.core.match_making import valid_job_specs_with_node
from matchmaking.models.job import Job
from matchmaking.models.node import Node

# Redis hash key constants — single source of truth for all py_redis modules.
JOBS_KEY = "py_redis:jobs"
NODES_KEY = "py_redis:nodes"


def filter_compatible_jobs(node: Node, candidates: list[Job]) -> list[Job]:
    """Filter candidate jobs to those whose requirements are satisfied by a node.

    Iterates over each job's ``matching_specs`` in order and includes the job
    as soon as one spec is satisfied by the node — matching the semantics of
    the pure-Python implementation in :mod:`matchmaking.core.match_making`.

    Args:
        node: The node (pilot) that is requesting work.
        candidates: Deserialized job objects to evaluate against the node.

    Returns:
        A list of jobs whose constraints are met by ``node``. Preserves the
        original ordering of ``candidates``.
    """
    compatible: list[Job] = []

    for job in candidates:
        for i, spec in enumerate(job.matching_specs):
            if valid_job_specs_with_node(f"{job.job_id}-{i}", spec, node):
                compatible.append(job)
                break  # First matching spec is sufficient — skip the rest.

    return compatible
