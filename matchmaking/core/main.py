#!/usr/bin/env python3

from __future__ import annotations

import random
from collections import Counter

from matchmaking.core.filter import filter
from matchmaking.core.match import is_matching
from matchmaking.core.rank import rank
from matchmaking.core.utils import assign_job_to_site, get_jobs, get_selection_configuration
from matchmaking.models.job import Job
from matchmaking.models.node import Node
from matchmaking.models.utils import JobStatus


def select_job(node: Node, rng: random.Random | None = None) -> Job | None:
    """Select a job from the matching jobs based on scheduling criteria.

    Args:
        node (Node): The node on which the job will be executed.
        rng (random.Random | None, optional): The random number generator to use for selection. Defaults to None.

    Returns:
        Job | None: The selected job or None if no suitable job is found.
    """
    jobs = get_jobs()

    # Match-making: Filter jobs that are compatible with the node's resources
    # and requirements, only in WAITING status jobs.
    waiting_matching_jobs = [job for job in jobs if job.status == JobStatus.WAITING and is_matching(job, node)]
    running_jobs = [job for job in jobs if job.status == JobStatus.RUNNING]

    if not waiting_matching_jobs:
        raise ValueError("No matching jobs found")

    config = get_selection_configuration()

    site_config = config.by_site.get(node.site)
    site_limits = site_config.running_limits if site_config else {}

    running_jobs_at_site = [job for job in running_jobs if job.assigned_site == node.site]

    running_job_type_counts = Counter(job.type for job in running_jobs_at_site)
    running_by_job_group = Counter(job.group for job in running_jobs)
    running_by_job_owner = Counter(job.owner for job in running_jobs)

    # Filtering: Filter by job type priority
    try:
        candidates = filter(waiting_matching_jobs, running_job_type_counts, site_limits, config, rng)
    except ValueError as e:
        raise ValueError(f"Error filtering candidates: {e}") from e

    # Ranking: Round-robin style sharing for job owner and job group.
    # We sort the candidates by running counts of group and owner, then FIFO.
    rank(candidates, running_by_job_group, running_by_job_owner)

    assign_job_to_site(candidates[0], node.site)

    return candidates[0]
