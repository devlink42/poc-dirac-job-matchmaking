#!/usr/bin/env python3

from __future__ import annotations

from matchmaking.models.config import SchedulingConfig
from matchmaking.models.job import Job
from matchmaking.models.node import Node


def select_job(
    node: Node,
    candidate_jobs: list[Job],
    config: SchedulingConfig,
) -> Job | None:
    """Select a job from the matching jobs based on scheduling criteria.

    Args:
        node (Node): The node on which the job will be executed.
        candidate_jobs (list[Job]): List of jobs that match the scheduling criteria.
        config (SchedulingConfig): Scheduling configuration parameters.

    Returns:
        Job | None: The selected job or None if no suitable job is found.
    """
    if not candidate_jobs or not node:
        return None

    allowed_jobs = []

    running_by_owner = {}
    running_by_group = {}
    running_type_by_site = {}
    default_limits = config.running_limits.get("default", {})
    site_limits = config.running_limits.get(node.site, {})

    for job in candidate_jobs:
        running_by_owner[job.owner] = running_by_owner.get(job.owner, 0) + 1
        running_by_group[job.group] = running_by_group.get(job.group, 0) + 1
        running_type_by_site[(node.site, job.job_type)] = running_type_by_site.get((node.site, job.job_type), 0) + 1

    for job in candidate_jobs:
        current_running = running_type_by_site.get((node.site, job.job_type), 0)
        limit = site_limits.get(job.job_type, default_limits.get(job.job_type, float("inf")))

        if current_running < limit:
            allowed_jobs.append(job)

    if not allowed_jobs:
        return None

    def sorting_key(job: Job) -> tuple[int | float, int, int, float]:
        """Sort by job type priority, then by group, then by owner, then by FIFO timestamp.

        Args:
            job (Job): The job to generate the sorting key for.

        Returns:
            tuple[int, int, int, float]: Sorting key tuple for job comparison.
        """
        # Job type priority
        try:
            type_priority = config.job_type_priorities.index(job.job_type)
        except ValueError:
            type_priority = float("inf")

        # Group and owner round-robin / fair share
        group_running_count = running_by_group.get(job.group, 0)
        owner_running_count = running_by_owner.get(job.owner, 0)

        # Tie-breaker (FIFO)
        fifo_timestamp = job.submission_time.timestamp() if job.submission_time else float("inf")

        return type_priority, group_running_count, owner_running_count, fifo_timestamp

    allowed_jobs.sort(key=sorting_key)

    return allowed_jobs[0]
