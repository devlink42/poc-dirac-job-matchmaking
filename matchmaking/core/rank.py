#!/usr/bin/env python3

from __future__ import annotations

from matchmaking.models.job import Job


def rank(
    job: Job, running_by_job_group: dict[str, int], running_by_job_owner: dict[str, int]
) -> tuple[int, int, float]:
    """Sort by job group running count, then owner running count, then FIFO timestamp.

    Args:
        job (Job): The job to generate the sorting key for.
        running_by_job_group (dict[str, int]): Running count by job group.
        running_by_job_owner (dict[str, int]): Running count by job owner.

    Returns:
        tuple[int, int, float]: Sorting key tuple for job comparison.
    """
    job_group_running_count = running_by_job_group[job.group]
    owner_running_count = running_by_job_owner[job.owner]
    fifo_timestamp = job.submit_time.timestamp()

    return job_group_running_count, owner_running_count, fifo_timestamp
