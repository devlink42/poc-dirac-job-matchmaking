#!/usr/bin/env python3

from __future__ import annotations

from collections import Counter

from matchmaking.models.job import Job


def _calculate_score(job: Job, running_by_job_group: Counter, running_by_job_owner: Counter) -> tuple:
    """Calculate the sorting score for a single job based on fairshare and FIFO.

    Args:
        job (Job): The job to evaluate.
        running_by_job_group (Counter): Running jobs count grouped by job group.
        running_by_job_owner (Counter): Running jobs count grouped by job owner.

    Returns:
        tuple: A score tuple used for sorting.
    """
    return running_by_job_group.get(job.group, 0), running_by_job_owner.get(job.owner, 0), job.submit_time


def rank(candidates: list[Job], running_by_job_group: Counter, running_by_job_owner: Counter) -> None:
    """Rank the candidate jobs and return the best match.

    Args:
        candidates (list[Job]): The list of filtered candidate jobs.
        running_by_job_group (Counter): Running jobs count grouped by job group.
        running_by_job_owner (Counter): Running jobs count grouped by job owner.
    """
    # Sort the candidates in-place using the private scoring function
    candidates.sort(key=lambda job: _calculate_score(job, running_by_job_group, running_by_job_owner))
