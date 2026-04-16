#!/usr/bin/env python3

from __future__ import annotations

from src.models.config import SchedulingConfig
from src.models.job import Job
from src.models.utils import JobGroup, JobOwner, JobType


def select_job(
    matching_jobs: list[Job],
    target_site: str,
    running_by_site_and_type: dict[str, dict[JobType, int]],
    running_by_owner: dict[JobOwner | str, int],
    running_by_group: dict[JobGroup, int],
    config: SchedulingConfig,
) -> Job | None:
    if not matching_jobs:
        return None

    allowed_jobs = []

    default_limits = config.running_limits.get("default", {})
    site_limits = config.running_limits.get(target_site, {})

    for job in matching_jobs:
        site_running_dict = running_by_site_and_type.get(target_site, {})
        current_running = site_running_dict.get(job.job_type, site_running_dict.get(job.job_type, 0))

        limit = site_limits.get(
            job.job_type,
            site_limits.get(
                str(job.job_type), default_limits.get(job.job_type, default_limits.get(str(job.job_type), float("inf")))
            ),
        )

        if current_running < limit:
            allowed_jobs.append(job)

    if not allowed_jobs:
        return None

    def sorting_key(job: Job):
        try:
            type_priority = config.job_type_priorities.index(job.job_type)
        except ValueError:
            try:
                priorities_str = [str(p) for p in config.job_type_priorities]
                type_priority = priorities_str.index(str(job.job_type))
            except ValueError:
                type_priority = float("inf")

        group_running_count = running_by_group.get(job.group, running_by_group.get(job.group, 0))
        owner_running_count = running_by_owner.get(job.owner, running_by_owner.get(job.owner, 0))

        fifo_timestamp = job.submission_time.timestamp() if job.submission_time else float("inf")

        return type_priority, group_running_count, owner_running_count, fifo_timestamp

    allowed_jobs.sort(key=sorting_key)

    return allowed_jobs[0]
