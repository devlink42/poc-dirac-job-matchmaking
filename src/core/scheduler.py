#!/usr/bin/env python3

from __future__ import annotations

from src.models.config import SchedulingConfig
from src.models.job import Job


def select_job(
    matching_jobs: list[Job],
    target_site: str,
    running_by_site_and_type: dict[str, dict[str, list[Job]]],
    running_by_owner: dict[str, list[Job]],
    config: SchedulingConfig,
) -> Job | None:
    if not matching_jobs:
        return None

    allowed_jobs = []

    site_limits = config.running_limits.get(target_site, config.running_limits.get("default", {}))

    for job in matching_jobs:
        current_running = running_by_site_and_type.get(target_site, {}).get(job.job_info.job_type, 0)
        limit = site_limits.get(job.job_info.job_type, float("inf"))

        if current_running < limit:
            allowed_jobs.append(job)

    if not allowed_jobs:
        return None

    def sorting_key(job: Job):
        try:
            type_priority = config.job_type_priorities.index(job.job_info.job_type)
        except ValueError:
            type_priority = 999

        owner_running_count = running_by_owner.get(job.job_info.owner, 0)

        fifo_timestamp = job.job_info.submit_time.timestamp() if job.job_info.submit_time else float("inf")

        return type_priority, owner_running_count, fifo_timestamp

    allowed_jobs.sort(key=sorting_key)

    return allowed_jobs[0]
