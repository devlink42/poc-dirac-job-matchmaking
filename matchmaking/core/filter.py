#!/usr/bin/env python3

from __future__ import annotations

import random
from random import Random

from matchmaking.models.config import SchedulingConfig
from matchmaking.models.job import Job


def filter(allowed_jobs: list[Job], config: SchedulingConfig, rng: Random | None) -> list[Job]:
    selected_job_type = None

    for priority_entry in config.job_type_priorities:
        if isinstance(priority_entry, dict):
            # Weighted random selection between JobTypes in this priority level
            relevant_types = {
                jt: weight for jt, weight in priority_entry.items() if any(job.type == jt for job in allowed_jobs)
            }
            if not relevant_types:
                continue

            total_weight = sum(relevant_types.values())
            rand_val = (rng if rng else random).uniform(0, total_weight)  # noqa: S311
            cumulative_weight = 0

            # Select a job type based on the weighted random selection algorithm using the cumulative weight.
            for jt in sorted(relevant_types.keys()):
                cumulative_weight += relevant_types[jt]
                if rand_val <= cumulative_weight:
                    selected_job_type = jt
                    break
        else:
            # Single JobType priority level
            if any(job.type == priority_entry for job in allowed_jobs):
                selected_job_type = priority_entry

        if selected_job_type:
            break

    if selected_job_type:
        return [job for job in allowed_jobs if job.type == selected_job_type]

    # If no job type from priorities is found in allowed_jobs, we might want to fallback.
    return allowed_jobs
