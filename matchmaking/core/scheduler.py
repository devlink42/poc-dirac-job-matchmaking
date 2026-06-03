#!/usr/bin/env python3

from __future__ import annotations

import random
from collections import Counter
from pathlib import Path

from matchmaking.config.logger import logger
from matchmaking.models.config import SchedulingConfig
from matchmaking.models.job import Job
from matchmaking.models.node import Node
from matchmaking.models.utils import JobStatus

CONFIG_PATH = "matchmaking/config/scheduling.yaml"
JOB_PATH = "tests/examples/jobs/"


def select_job(
    node: Node,
) -> Job | None:
    """Select a job from the matching jobs based on scheduling criteria.

    Args:
        node (Node): The node on which the job will be executed.

    Returns:
        Job | None: The selected job or None if no suitable job is found.
    """
    if not node:
        return None

    try:
        jobs = []

        for job_file in Path(JOB_PATH).glob("*.yaml"):
            if job_file.stem.startswith("invalid"):
                continue

            jobs.append(Job.load_from_yaml(job_file))
    except FileNotFoundError as e:
        raise ValueError(f"Job examples not found at: '{JOB_PATH}'") from e
    except Exception as e:
        raise ValueError(f"Failed to load job examples: {e}") from e
    else:
        logger.info(f"Loaded job examples from: '{JOB_PATH}'")

    running_jobs = [job for job in jobs if job.status == JobStatus.RUNNING]
    waiting_jobs = [job for job in jobs if job.status == JobStatus.WAITING]

    if not waiting_jobs:
        return None

    try:
        config = SchedulingConfig.load_from_yaml(CONFIG_PATH)
    except FileNotFoundError as e:
        raise ValueError(f"Default scheduling config not found at: '{CONFIG_PATH}'") from e
    except Exception as e:
        raise ValueError(f"Failed to load default scheduling config: {e}") from e
    else:
        logger.info(f"Loaded default scheduling config from: '{CONFIG_PATH}'")

    site_config = config.by_site.get(node.site)
    site_limits = site_config.running_limits if site_config else {}

    running_job_type_counts = Counter(job.job_type for job in running_jobs)
    running_by_job_group = Counter(job.group for job in running_jobs)
    running_by_job_owner = Counter(job.owner for job in running_jobs)

    allowed_jobs = [
        job
        for job in waiting_jobs
        if running_job_type_counts[job.job_type] < site_limits.get(job.job_type, float("inf"))
    ]

    if not allowed_jobs:
        return None

    # Step 1: Filter by job type priority
    selected_job_type = None

    for priority_entry in config.job_type_priorities:
        if isinstance(priority_entry, dict):
            # Weighted random selection between JobTypes in this priority level
            relevant_types = {
                jt: weight for jt, weight in priority_entry.items() if any(j.job_type == jt for j in allowed_jobs)
            }
            if not relevant_types:
                continue

            total_weight = sum(relevant_types.values())
            rand_val = random.uniform(0, total_weight)  # noqa: S311
            cumulative_weight = 0

            # Sort types to ensure deterministic behavior for a given random seed
            for jt in sorted(relevant_types.keys()):
                cumulative_weight += relevant_types[jt]
                if rand_val <= cumulative_weight:
                    selected_job_type = jt
                    break
        else:
            # Single JobType priority level
            if any(job.job_type == priority_entry for job in allowed_jobs):
                selected_job_type = priority_entry

        if selected_job_type:
            break

    # If no job type from priorities is found in allowed_jobs, we might want to fallback
    # or just return None if we strictly follow the priorities.
    # The issue says "prendre les jobs qui sont de ce type la un par un, puis si il y en a plus prendre le 2eme..."
    # implying we only consider what's in the list.

    if not selected_job_type:
        # Fallback for jobs whose type is not in the priority list?
        # The current implementation uses float("inf") for them.
        # Let's see if we have any allowed jobs left.
        # If we didn't find a selected_job_type, it means none of the allowed_jobs types
        # are in the priority list.
        # In that case, we can still pick from allowed_jobs.
        candidates = allowed_jobs
    else:
        candidates = [j for j in allowed_jobs if j.job_type == selected_job_type]

    # Step 2: Round-robin style sharing for job owner and job group
    # We sort the candidates by running counts of group and owner, then FIFO.
    def sorting_key(job: Job) -> tuple[int, int, float]:
        """Sort by job group running count, then owner running count, then FIFO timestamp.

        Args:
            job (Job): The job to generate the sorting key for.

        Returns:
            tuple[int, int, float]: Sorting key tuple for job comparison.
        """
        job_group_running_count = running_by_job_group[job.group]
        owner_running_count = running_by_job_owner[job.owner]
        fifo_timestamp = job.submit_time.timestamp() if job.submit_time else float("inf")

        return job_group_running_count, owner_running_count, fifo_timestamp

    candidates.sort(key=sorting_key)

    return candidates[0]
