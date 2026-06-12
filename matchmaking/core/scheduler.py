#!/usr/bin/env python3

from __future__ import annotations

import random
from collections import Counter
from pathlib import Path

from matchmaking.config.logger import logger
from matchmaking.core.match_making import match
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
    jobs = get_jobs()

    # Match-making: Filter jobs that are compatible with the node's resources
    # and requirements, only in WAITING status jobs.
    waiting_jobs = [job for job in jobs if job.status == JobStatus.WAITING and match(job, node) is not None]
    running_jobs = [job for job in jobs if job.status == JobStatus.RUNNING]

    if not waiting_jobs:
        return None

    config = get_selection_configuration()

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

    # Filtering: Filter by job type priority
    selected_job_type = None

    # Initialize a local random number generator to ensure determinism in the selection process.
    rng = random.Random(42)  # noqa: S311

    for priority_entry in config.job_type_priorities:
        if isinstance(priority_entry, dict):
            # Weighted random selection between JobTypes in this priority level
            relevant_types = {
                jt: weight for jt, weight in priority_entry.items() if any(job.job_type == jt for job in allowed_jobs)
            }
            if not relevant_types:
                continue

            total_weight = sum(relevant_types.values())
            rand_val = rng.uniform(0, total_weight)  # noqa: S311
            cumulative_weight = 0

            # Select a job type based on the weighted random selection algorithm using the cumulative weight.
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

    # If no job type from priorities is found in allowed_jobs, we might want to fallback.
    if not selected_job_type:
        # Fallback for jobs whose type is not in the priority list.
        # If we didn't find a selected_job_type, it means none of the allowed_jobs types
        # are in the priority list.
        # In that case, we can still pick from allowed_jobs.
        candidates = allowed_jobs
    else:
        candidates = [job for job in allowed_jobs if job.job_type == selected_job_type]

    # Ranking: Round-robin style sharing for job owner and job group.
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
        fifo_timestamp = job.submit_time.timestamp()

        return job_group_running_count, owner_running_count, fifo_timestamp

    candidates.sort(key=sorting_key)

    return candidates[0]


def get_jobs() -> list[Job]:
    """Load job examples from the specified path.

    Returns:
        list[Job]: List of job examples.
    """
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

    return jobs


def get_selection_configuration() -> SchedulingConfig:
    """Load default scheduling config from the specified path.

    Returns:
        SchedulingConfig: Default scheduling config.
    """
    try:
        config = SchedulingConfig.load_from_yaml(CONFIG_PATH)
    except FileNotFoundError as e:
        raise ValueError(f"Default scheduling config not found at: '{CONFIG_PATH}'") from e
    except Exception as e:
        raise ValueError(f"Failed to load default scheduling config: {e}") from e
    else:
        logger.info(f"Loaded default scheduling config from: '{CONFIG_PATH}'")

    return config
