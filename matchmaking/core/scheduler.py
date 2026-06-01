#!/usr/bin/env python3

from __future__ import annotations

from collections import Counter
from pathlib import Path

from matchmaking.config.logger import logger
from matchmaking.models.config import SchedulingConfig
from matchmaking.models.job import Job
from matchmaking.models.node import Node

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
        candidate_jobs = []

        for job in Path(JOB_PATH).glob("*.yaml"):
            if job.stem.startswith("invalid"):
                continue

            # We assume that all jobs files are in the running part
            candidate_jobs.append(Job.load_from_yaml(job))
    except FileNotFoundError as e:
        raise ValueError(f"Job examples not found at: '{JOB_PATH}'") from e
    except Exception as e:
        raise ValueError(f"Failed to load job examples: {e}") from e
    else:
        logger.info(f"Loaded job examples from: '{JOB_PATH}'")

    try:
        config = SchedulingConfig.load_from_yaml(CONFIG_PATH)
    except FileNotFoundError as e:
        raise ValueError(f"Default scheduling config not found at: '{CONFIG_PATH}'") from e
    except Exception as e:
        raise ValueError(f"Failed to load default scheduling config: {e}") from e
    else:
        logger.info(f"Loaded default scheduling config from: '{CONFIG_PATH}'")

    site_limits = config.running_limits.get(node.site, {})

    # Calculate counts using Counter for efficiency
    running_job_type_counts = Counter(job.type for job in candidate_jobs)
    running_by_owner_group = Counter(job.owner_group for job in candidate_jobs)
    running_by_job_group = Counter(job.job_group for job in candidate_jobs)
    running_by_owner = Counter(job.owner for job in candidate_jobs)

    allowed_jobs = [
        job for job in candidate_jobs if running_job_type_counts[job.type] < site_limits.get(job.type, float("inf"))
    ]

    if not allowed_jobs:
        return None

    # Pre-calculate priority map to avoid repeated .index() calls during sorting
    priority_map = {jtype: i for i, jtype in enumerate(config.job_type_priorities)}

    def sorting_key(job: Job) -> tuple[int | float, int, int, int, float]:
        """Sort by job type priority, then by owner group, then by job group, then by owner, then by FIFO timestamp.

        Args:
            job (Job): The job to generate the sorting key for.

        Returns:
            tuple[int | float, int, int, int, float]: Sorting key tuple for job comparison.
        """
        # Job type priority
        type_priority = priority_map.get(job.type, float("inf"))

        # Group and owner round-robin / fair share
        owner_group_running_count = running_by_owner_group[job.owner_group]
        job_group_running_count = running_by_job_group[job.job_group]
        owner_running_count = running_by_owner[job.owner]

        # Tie-breaker (FIFO)
        fifo_timestamp = job.submission_time.timestamp() if job.submission_time else float("inf")

        return type_priority, owner_group_running_count, job_group_running_count, owner_running_count, fifo_timestamp

    allowed_jobs.sort(key=sorting_key)

    return allowed_jobs[0]
