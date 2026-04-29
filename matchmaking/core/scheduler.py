#!/usr/bin/env python3

from __future__ import annotations

from collections import Counter

from matchmaking.config.logger import logger
from matchmaking.models.config import SchedulingConfig
from matchmaking.models.job import Job
from matchmaking.models.node import Node

DEFAULT_CONFIG_PATH = "matchmaking/config/scheduling.yaml"


def select_job(
    node: Node,
    candidate_jobs: list[Job],
    config: SchedulingConfig | None = None,
) -> Job | None:
    """Select a job from the matching jobs based on scheduling criteria.

    Args:
        node (Node): The node on which the job will be executed.
        candidate_jobs (list[Job]): List of jobs that match the scheduling criteria.
        config (SchedulingConfig | None): Scheduling configuration parameters.
        If None, the default config is loaded.

    Returns:
        Job | None: The selected job or None if no suitable job is found.
    """
    if not candidate_jobs or not node:
        return None

    if not config:
        try:
            config = SchedulingConfig.load_from_yaml(DEFAULT_CONFIG_PATH)
        except FileNotFoundError as e:
            raise ValueError(f"Default scheduling config not found at: '{DEFAULT_CONFIG_PATH}'") from e
        except Exception as e:
            raise ValueError(f"Failed to load default scheduling config: {e}") from e
        else:
            logger.info(f"Loaded default scheduling config from: '{DEFAULT_CONFIG_PATH}'")

    default_limits = config.running_limits.get("default", {})
    site_limits = config.running_limits.get(node.site, {})

    # Calculate counts using Counter for efficiency
    running_by_owner = Counter(job.owner for job in candidate_jobs)
    running_by_group = Counter(job.group for job in candidate_jobs)
    type_counts = Counter(job.job_type for job in candidate_jobs)

    allowed_jobs = [
        job
        for job in candidate_jobs
        if type_counts[job.job_type] < site_limits.get(job.job_type, default_limits.get(job.job_type, float("inf")))
    ]

    if not allowed_jobs:
        return None

    # Pre-calculate priority map to avoid repeated .index() calls during sorting
    priority_map = {jtype: i for i, jtype in enumerate(config.job_type_priorities)}

    def sorting_key(job: Job) -> tuple[int | float, int, int, float]:
        """Sort by job type priority, then by group, then by owner, then by FIFO timestamp.

        Args:
            job (Job): The job to generate the sorting key for.

        Returns:
            tuple[int, int, int, float]: Sorting key tuple for job comparison.
        """
        # Job type priority
        type_priority = priority_map.get(job.job_type, float("inf"))

        # Group and owner round-robin / fair share
        group_running_count = running_by_group[job.group]
        owner_running_count = running_by_owner[job.owner]

        # Tie-breaker (FIFO)
        fifo_timestamp = job.submission_time.timestamp() if job.submission_time else float("inf")

        return type_priority, group_running_count, owner_running_count, fifo_timestamp

    allowed_jobs.sort(key=sorting_key)

    return allowed_jobs[0]
