#!/usr/bin/env python3

from __future__ import annotations

from matchmaking.config.logger import logger
from matchmaking.models.config import SchedulingConfig
from matchmaking.models.job import Job
from matchmaking.models.utils import JobStatus

CONFIG_PATH: str = "matchmaking/config/scheduling.yaml"
JOBS: list[Job] = []

_JOBS_CACHE: list[Job] | None = None
_CONFIG_CACHE: SchedulingConfig | None = None


def get_jobs() -> list[Job]:
    """Load job examples from the specified path.

    Returns:
        list[Job]: List of job examples.

    Raises:
        ValueError: If the job examples file is not found or fails to load.
    """
    if not all(isinstance(job, Job) for job in JOBS):
        raise ValueError("All elements in the dynamically injected JOBS list must be Job instances.")

    return JOBS


def get_selection_configuration() -> SchedulingConfig:
    """Load scheduling configuration from the specified path.

    Returns:
        SchedulingConfig: Scheduling configuration.

    Raises:
        ValueError: If the scheduling config file is not found or fails to load.
    """
    global _CONFIG_CACHE

    if _CONFIG_CACHE is not None:
        return _CONFIG_CACHE

    try:
        config = SchedulingConfig.load_from_yaml(CONFIG_PATH)
    except FileNotFoundError as e:
        raise ValueError(f"Scheduling config not found at: '{CONFIG_PATH}'") from e
    except Exception as e:
        raise ValueError(f"Failed to load scheduling config from: '{CONFIG_PATH}': {e}") from e
    else:
        logger.info("Loaded scheduling config from: '%s'", CONFIG_PATH)

    _CONFIG_CACHE = config

    return _CONFIG_CACHE


def assign_job_to_site(job: Job, node_site: str) -> None:
    job.assigned_site = node_site
    job.status = JobStatus.RUNNING

    logger.debug("Assigned job '%s' to site '%s' in memory.", job.job_id, node_site)
