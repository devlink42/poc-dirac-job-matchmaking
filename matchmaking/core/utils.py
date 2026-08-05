#!/usr/bin/env python3

from __future__ import annotations

from matchmaking.config.logger import logger
from matchmaking.models.config import SchedulingConfig
from matchmaking.models.job import Job
from matchmaking.models.utils import JobStatus

CONFIG_PATH: str = "matchmaking/config/scheduling.yaml"
JOBS: list[Job] = []

_CONFIG_CACHE: SchedulingConfig | None = None


def set_jobs(jobs: list[Job]) -> None:
    """Set the list of jobs.

    Args:
        jobs: List of jobs to set.
    """
    global JOBS

    JOBS = jobs

    logger.debug("Set %d jobs in memory.", len(JOBS))


def get_jobs() -> list[Job]:
    """Get the list of jobs.

    Returns:
        List of jobs.
    """
    return JOBS


def get_selection_configuration() -> SchedulingConfig:
    """Load scheduling configuration from the specified path.

    Returns:
        Scheduling configuration.

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
