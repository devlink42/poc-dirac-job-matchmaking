#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path

from matchmaking.config.logger import logger
from matchmaking.models.config import SchedulingConfig
from matchmaking.models.job import Job
from matchmaking.models.utils import JobStatus

CONFIG_PATH: str = "matchmaking/config/scheduling.yaml"
JOBS: str | list[Job] = "tests/examples/jobs/"

_JOBS_CACHE: list[Job] | None = None


def get_jobs() -> list[Job]:
    """Load job examples from the specified path.

    Returns:
        list[Job]: List of job examples.

    Raises:
        ValueError: If the job examples file is not found or fails to load.
    """
    global _JOBS_CACHE

    if _JOBS_CACHE is not None:
        return _JOBS_CACHE

    if isinstance(JOBS, list) and isinstance(any(job for job in JOBS if isinstance(job, Job)), Job):
        logger.info("Using in-memory job examples")

        _JOBS_CACHE = JOBS
    elif isinstance(JOBS, str) and Path(JOBS).exists():
        logger.info("Loading job examples from: '%s'", JOBS)
        try:
            jobs = []

            for job_file in Path(JOBS).glob("*.yaml"):
                if job_file.stem.startswith("invalid"):
                    continue

                jobs.append(Job.load_from_yaml(job_file))
        except FileNotFoundError as e:
            raise ValueError(f"Job examples not found at: '{JOBS}'") from e
        except Exception as e:
            raise ValueError(f"Failed to load job examples from: '{JOBS}': {e}") from e
        else:
            logger.info("Loaded job examples from: '%s'", JOBS)
            _JOBS_CACHE = jobs
    else:
        raise ValueError(f"Invalid JOBS path: '{JOBS}'")

    return _JOBS_CACHE


def get_selection_configuration() -> SchedulingConfig:
    """Load scheduling configuration from the specified path.

    Returns:
        SchedulingConfig: Scheduling configuration.

    Raises:
        ValueError: If the scheduling config file is not found or fails to load.
    """
    try:
        config = SchedulingConfig.load_from_yaml(CONFIG_PATH)
    except FileNotFoundError as e:
        raise ValueError(f"Scheduling config not found at: '{CONFIG_PATH}'") from e
    except Exception as e:
        raise ValueError(f"Failed to load scheduling config from: '{CONFIG_PATH}': {e}") from e
    else:
        logger.info("Loaded scheduling config from: '%s'", CONFIG_PATH)

    return config


def assign_job_to_site(job: Job, node_site: str):
    job.assigned_site = node_site
    job.status = JobStatus.RUNNING

    logger.debug("Assigned job '%s' to site '%s' in memory.", job.job_id, node_site)
