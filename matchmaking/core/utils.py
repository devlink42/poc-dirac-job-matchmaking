#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path

from matchmaking.config.logger import logger
from matchmaking.models.config import SchedulingConfig
from matchmaking.models.job import Job

CONFIG_PATH = "matchmaking/config/scheduling.yaml"
JOB_PATH = "tests/examples/jobs/"


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
