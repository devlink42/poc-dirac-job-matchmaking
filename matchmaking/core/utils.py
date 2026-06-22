#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path

from ruamel.yaml import YAML

from matchmaking.config.logger import logger
from matchmaking.models.config import SchedulingConfig
from matchmaking.models.job import Job

CONFIG_PATH = "matchmaking/config/scheduling.yaml"
JOBS = "tests/examples/jobs/"


def get_jobs() -> list[Job]:
    """Load job examples from the specified path.

    Returns:
        list[Job]: List of job examples.
    """
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
        logger.info(f"Loaded job examples from: '{JOBS}'")

    return jobs


def get_selection_configuration() -> SchedulingConfig:
    """Load scheduling configuration from the specified path.

    Returns:
        SchedulingConfig: Scheduling configuration.
    """
    try:
        config = SchedulingConfig.load_from_yaml(CONFIG_PATH)
    except FileNotFoundError as e:
        raise ValueError(f"Scheduling config not found at: '{CONFIG_PATH}'") from e
    except Exception as e:
        raise ValueError(f"Failed to load scheduling config from: '{CONFIG_PATH}': {e}") from e
    else:
        logger.info(f"Loaded scheduling config from: '{CONFIG_PATH}'")

    return config


def assign_job_to_site(job: Job, node_site: str):
    job_yaml = YAML()
    job_yaml.preserve_quotes = True
    job_yaml.default_flow_style = False
    job_yaml.indent(mapping=2, sequence=4, offset=2)
    # TODO: check how to retrieve the job file path
