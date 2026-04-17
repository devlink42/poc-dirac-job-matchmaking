#!/usr/bin/env python3

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest

from config import configure_logger
from src.models.config import SchedulingConfig
from src.models.utils import JobType

PROJECT_ROOT = Path(__file__).parent.parent.absolute()


def pytest_configure() -> None:
    """Apply a consistent logger configuration for the whole test suite."""
    configure_logger("DEBUG")


@pytest.fixture
def config():
    return SchedulingConfig(
        job_type_priorities=[JobType.WGPRODUCTION, JobType.MCSIMULATION, JobType.USER],
        running_limits={
            "default": {JobType.MCSIMULATION: 1000, JobType.USER: 200},
            "LCG.CERN.ch": {JobType.WGPRODUCTION: 500, JobType.MCSIMULATION: 2000},
        },
    )


@pytest.fixture
def base_time():
    return datetime(2026, 1, 1, 12, 0, 0, tzinfo=datetime.now().astimezone().tzinfo)


@pytest.fixture
def mock_argv():
    with patch("sys.argv", ["scheduler.py", "jobs.yaml", "pilot.yaml", "config.yaml"]):
        yield


@pytest.fixture
def mock_valid_pilot():
    with patch("src.core.scheduler.valid_pilot") as mock:
        yield mock


@pytest.fixture
def mock_select_job():
    with patch("src.core.scheduler.select_job") as mock:
        yield mock


@pytest.fixture
def mock_scheduling_config():
    with patch("src.core.scheduler.SchedulingConfig") as mock:
        yield mock


@pytest.fixture
def mock_logger():
    with patch("src.core.scheduler.logger") as mock:
        yield mock
