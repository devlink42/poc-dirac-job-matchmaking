#!/usr/bin/env python3

from __future__ import annotations

from datetime import datetime
from pathlib import Path

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
