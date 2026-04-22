#!/usr/bin/env python3

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from config import configure_logger
from src.models.config import SchedulingConfig
from src.models.job import Job
from src.models.node import Node

PROJECT_ROOT = Path(__file__).parent.parent.absolute()


def pytest_configure() -> None:
    """Apply a consistent logger configuration for the whole test suite."""
    configure_logger("DEBUG")


@pytest.fixture
def example_config():
    return SchedulingConfig.load_from_yaml(PROJECT_ROOT / "tests/examples/config/config_01_scheduling_valid.yaml")


@pytest.fixture
def load_job():
    def _load(name):
        path = PROJECT_ROOT / f"tests/examples/jobs/{name}.yaml"
        with open(path, "r") as f:
            data = yaml.safe_load(f)

        return Job.model_validate(data)

    return _load


@pytest.fixture
def load_node():
    def _load(name):
        path = PROJECT_ROOT / f"tests/examples/nodes/{name}.yaml"
        with open(path, "r") as f:
            data = yaml.safe_load(f)

        return Node.model_validate(data)

    return _load


@pytest.fixture
def base_time():
    return datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


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
