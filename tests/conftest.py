#!/usr/bin/env python3

from __future__ import annotations

from datetime import datetime, timezone

import pytest
import yaml

from matchmaking.config.paths import PROJECT_ROOT
from matchmaking.models.config import SchedulingConfig
from matchmaking.models.job import Job
from matchmaking.models.node import Node


@pytest.fixture
def example_config():
    return SchedulingConfig.load_from_yaml(PROJECT_ROOT / "tests/examples/config/config_01_scheduling_valid.yaml")


@pytest.fixture
def load_job():
    def _load(name):
        # Allow passing full path or just the name
        if name.endswith(".yaml"):
            path = PROJECT_ROOT / name
        elif "/" in name:
            path = PROJECT_ROOT / f"{name}.yaml"
        else:
            path = PROJECT_ROOT / f"tests/examples/jobs/{name}.yaml"

        with open(path, "r") as f:
            data = yaml.safe_load(f)

        return Job.model_validate(data)

    return _load


@pytest.fixture
def load_node():
    def _load(name):
        # Allow passing full path or just the name
        if name.endswith(".yaml"):
            path = PROJECT_ROOT / name
        elif "/" in name:
            path = PROJECT_ROOT / f"{name}.yaml"
        else:
            path = PROJECT_ROOT / f"tests/examples/nodes/{name}.yaml"

        with open(path, "r") as f:
            data = yaml.safe_load(f)

        return Node.model_validate(data)

    return _load


@pytest.fixture
def base_time():
    return datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
