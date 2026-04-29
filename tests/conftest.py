#!/usr/bin/env python3

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from matchmaking.models.config import SchedulingConfig
from matchmaking.models.job import Job
from matchmaking.models.node import Node


@pytest.fixture
def example_config():
    config_path = Path(__file__).parent / "examples/config/config_01_scheduling_valid.yaml"

    return SchedulingConfig.load_from_yaml(config_path)


@pytest.fixture
def load_job():
    def _load(name):
        # Allow passing full path or just the name
        base_path = Path(__file__).parent
        if name.endswith(".yaml"):
            path = base_path.parent / name
        elif "/" in name:
            path = base_path.parent / f"{name}.yaml"
        else:
            path = base_path / f"examples/jobs/{name}.yaml"

        return Job.load_from_yaml(path)

    return _load


@pytest.fixture
def load_node():
    def _load(name):
        # Allow passing full path or just the name
        base_path = Path(__file__).parent
        if name.endswith(".yaml"):
            path = base_path.parent / name
        elif "/" in name:
            path = base_path.parent / f"{name}.yaml"
        else:
            path = base_path / f"examples/nodes/{name}.yaml"

        return Node.load_from_yaml(path)

    return _load


@pytest.fixture
def base_time():
    return datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
