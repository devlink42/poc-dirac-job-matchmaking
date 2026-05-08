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


def _create_loader(model_cls, sub_dir: str):
    def _load(name: str):
        base_path = Path(__file__).parent

        if "/" in name:
            path = base_path.parent / name
            if not str(path).endswith(".yaml"):
                path = path.with_name(f"{path.name}.yaml")
        else:
            filename = name if name.endswith(".yaml") else f"{name}.yaml"
            path = base_path / "examples" / sub_dir / filename

        return model_cls.load_from_yaml(path)

    return _load


@pytest.fixture
def load_job():
    return _create_loader(Job, "jobs")


@pytest.fixture
def load_node():
    return _create_loader(Node, "nodes")


@pytest.fixture
def load_config():
    return _create_loader(SchedulingConfig, "config")


@pytest.fixture
def base_time():
    return datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
