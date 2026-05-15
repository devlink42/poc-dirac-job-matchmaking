#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from matchmaking.models.config import SchedulingConfig
from matchmaking.models.utils import JobType


def test_load_scheduling_config_from_valid_yaml(load_config):
    config = load_config("config_01_scheduling_valid")

    assert config.job_type_priorities == [
        JobType.WGPRODUCTION,
        JobType.SPRUCING,
        JobType.MCFASTSIMULATION,
        JobType.MCSIMULATION,
        JobType.USER,
        JobType.MERGE,
        JobType.MCRECONSTRUCTION,
        JobType.APMERGE,
        JobType.APPOSTPROC,
        JobType.MCMERGE,
        JobType.LBAPI,
    ]
    assert config.running_limits["default"][JobType.MCSIMULATION] == 1000
    assert config.running_limits["LCG.CERN.cern"][JobType.WGPRODUCTION] == 1000


def test_load_scheduling_config_from_empty_yaml(load_config):
    config = load_config("config_02_scheduling_empty")

    assert config.job_type_priorities == []
    assert config.running_limits == {}


def test_load_scheduling_config_missing_file_raises():
    missing_file = Path("tests/examples/config/does_not_exist.yaml")

    with pytest.raises(FileNotFoundError):
        SchedulingConfig.load_from_yaml(missing_file)


def test_load_scheduling_config_invalid_yaml_raises_validation_error():
    with pytest.raises(ValidationError):
        SchedulingConfig.load_from_yaml("tests/examples/config/invalid_10_scheduling_negative_limit.yaml")
