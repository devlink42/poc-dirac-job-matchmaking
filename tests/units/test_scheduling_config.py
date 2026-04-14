#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from src.models.config import SchedulingConfig
from tests.conftest import PROJECT_ROOT

CONFIG_DIR = PROJECT_ROOT / "tests" / "examples" / "config"


def test_load_scheduling_config_from_valid_yaml():
    config = SchedulingConfig.load_from_yaml(CONFIG_DIR / "config_01_scheduling_valid.yaml")

    assert config.job_type_priorities == ["wg_production", "mc_simulation", "user_analysis"]
    assert config.running_limits["default"]["mc_simulation"] == 1000
    assert config.running_limits["LCG.CERN.ch"]["wg_production"] == 500


def test_load_scheduling_config_from_empty_yaml():
    config = SchedulingConfig.load_from_yaml(CONFIG_DIR / "config_02_scheduling_empty.yaml")

    assert config.job_type_priorities == []
    assert config.running_limits == {}


def test_load_scheduling_config_missing_file_raises():
    missing_file = Path(CONFIG_DIR / "does_not_exist.yaml")
    with pytest.raises(FileNotFoundError):
        SchedulingConfig.load_from_yaml(missing_file)


def test_load_scheduling_config_invalid_yaml_raises_validation_error():
    with pytest.raises(ValidationError):
        SchedulingConfig.load_from_yaml(CONFIG_DIR / "invalid_01_scheduling_negative_limit.yaml")
