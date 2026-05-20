#!/usr/bin/env python3

from __future__ import annotations

import pytest

from matchmaking.core.match_making import filter_compatible_jobs as filter_jobs_python
from matchmaking.core.py_redis.match_making import filter_compatible_jobs as filter_jobs_redis
from matchmaking.core.router import MatchMode, get_filter_strategy, select_job_for_node


@pytest.mark.parametrize("mode", [MatchMode.PYTHON, MatchMode.PYTHON_REDIS])
def test_get_filter_strategy_returns_variant(mode):
    expected = {MatchMode.PYTHON: filter_jobs_python, MatchMode.PYTHON_REDIS: filter_jobs_redis}[mode]

    assert get_filter_strategy(mode) is expected


def test_get_filter_strategy_unsupported_mode_raises():
    with pytest.raises(ValueError, match="Unsupported matchmaking mode"):
        get_filter_strategy("unknown_mode")


@pytest.mark.parametrize("mode", [MatchMode.PYTHON, MatchMode.PYTHON_REDIS])
def test_select_job_for_node_routes_and_selects(mode, example_config, load_job, load_node):
    job = load_job("job_01_mcsimulation_any_site")
    node = load_node("node_01_cern_typical")

    selected = select_job_for_node(mode, node, [job], example_config)

    assert selected == job


@pytest.mark.parametrize("mode", [MatchMode.PYTHON, MatchMode.PYTHON_REDIS])
def test_select_job_for_node_no_compatible_returns_none(mode, example_config, load_job, load_node):
    job = load_job("job_04_wgproduction_with_ram")
    node = load_node("node_01_cern_typical")

    assert select_job_for_node(mode, node, [job], example_config) is None
