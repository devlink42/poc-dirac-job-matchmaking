#!/usr/bin/env python3

from __future__ import annotations

from matchmaking.core.router import select_job


def test_select_job_routes_and_selects(example_config, load_job, load_node):
    job = load_job("job_01_mcsimulation_any_site")
    node = load_node("node_01_cern_typical")

    selected = select_job(node, [job], example_config)

    assert selected == job


def test_select_job_no_compatible_returns_none(example_config, load_job, load_node):
    job = load_job("job_04_wgproduction_with_ram")
    node = load_node("node_01_cern_typical")

    assert select_job(node, [job], example_config) is None
