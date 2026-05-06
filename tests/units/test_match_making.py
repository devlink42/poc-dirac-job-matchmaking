#!/usr/bin/env python3

from __future__ import annotations

import pytest

from matchmaking.core.match_making import match_jobs_with_node, valid_job_specs_with_node

JOB_FILES = {
    "job_01": "tests/examples/jobs/job_01_mcsimulation_any_site.yaml",
    "job_02": "tests/examples/jobs/job_02_mcsimulation_multi_site.yaml",
    "job_03": "tests/examples/jobs/job_03_mcfastsimulation.yaml",
    "job_04": "tests/examples/jobs/job_04_wgproduction_with_ram.yaml",
    "job_05": "tests/examples/jobs/job_05_user_with_banned_site.yaml",
    "job_06": "tests/examples/jobs/job_06_gpu.yaml",
    "job_07": "tests/examples/jobs/job_07_sprucing_niche.yaml",
    "job_08": "tests/examples/jobs/job_08_darwin.yaml",
    "job_09": "tests/examples/jobs/job_09_high_glibc.yaml",
    "job_10": "tests/examples/jobs/job_10_ram_tests.yaml",
    "job_11": "tests/examples/jobs/job_11_ram_limits.yaml",
}

NODE_FILES = {
    "node_01": "tests/examples/nodes/node_01_cern_typical.yaml",
    "node_02": "tests/examples/nodes/node_02_tier2_older.yaml",
    "node_03": "tests/examples/nodes/node_03_gpu.yaml",
    "node_04": "tests/examples/nodes/node_04_low_ram.yaml",
    "node_05": "tests/examples/nodes/node_05_high_glibc.yaml",
    "node_06": "tests/examples/nodes/node_06_darwin.yaml",
}

MATCHMAKING_CASES = [
    # Format: (job_id, node_id, expected_match)
    # Node 01: typical CERN node
    ("job_01", "node_01", True),
    ("job_02", "node_01", True),
    ("job_03", "node_01", True),
    ("job_04", "node_01", False),
    ("job_05", "node_01", True),
    ("job_06", "node_01", False),
    ("job_07", "node_01", True),
    ("job_08", "node_01", False),
    ("job_09", "node_01", False),
    ("job_10", "node_01", False),
    ("job_11", "node_01", False),
    # Node 02: older Tier2
    ("job_01", "node_02", False),
    ("job_02", "node_02", False),
    ("job_03", "node_02", True),
    ("job_04", "node_02", False),
    ("job_05", "node_02", False),
    ("job_06", "node_02", False),
    ("job_07", "node_02", False),
    ("job_08", "node_02", False),
    ("job_09", "node_02", False),
    ("job_10", "node_02", False),
    ("job_11", "node_02", False),
    # Node 03: GPU node
    ("job_01", "node_03", False),
    ("job_02", "node_03", False),
    ("job_03", "node_03", True),
    ("job_04", "node_03", False),
    ("job_05", "node_03", True),
    ("job_06", "node_03", True),
    ("job_07", "node_03", True),
    ("job_08", "node_03", False),
    ("job_09", "node_03", False),
    ("job_10", "node_03", False),
    ("job_11", "node_03", False),
    # Node 04: Low RAM
    ("job_01", "node_04", False),
    ("job_02", "node_04", False),
    ("job_03", "node_04", False),
    ("job_04", "node_04", False),
    ("job_05", "node_04", False),
    ("job_06", "node_04", False),
    ("job_07", "node_04", False),
    ("job_08", "node_04", False),
    ("job_09", "node_04", False),
    ("job_10", "node_04", False),
    ("job_11", "node_04", False),
    # Node 05: High GLIBC
    ("job_01", "node_05", False),
    ("job_02", "node_05", False),
    ("job_03", "node_05", True),
    ("job_04", "node_05", False),
    ("job_05", "node_05", True),
    ("job_06", "node_05", True),
    ("job_07", "node_05", True),
    ("job_08", "node_05", False),
    ("job_09", "node_05", True),
    ("job_10", "node_05", False),
    ("job_11", "node_05", False),
    # Node 06: Darwin
    ("job_01", "node_06", False),
    ("job_02", "node_06", False),
    ("job_03", "node_06", False),
    ("job_04", "node_06", False),
    ("job_05", "node_06", False),
    ("job_06", "node_06", False),
    ("job_07", "node_06", False),
    ("job_08", "node_06", True),
    ("job_09", "node_06", False),
    ("job_10", "node_06", False),
    ("job_11", "node_06", False),
]


@pytest.mark.parametrize(
    "job_id, node_id, expected_match",
    MATCHMAKING_CASES,
)
def test_matchmaking_logic(load_job, load_node, job_id, node_id, expected_match):
    """Test matchmaking at both levels.

    1. Core logic (valid_job_specs_with_node)
    2. Higher-level API (match_jobs_with_node)
    """
    job_file = JOB_FILES[job_id]
    node_file = NODE_FILES[node_id]

    # Level 1: Core logic verification
    node_obj = load_node(node_file)
    job_objs = load_job(job_file)
    core_matches = [valid_job_specs_with_node(job_id, job_specs, node_obj) for job_specs in job_objs.matching_specs]

    # Level 2: Higher-level API verification
    job_match_list, _ = match_jobs_with_node(job_file, node_file)

    if expected_match:
        assert any(core_matches), f"Core: Expected {job_id} to match {node_id} ({job_file})"
        assert any(job_match_list), f"API: Expected {job_id} to match {node_id} ({job_file})"
    else:
        assert not any(core_matches), f"Core: Expected {job_id} NOT to match {node_id} ({job_file})"
        assert not any(job_match_list), f"API: Expected {job_id} NOT to match {node_id} ({job_file})"
