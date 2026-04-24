#!/usr/bin/env python3

from __future__ import annotations

import pytest
import yaml

from src.core.match_making import match_jobs_with_node, valid_job_with_node
from src.models.job import Job
from src.models.node import Node


def load_node(node_path: str, node_id: int) -> Node:
    """Load a Node object from a YAML file."""
    with open(node_path, "r") as f:
        data = yaml.safe_load(f)

    if "node_id" not in data:
        data["node_id"] = f"test-node-{node_id}"

    return Node.model_validate(data)


def load_job_specs(job_path: str) -> list[Job]:
    """Load Job objects from a YAML file containing matching_specs."""
    with open(job_path, "r") as f:
        data = yaml.safe_load(f)

    specs = []
    for i, spec in enumerate(data.get("matching_specs", [])):
        if "job_id" not in spec:
            spec["job_id"] = f"test-job-{i}"

        specs.append(Job.model_validate(spec))

    return specs


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
}

NODE_FILES = {
    1: "tests/examples/nodes/node_01_cern_typical.yaml",
    2: "tests/examples/nodes/node_02_tier2_older.yaml",
    3: "tests/examples/nodes/node_03_gpu.yaml",
    4: "tests/examples/nodes/node_04_low_ram.yaml",
    5: "tests/examples/nodes/node_05_high_glibc.yaml",
    6: "tests/examples/nodes/node_06_darwin.yaml",
}

# Matrix from the expected behavior table shared in the test request (job x node_01..node_06).
EXPECTED_BY_JOB = {
    "job_01": (True, False, False, False, False, False),
    "job_02": (True, False, False, False, False, False),
    "job_03": (True, True, True, False, True, False),
    "job_04": (False, False, False, False, False, False),
    "job_05": (True, False, True, False, True, False),
    "job_06": (False, False, True, False, True, False),
    "job_07": (True, False, True, False, True, False),
    "job_08": (False, False, False, False, False, True),
    "job_09": (False, False, False, False, True, False),
}

MATCHMAKING_CASES = [
    (job_id, node_id, EXPECTED_BY_JOB[job_id][node_id - 1])
    for job_id in sorted(JOB_FILES.keys())
    for node_id in sorted(NODE_FILES.keys())
]


@pytest.mark.parametrize(
    "job_id, node_id, expected_match",
    MATCHMAKING_CASES,
)
def test_matchmaking_logic(job_id, node_id, expected_match):
    """Test matchmaking at both levels.

    1. Core logic (valid_job_with_node)
    2. Higher-level API (match_jobs_with_node)
    """
    job_file = JOB_FILES[job_id]
    node_file = NODE_FILES[node_id]

    # Level 1: Core logic verification
    node_obj = load_node(node_file, node_id)
    job_objs = load_job_specs(job_file)
    core_matches = [valid_job_with_node(job, node_obj) for job in job_objs]

    # Level 2: Higher-level API verification
    api_matches = match_jobs_with_node(job_file, node_file)

    if expected_match:
        assert any(core_matches), f"Core: Expected {job_id} to match {node_id} ({job_file})"
        assert any(api_matches), f"API: Expected {job_id} to match {node_id} ({job_file})"
    else:
        assert not any(core_matches), f"Core: Expected {job_id} NOT to match {node_id} ({job_file})"
        assert not any(api_matches), f"API: Expected {job_id} NOT to match {node_id} ({job_file})"
