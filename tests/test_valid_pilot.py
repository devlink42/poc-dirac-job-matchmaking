#!/usr/bin/env python3

from __future__ import annotations

from glob import glob
from itertools import product

import pytest
import yaml

from src.core.valid_pilot import valid_job_with_node, valid_pilot
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
    1: "tests/examples/nodes/pilot_01_cern_typical.yaml",
    2: "tests/examples/nodes/pilot_02_tier2_older.yaml",
    3: "tests/examples/nodes/pilot_03_gpu.yaml",
    4: "tests/examples/nodes/pilot_04_low_ram.yaml",
    5: "tests/examples/nodes/pilot_05_high_glibc.yaml",
    6: "tests/examples/nodes/pilot_06_darwin.yaml",
}

# Matrix from the expected behavior table shared in the test request (job x pilot_01..pilot_06).
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
    (JOB_FILES[job_id], NODE_FILES[node_id], node_id, EXPECTED_BY_JOB[job_id][node_id - 1])
    for job_id in ("job_01", "job_02", "job_03", "job_04", "job_05", "job_06", "job_07", "job_08", "job_09")
    for node_id in (1, 2, 3, 4, 5, 6)
]


@pytest.mark.parametrize(
    "job_file, node_file, node_id, expected_match",
    MATCHMAKING_CASES,
)
def test_matchmaking_combinations(job_file, node_file, node_id, expected_match):
    """Test the core matchmaking logic between jobs and nodes from YAML examples."""
    node = load_node(node_file, node_id)
    jobs = load_job_specs(job_file)

    # Check if any spec matches (for the tested files, usually only one spec exists or result is same for all)
    matches = [valid_job_with_node(job, node) for job in jobs]

    if expected_match:
        assert any(matches), f"Expected at least one match for {job_file} and {node_file}"
    else:
        assert not any(matches), f"Expected no match for {job_file} and {node_file}"


def test_valid_pilot_from_files():
    """Test the higher-level valid_pilot function using real YAML paths."""
    matches = valid_pilot(
        "tests/examples/jobs/job_01_mcsimulation_any_site.yaml", "tests/examples/nodes/pilot_01_cern_typical.yaml"
    )

    assert len(matches) == 1
    assert matches[0].job_id == "unknown-job-id"


@pytest.mark.parametrize(
    "job_file, node_file",
    list(product(sorted(glob("tests/examples/jobs/job_0*.yaml")), sorted(glob("tests/examples/nodes/pilot_0*.yaml")))),
)
def test_all_job_node_combinations(job_file, node_file):
    """Not a test, but useful for debugging invalid combinations."""
    res = valid_pilot(job_file, node_file)
    print(f"Validating {job_file} against {node_file}:\n{res}")
