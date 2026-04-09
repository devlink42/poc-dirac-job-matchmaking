#!/usr/bin/env python3

from __future__ import annotations

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


@pytest.mark.parametrize(
    "job_file, node_file, node_id, expected_match",
    [
        # SUCCESS CASES
        # Typical MCSimulation on CERN node
        (
            "tests/examples/jobs/job_01_mcsimulation_any_site.yaml",
            "tests/examples/nodes/pilot_01_cern_typical.yaml",
            1,
            True,
        ),
        # MCFastSimulation (v1) on Older Tier-2 node (v2)
        ("tests/examples/jobs/job_03_mcfastsimulation.yaml", "tests/examples/nodes/pilot_02_tier2_older.yaml", 2, True),
        # GPU job on GPU node
        ("tests/examples/jobs/job_06_gpu.yaml", "tests/examples/nodes/pilot_03_gpu.yaml", 3, True),
        # FAILURE CASES
        # OS Mismatch (Darwin vs Linux)
        ("tests/examples/jobs/job_08_darwin.yaml", "tests/examples/nodes/pilot_01_cern_typical.yaml", 1, False),
        # GLIBC too old on node (Job 2.35 vs Node 2.28)
        ("tests/examples/jobs/job_09_high_glibc.yaml", "tests/examples/nodes/pilot_01_cern_typical.yaml", 1, False),
        # Microarchitecture level too low (Job v4 vs Node v2)
        (
            "tests/examples/jobs/job_01_mcsimulation_any_site.yaml",
            "tests/examples/nodes/pilot_02_tier2_older.yaml",
            2,
            False,
        ),
        # RAM too small (Job 1.5GB vs Node 1GB)
        (
            "tests/examples/jobs/job_01_mcsimulation_any_site.yaml",
            "tests/examples/nodes/pilot_04_low_ram.yaml",
            4,
            False,
        ),
        # Missing GPU on node
        ("tests/examples/jobs/job_06_gpu.yaml", "tests/examples/nodes/pilot_01_cern_typical.yaml", 1, False),
        # Tag mismatch (missing gpu:nvidia tag)
        ("tests/examples/jobs/job_06_gpu.yaml", "tests/examples/nodes/pilot_01_cern_typical.yaml", 1, False),
    ],
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
