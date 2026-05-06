#!/usr/bin/env python3

from __future__ import annotations

import os
from glob import glob

import pytest
from pydantic import ValidationError
from yaml import safe_load

from matchmaking.core.match_making import valid_job, valid_node
from matchmaking.models.job import Job
from matchmaking.models.node import Node


@pytest.mark.parametrize("job_file", sorted(glob("tests/examples/jobs/*.yaml")))
def test_all_job_examples(job_file):
    """Ensure all job files validate (or fail correctly) against the Job model."""
    with open(job_file, "r") as f:
        data = safe_load(f)

    filename = os.path.basename(job_file)
    is_invalid = filename.startswith("invalid_")

    if "job_id" not in data:
        data["job_id"] = "test-job-id"

    # invalid_05 has no specs but remains valid for the Job schema.
    if filename == "invalid_05_job_empty_specs.yaml":
        assert not data.get("matching_specs")

        with pytest.raises(ValidationError):
            Job.model_validate(data)

        return

    if is_invalid:
        with pytest.raises(ValidationError):
            Job.model_validate(data)
    else:
        Job.model_validate(data)


@pytest.mark.parametrize("node_file", sorted(glob("tests/examples/nodes/*.yaml")))
def test_all_node_examples(node_file):
    """Ensure all node files validate (or fail correctly) against the Node model."""
    with open(node_file, "r") as f:
        data = safe_load(f)

    filename = os.path.basename(node_file)
    is_invalid = filename.startswith("invalid_")

    if "node_id" not in data:
        data["node_id"] = "test-node-id"

    if is_invalid:
        with pytest.raises(ValidationError):
            Node.model_validate(data)
    else:
        Node.model_validate(data)


def test_valid_job_failure_paths():
    assert not valid_job("tests/examples/jobs/does_not_exist.yaml")
    assert not valid_job("tests/examples/jobs/invalid_01_job_min_gt_max.yaml")
    assert not valid_job("tests/examples/jobs/invalid_05_job_empty_specs.yaml")


def test_valid_node_failure_paths():
    assert not valid_node("tests/examples/nodes/does_not_exist.yaml")
    assert not valid_node("tests/examples/nodes/invalid_07_node_negative_cores.yaml")


def test_job_model_validation_no_time_or_work():
    job_path = "tests/examples/jobs/job_01_mcsimulation_any_site.yaml"

    with open(job_path, "r") as f:
        job_data = safe_load(f)

    # Mutate to remove both wall-time and cpu-work
    for spec in job_data.get("matching_specs", []):
        spec.pop("wall-time", None)
        spec.pop("cpu-work", None)

    with pytest.raises(ValidationError, match="At least one of 'wall-time' or 'cpu-work' must be provided"):
        Job.model_validate(job_data)
