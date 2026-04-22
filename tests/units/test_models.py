#!/usr/bin/env python3

from __future__ import annotations

import os
from glob import glob

import pytest
from pydantic import ValidationError
from yaml import safe_load

from src.models.job import Job
from src.models.node import Node


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
    if filename == "invalid_05_empty_specs.yaml":
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
