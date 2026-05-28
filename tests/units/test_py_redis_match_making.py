#!/usr/bin/env python3

from __future__ import annotations

import pytest
import yaml
from pydantic import ValidationError

import matchmaking.core.py_redis.match_making as match_making_module
from matchmaking.core.py_redis.match_making import match_jobs_with_node_redis
from matchmaking.models.job import Job
from matchmaking.models.node import Node

NODE_01_PATH = "tests/examples/nodes/node_01_cern_typical.yaml"
JOB_01_PATH = "tests/examples/jobs/job_01_mcsimulation_any_site.yaml"


@pytest.fixture
def sample_job():
    with open(JOB_01_PATH) as f:
        data = yaml.safe_load(f)
        return Job.model_validate(data)


def test_match_jobs_with_node_redis_success(sample_job, monkeypatch):
    jobs, node = match_jobs_with_node_redis([sample_job], NODE_01_PATH)
    assert len(jobs) >= 1
    assert isinstance(node, Node)


def test_match_jobs_with_node_redis_node_without_id(sample_job, monkeypatch, caplog):
    original_load = Node.load_from_yaml

    def mock_load(path):
        node = original_load(path)
        node.node_id = None
        return node

    monkeypatch.setattr(Node, "load_from_yaml", mock_load)

    jobs, node = match_jobs_with_node_redis([sample_job], NODE_01_PATH)

    # Path is tests/examples/nodes/node_01_cern_typical.yaml, stem is node_01_cern_typical
    assert node.node_id == "node_01_cern_typical"
    assert "Node ID not specified" in caplog.text


def test_match_jobs_with_node_redis_invalid_node(sample_job, monkeypatch, caplog):
    def mock_load(path):
        raise ValidationError.from_exception_data(title="Node", line_errors=[], input_type="python")

    monkeypatch.setattr(Node, "load_from_yaml", mock_load)

    with pytest.raises(ValidationError):
        match_jobs_with_node_redis([sample_job], NODE_01_PATH)


def test_match_jobs_with_node_redis_invalid_job_spec(sample_job, monkeypatch, caplog):
    def mock_valid_job_specs(*args, **kwargs):
        raise ValidationError.from_exception_data(title="Job", line_errors=[], input_type="python")

    monkeypatch.setattr(match_making_module, "valid_job_specs_with_node", mock_valid_job_specs)

    jobs, node = match_jobs_with_node_redis([sample_job], NODE_01_PATH)

    assert jobs == []
    assert "Invalid job specification:" in caplog.text
