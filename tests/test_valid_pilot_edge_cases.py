#!/usr/bin/env python3

from __future__ import annotations

from copy import deepcopy

import pytest
import yaml

from src.core.valid_pilot import _eval_tag_expression, valid_job_with_node, valid_pilot
from src.models.job import Job
from src.models.node import Node


def _base_node_spec() -> dict:
    return {
        "node_id": "edge-node",
        "site": "LCG.CERN.cern",
        "system": {"name": "Linux", "glibc": "2.28", "user-namespaces": True},
        "wall-time": 3600,
        "cpu-work": 1000,
        "cpu": {
            "num-nodes": 1,
            "num-cores": 1,
            "ram-mb": 1536,
            "architecture": {"name": "x86_64", "microarchitecture-level": 4},
        },
        "gpu": {
            "count": 1,
            "ram-mb": 8192,
            "vendor": "nvidia",
            "compute-capability": "8.0",
            "driver-version": "510.47.03",
        },
        "tags": ["cvmfs:lhcb", "gpu:nvidia", "os:el9"],
    }


def _base_job_spec() -> dict:
    return {
        "job_id": "edge-job",
        "site": "LCG.CERN.cern",
        "system": {"name": "Linux", "glibc": "2.28", "user-namespaces": True},
        "wall-time": 3600,
        "cpu-work": 1000,
        "cpu": {
            "num-cores": {"min": 1, "max": 1},
            "ram-mb": {
                "request": {"overhead": 1024, "per-core": 512},
                "limit": {"overhead": 1024, "per-core": 512},
            },
            "architecture": {"name": "x86_64", "microarchitecture-level": {"min": 4, "max": 4}},
        },
        "gpu": {
            "count": {"min": 1, "max": 1},
            "ram-mb": 8192,
            "vendor": "nvidia",
            "compute-capability": {"min": "8.0", "max": "8.0"},
            "driver-version": "510.47.03",
        },
        "tags": "cvmfs:lhcb & gpu:nvidia",
    }


def test_eval_tag_expression_supports_special_chars_and_not_operator():
    assert _eval_tag_expression("cvmfs:lhcb & ~diracx:banned:LCG.NIPNE-07.ro", {"cvmfs:lhcb"})
    assert not _eval_tag_expression("cvmfs:lhcb & diracx:banned:LCG.NIPNE-07.ro", {"cvmfs:lhcb"})


def test_eval_tag_expression_respects_precedence_and_parentheses():
    assert _eval_tag_expression("a | b & c", {"b", "c"})
    assert _eval_tag_expression("(a | b) & c", {"b", "c"})
    assert not _eval_tag_expression("(a | b) & c", {"b"})


def test_eval_tag_expression_invalid_syntax_returns_false():
    assert not _eval_tag_expression("a & (", {"a"})


def test_valid_job_with_node_accepts_boundary_equal_values():
    job = Job.model_validate(_base_job_spec())
    node = Node.model_validate(_base_node_spec())
    assert valid_job_with_node(job, node)


def test_valid_pilot_returns_empty_for_invalid_node(tmp_path):
    job_file = tmp_path / "job.yaml"
    node_file = tmp_path / "node_invalid.yaml"

    with open(job_file, "w") as f:
        yaml.safe_dump({"matching_specs": [_base_job_spec()]}, f)

    invalid_node = _base_node_spec()
    invalid_node.pop("site")
    with open(node_file, "w") as f:
        yaml.safe_dump(invalid_node, f)

    assert valid_pilot(str(job_file), str(node_file)) == []


def test_valid_pilot_skips_invalid_job_spec_and_keeps_valid_one(tmp_path):
    job_file = tmp_path / "job_mixed.yaml"
    node_file = tmp_path / "node.yaml"

    valid_job = _base_job_spec()
    valid_job["job_id"] = "valid-job"
    invalid_job = deepcopy(_base_job_spec())
    invalid_job["job_id"] = "invalid-job"
    invalid_job["cpu"]["num-cores"] = {"min": 2, "max": 1}

    with open(job_file, "w") as f:
        yaml.safe_dump({"matching_specs": [invalid_job, valid_job]}, f)
    with open(node_file, "w") as f:
        yaml.safe_dump(_base_node_spec(), f)

    matches = valid_pilot(str(job_file), str(node_file))
    assert len(matches) == 1
    assert matches[0].job_id == "valid-job"


@pytest.mark.parametrize("job_content", [{}, {"matching_specs": []}])
def test_valid_pilot_handles_missing_or_empty_matching_specs(tmp_path, job_content):
    job_file = tmp_path / "job_empty.yaml"
    node_file = tmp_path / "node.yaml"

    with open(job_file, "w") as f:
        yaml.safe_dump(job_content, f)
    with open(node_file, "w") as f:
        yaml.safe_dump(_base_node_spec(), f)

    assert valid_pilot(str(job_file), str(node_file)) == []
