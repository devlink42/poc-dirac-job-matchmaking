#!/usr/bin/env python3
"""Unit tests for the Alternative C requirement-group model."""

from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from matchmaking.models.lua.alt_c.requirement_group import RequirementGroup


def _group_data(**overrides):
    data = {
        "eligible_sites": [" site-b ", "site-a", "site-a"],
        "job_type": "MCSimulation",
        "system_name": "Linux",
        "min_cpu_cores": 1,
        "max_cpu_cores": 4,
        "architecture": "x86_64",
        "min_microarchitecture_level": 3,
        "has_gpu": False,
        "tags": "cvmfs:lhcb",
    }
    data.update(overrides)
    return data


def test_group_normalizes_and_serializes_deterministically():
    group = RequirementGroup.model_validate(_group_data())
    group_ab = RequirementGroup.model_validate(_group_data(eligible_sites=["site-a", "site-b"])).canonical_json()

    assert group.eligible_sites == ("site-a", "site-b")
    assert group.canonical_json() == group_ab
    assert len(group.req_group_id) == 64

    redis_hash = group.to_redis_hash()
    assert redis_hash["eligible_sites"] == '["site-a","site-b"]'
    assert redis_hash["has_gpu"] == "0"
    assert "min_system_glibc" not in redis_hash
    assert json.loads(redis_hash["eligible_sites"]) == ["site-a", "site-b"]


@pytest.mark.parametrize(
    "sites",
    ["site-a", b"site-a", [], ["site-a", "   "], ["site-a", 1]],
)
def test_group_rejects_invalid_sites(sites):
    with pytest.raises((TypeError, ValueError, ValidationError)):
        RequirementGroup.model_validate(_group_data(eligible_sites=sites))


@pytest.mark.parametrize(
    ("minimum", "maximum", "field", "message"),
    [
        (2, 1, "min_cpu_cores", "CPU core"),
        (3, 2, "min_microarchitecture_level", "microarchitecture level"),
        (2, 1, "min_gpu_count", "GPU count"),
        ("8.0", "7.0", "min_gpu_compute_capability", "GPU compute capability"),
    ],
)
def test_group_rejects_inverted_ranges(minimum, maximum, field, message):
    with pytest.raises(ValidationError, match=f"maximum {message}"):
        RequirementGroup.model_validate(
            _group_data(
                **{
                    field: minimum,
                    field.replace("min_", "max_"): maximum,
                }
            )
        )
