from __future__ import annotations

import pytest
from pydantic import ValidationError

from matchmaking.models.job import Job

BASE_JOB_DATA = {
    "job_id": "test-job",
    "system": {"name": "linux"},
    "wall-time": 3600,
    "cpu": {
        "num-cores": {"min": 1, "max": 1},
        "architecture": {"name": "x86_64", "microarchitecture-level": {"min": 1}},
    },
}


def test_invalid_tag_expression():
    job_data = BASE_JOB_DATA.copy()
    job_data["tags"] = "cvmfs:lhcb & (os:el9 | )"  # Invalid expression: empty parentheses or missing operand

    with pytest.raises(ValidationError) as excinfo:
        Job.model_validate(job_data)

    assert "Invalid tag expression" in str(excinfo.value)


def test_unsupported_ast_node_in_tags():
    job_data = BASE_JOB_DATA.copy()
    job_data["tags"] = "cvmfs:lhcb if os:el9 else os:alma9"  # Conditional expression not supported

    with pytest.raises(ValidationError) as excinfo:
        Job.model_validate(job_data)

    assert "Invalid tag expression syntax" in str(excinfo.value)


def test_valid_tag_expression():
    job_data = BASE_JOB_DATA.copy()
    job_data["tags"] = "cvmfs:lhcb & (os:el9 | os:alma9)"
    # Should not raise
    Job.model_validate(job_data)


@pytest.mark.parametrize("operator", ["+", "-", "*", "/", "%", "**", "//", ","])
def test_unsupported_operator_in_tags(operator):
    job_data = BASE_JOB_DATA.copy()
    job_data["tags"] = f"cvmfs:lhcb {operator} os:el9"  # Unsupported operation

    with pytest.raises(ValidationError) as excinfo:
        Job.model_validate(job_data)

    assert "Invalid tag expression" in str(excinfo.value)
    assert "Unsupported operation in tag expression" in str(excinfo.value)
