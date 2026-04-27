from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from matchmaking.models.job import Job
from matchmaking.models.utils import JobGroup, JobType, SystemName

BASE_JOB_DATA = {
    "job_id": "test-job",
    "owner": "test-owner",
    "group": JobGroup.LHCB_MC,
    "job_type": JobType.USER,
    "submission_time": datetime.now(tz=timezone.utc),
    "matching_specs": [
        {
            "system": {"name": SystemName.LINUX},
            "wall-time": 3600,
            "cpu": {
                "num-cores": {"min": 1, "max": 1},
                "architecture": {"name": "x86_64", "microarchitecture-level": {"min": 1}},
            },
            "tags": "a",
        }
    ],
}


def test_invalid_tag_expression():
    job_data = BASE_JOB_DATA.copy()
    job_data["matching_specs"][0]["tags"] = "a & (b | )"  # Invalid expression

    with pytest.raises(ValidationError) as excinfo:
        Job.model_validate(job_data)

    assert "Invalid tag expression" in str(excinfo.value)


def test_unsupported_ast_node_in_tags():
    job_data = BASE_JOB_DATA.copy()
    job_data["matching_specs"][0]["tags"] = "a if b else c"  # Conditional expression not supported

    with pytest.raises(ValidationError) as excinfo:
        Job.model_validate(job_data)

    assert "Invalid tag expression syntax" in str(excinfo.value)


def test_valid_tag_expression():
    job_data = BASE_JOB_DATA.copy()
    job_data["matching_specs"][0]["tags"] = "a & (b | c)"
    # Should not raise
    Job.model_validate(job_data)


def test_unsupported_operator_in_tags():
    job_data = BASE_JOB_DATA.copy()
    job_data["matching_specs"][0]["tags"] = "a + b"  # '+' is not supported

    with pytest.raises(ValidationError) as excinfo:
        Job.model_validate(job_data)

    assert "Invalid tag expression" in str(excinfo.value)
    assert "Unsupported operation" in str(excinfo.value)
