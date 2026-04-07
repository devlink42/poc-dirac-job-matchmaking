import pytest
import yaml
from pydantic import ValidationError

from src.models.job import Job
from src.models.node import Node


def test_job_validation_invalid_range():
    with open("tests/examples/jobs/invalid_01_min_gt_max.yaml", "r") as f:
        data = yaml.safe_load(f)

    # Prends le premier spec
    spec = data["matching_specs"][0]
    spec["job_id"] = "test"

    with pytest.raises(ValidationError) as exc_info:
        Job.model_validate(spec)

    assert "max must be greater than or equal to min" in str(exc_info.value)


def test_job_validation_negative_walltime():
    with open("tests/examples/jobs/invalid_02_negative_walltime.yaml", "r") as f:
        data = yaml.safe_load(f)

    spec = data["matching_specs"][0]
    spec["job_id"] = "test"

    with pytest.raises(ValidationError):
        Job.model_validate(spec)


def test_node_validation_negative_cores():
    with open("tests/examples/nodes/invalid_07_pilot_negative_cores.yaml", "r") as f:
        data = yaml.safe_load(f)

    data["node_id"] = "test"

    with pytest.raises(ValidationError):
        Node.model_validate(data)


def test_job_gpu_validation():
    # Test valid GPU
    with open("tests/examples/jobs/job_06_gpu.yaml", "r") as f:
        data = yaml.safe_load(f)

    spec = data["matching_specs"][0]
    spec["job_id"] = "test"

    job = Job.model_validate(spec)

    assert job.gpu.count.min == 1
    assert job.gpu.ram_mb == 8192


def test_job_system_glibc_optional():
    # Test job without glibc spec
    spec = {
        "job_id": "test",
        "system": {"name": "Linux"},
        "wall-time": 3600,
        "cpu-work": 1000,
        "cpu": {
            "num-cores": {"min": 1, "max": 1},
            "architecture": {"name": "x86_64", "microarchitecture-level": {"min": 1}}},
        "tags": ""
    }

    job = Job.model_validate(spec)

    assert job.system.glibc is None
