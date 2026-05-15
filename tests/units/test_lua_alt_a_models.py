#!/usr/bin/env python3

from __future__ import annotations

from glob import glob

import pytest

from matchmaking.models.lua.alt_a.job import Job


@pytest.mark.parametrize("job_file", sorted(glob("tests/examples/jobs/job_*.yaml")))
def test_job_to_redis_hash(job_file):
    """Ensure that the job can be converted to a redis hash correctly."""
    job = Job.load_from_yaml(job_file)

    # Only process entries that actually have specs
    if not job.matching_specs:
        return

    # We just ensure it runs and returns a dictionary of strings to strings
    redis_hash = job.to_redis_hash()

    assert isinstance(redis_hash, dict)

    for k, v in redis_hash.items():
        assert isinstance(k, str)
        assert isinstance(v, str)


def test_job_to_redis_hash_detailed():
    job_path = "tests/examples/jobs/job_01_mcsimulation_any_site.yaml"
    job = Job.load_from_yaml(job_path)

    redis_hash = job.to_redis_hash()

    assert "system_name" in redis_hash
    assert "cpu_num_cores_min" in redis_hash
    assert "cpu_architecture_name" in redis_hash
    assert "cpu_architecture_microarchitecture_level_min" in redis_hash

    # Inject cpu max level
    job.matching_specs[0].cpu.architecture.microarchitecture_level.max = 5
    redis_hash_cpu_max = job.to_redis_hash()

    assert "cpu_architecture_microarchitecture_level_max" in redis_hash_cpu_max

    # Check gpu attributes if testing a gpu job
    gpu_job_path = "tests/examples/jobs/job_06_gpu.yaml"
    gpu_job = Job.load_from_yaml(gpu_job_path)

    gpu_redis_hash = gpu_job.to_redis_hash()

    assert "gpu_count_min" in gpu_redis_hash
    assert "gpu_ram_mb" in gpu_redis_hash
    assert "gpu_vendor" in gpu_redis_hash
    assert "gpu_compute_capability_min" in gpu_redis_hash

    # Inject gpu max compute capability
    gpu_job.matching_specs[0].gpu.compute_capability.max = "9.0"
    gpu_redis_hash_max = gpu_job.to_redis_hash()

    assert "gpu_compute_capability_max" in gpu_redis_hash_max
