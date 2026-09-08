#!/usr/bin/env python3
"""Unit tests for the Alternative C Redis seeder."""

from __future__ import annotations

from unittest.mock import Mock

import pytest
import redis

from matchmaking.logic.lua.alt_c.redis_seeder import RedisSeeder
from matchmaking.models.utils import JobStatus


class RecordingPipeline:
    def __init__(self, *, exists=False, fail=False):
        self.exists_result = exists
        self.fail = fail
        self.calls = []

    def watch(self, key):
        self.calls.append(("watch", key))

    def exists(self, key):
        self.calls.append(("exists", key))
        return self.exists_result

    def multi(self):
        self.calls.append(("multi",))

    def sadd(self, key, value):
        self.calls.append(("sadd", key, value))

    def hset(self, key, mapping):
        self.calls.append(("hset", key, mapping))

    def lpush(self, key, value):
        self.calls.append(("lpush", key, value))

    def execute(self):
        self.calls.append(("execute",))
        if self.fail:
            self.fail = False
            raise redis.WatchError("conflict")

    def reset(self):
        self.calls.append(("reset",))


class RecordingRedis:
    def __init__(self, pipelines):
        self.pipelines = iter(pipelines)
        self.created = []

    def pipeline(self, transaction=True):
        assert transaction is True
        pipeline = next(self.pipelines)
        self.created.append(pipeline)
        return pipeline


@pytest.mark.parametrize(
    "kwargs",
    [
        {"memory_bucket_mb": 0},
        {"cpu_work_bucket": True},
        {"wall_time_bucket_seconds": -1},
        {"core_bucket": 0},
        {"watch_retries": False},
        {"known_sites": [""]},
        {"known_sites": [1]},
    ],
)
def test_seeder_rejects_invalid_configuration(kwargs):
    with pytest.raises(ValueError):
        RedisSeeder(Mock(), **kwargs)


def test_build_requirement_group_normalizes_any_site_and_gpu(load_job):
    job = load_job("job_06_gpu")
    job.matching_specs[0].site = None

    group = RedisSeeder(
        Mock(),
        known_sites=["site-b", "site-a"],
        memory_bucket_mb=1000,
        cpu_work_bucket=1000,
        wall_time_bucket_seconds=1000,
        core_bucket=1,
    ).build_requirement_group(job)

    assert group.eligible_sites == ("site-a", "site-b")
    assert group.min_cpu_cores == 1
    assert group.max_cpu_cores == 4
    assert group.min_ram_mb == 8000
    assert group.has_gpu is True
    assert group.min_gpu_count == group.max_gpu_count == 1
    assert group.min_gpu_ram_mb == 9000


def test_build_requirement_group_rejects_invalid_jobs(load_job):
    seeder = RedisSeeder(Mock(), known_sites=["site-a"])

    job = load_job("job_01_mcsimulation_any_site")
    job.job_id = None
    with pytest.raises(ValueError, match="job_id"):
        seeder.build_requirement_group(job)

    job = load_job("job_01_mcsimulation_any_site")
    job.status = JobStatus.RUNNING
    with pytest.raises(ValueError, match="waiting"):
        seeder.build_requirement_group(job)

    job = load_job("job_01_mcsimulation_any_site")
    with pytest.raises(ValueError, match="known_sites"):
        RedisSeeder(Mock()).build_requirement_group(job)

    job = load_job("job_02_mcsimulation_multi_site")
    job.matching_specs[1].cpu_work += 1
    with pytest.raises(ValueError, match="differ only by site"):
        seeder.build_requirement_group(job)

    job = load_job("job_01_mcsimulation_any_site")
    job.matching_specs[0].site = ""
    with pytest.raises(ValueError, match="non-empty sites"):
        seeder.build_requirement_group(job)

    job = load_job("job_01_mcsimulation_any_site")
    with pytest.raises(ValueError, match="CPU range"):
        RedisSeeder(Mock(), known_sites=["site-a"], core_bucket=2).build_requirement_group(job)

    job = load_job("job_06_gpu")
    with pytest.raises(ValueError, match="GPU range"):
        RedisSeeder(Mock(), known_sites=["site-a"], core_bucket=2).build_requirement_group(job)

    job = load_job("job_01_mcsimulation_any_site")
    job.matching_specs[0].cpu.ram_mb = None
    group = RedisSeeder(Mock(), known_sites=["site-a"]).build_requirement_group(job)
    assert group.min_ram_mb == 0


def test_seed_job_writes_indexes_hash_and_queue(load_job):
    job = load_job("job_02_mcsimulation_multi_site")
    pipeline = RecordingPipeline()
    redis_client = RecordingRedis([pipeline])
    seeder = RedisSeeder(redis_client)

    group_id = seeder.seed_job(job)

    assert group_id == seeder.build_requirement_group(job).req_group_id
    operations = [call[0] for call in pipeline.calls]
    assert operations.count("sadd") == len(job.matching_specs)
    assert "hset" in operations
    assert ("lpush", f"queue:{group_id}", job.job_id) in pipeline.calls


def test_seed_job_skips_existing_hash_and_retries_watch_conflict(load_job):
    job = load_job("job_06_gpu")
    first = RecordingPipeline(fail=True)
    second = RecordingPipeline(exists=True)
    redis_client = RecordingRedis([first, second])

    group_id = RedisSeeder(redis_client, watch_retries=2).seed_job(job)

    assert group_id
    assert ("hset",) not in [call[:1] for call in second.calls]
    assert ("reset",) in first.calls
    assert ("reset",) in second.calls

    failing = RecordingRedis([RecordingPipeline(fail=True), RecordingPipeline(fail=True)])
    with pytest.raises(redis.WatchError, match="after 2 attempts"):
        RedisSeeder(failing, watch_retries=2).seed_job(job)
