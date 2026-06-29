#!/usr/bin/env python3

from __future__ import annotations

from unittest.mock import MagicMock

import yaml

from matchmaking.core.py_redis.scheduler import fetch_candidate_jobs
from matchmaking.models.job import Job

JOB_01_PATH = "tests/examples/jobs/job_01_mcsimulation_any_site.yaml"


def test_fetch_candidate_jobs_empty():
    redis_mock = MagicMock()
    redis_mock.hrandfield.return_value = []

    jobs = fetch_candidate_jobs(redis_mock, 100)

    assert jobs == []
    redis_mock.hrandfield.assert_called_once()
    redis_mock.hmget.assert_not_called()


def test_fetch_candidate_jobs_success_and_malformed(caplog):
    redis_mock = MagicMock()
    redis_mock.hrandfield.return_value = ["job1", "job2", "job3", "job4"]

    with open(JOB_01_PATH) as f:
        job = Job.model_validate(yaml.safe_load(f))

    valid_job_json = job.model_dump_json()

    redis_mock.hmget.return_value = [valid_job_json, None, '{"invalid": "data"}', "not json"]

    jobs = fetch_candidate_jobs(redis_mock, 10)

    assert len(jobs) == 1
    assert jobs[0].job_id == job.job_id
    assert "Skipping malformed job payload in Redis" in caplog.text
