#!/usr/bin/env python3

from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import Mock

from src.core.scheduler import select_job
from src.models.job import Job
from src.models.utils import JobGroup, JobType


def test_select_job_respects_site_limits(config, base_time):
    job = Job.model_construct(
        job_id="job1", owner="user1", group=JobGroup.LHCB_MC, job_type=JobType.WGPRODUCTION, submission_time=base_time
    )

    running = {"LCG.CERN.ch": {JobType.WGPRODUCTION: 500}}
    selected = select_job([job], "LCG.CERN.ch", running, {}, {}, config)
    assert selected is None

    running = {"LCG.CERN.ch": {JobType.WGPRODUCTION: 499}}
    selected = select_job([job], "LCG.CERN.ch", running, {}, {}, config)
    assert selected == job


def test_select_job_respects_default_limits_fallback(config, base_time):
    job = Job.model_construct(
        job_id="job1", owner="user1", group=JobGroup.LHCB_USER, job_type=JobType.USER, submission_time=base_time
    )

    running = {"LCG.IN2P3.fr": {JobType.USER: 200}}
    selected = select_job([job], "LCG.IN2P3.fr", running, {}, {}, config)
    assert selected is None


def test_select_job_prioritizes_by_job_type(config, base_time):
    job_mc = Job.model_construct(
        job_id="mc", owner="user1", group=JobGroup.LHCB_MC, job_type=JobType.MCSIMULATION, submission_time=base_time
    )
    job_wg = Job.model_construct(
        job_id="wg", owner="user1", group=JobGroup.LHCB_MCPROC, job_type=JobType.WGPRODUCTION, submission_time=base_time
    )

    selected = select_job([job_mc, job_wg], "LCG.CERN.ch", {}, {}, {}, config)
    assert selected == job_wg


def test_select_job_tiebreaker_is_fifo(config, base_time):
    job_new = Job.model_construct(
        job_id="new", owner="user1", group=JobGroup.LHCB_USER, job_type=JobType.USER, submission_time=base_time
    )
    job_old = Job.model_construct(
        job_id="old",
        owner="user1",
        group=JobGroup.LHCB_USER,
        job_type=JobType.USER,
        submission_time=base_time - timedelta(hours=1),
    )

    selected = select_job([job_new, job_old], "LCG.CERN.ch", {}, {}, {}, config)
    assert selected == job_old


def test_select_job_no_matching_jobs_returns_none():
    assert select_job([], "LCG.CERN.ch", {}, {}, {}, None) is None


def test_select_job_unknown_type_exceptions():
    config = Mock()
    config.running_limits = {"default": {}}
    config.job_type_priorities = ["TYPE_A", "TYPE_B"]

    job_unknown = Mock()
    job_unknown.job_type = "TYPE_UNKNOWN"
    job_unknown.group = JobGroup.LHCB_USER
    job_unknown.owner = "owner1"
    job_unknown.submission_time = datetime(2023, 1, 1, tzinfo=datetime.now().astimezone().tzinfo)

    job_older = Mock()
    job_older.job_type = "ANOTHER_UNKNOWN"
    job_older.group = JobGroup.LHCB_USER
    job_older.owner = "owner1"
    job_older.submission_time = datetime(2022, 1, 1, tzinfo=datetime.now().astimezone().tzinfo)

    matching_jobs = [job_unknown, job_older]

    selected = select_job(
        matching_jobs=matching_jobs,
        target_site="site1",
        running_by_site_and_type={},
        running_by_owner={},
        running_by_group={},
        config=config,
    )

    assert selected == job_older
