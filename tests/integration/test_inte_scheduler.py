#!/usr/bin/env python3

from __future__ import annotations

from datetime import timedelta

from src.core.scheduler import select_job
from src.models.job import Job
from src.models.utils import JobGroup, JobOwner, JobType


def test_integration_fair_distribution_round_robin_across_owners(config, base_time):
    """Simulates repeated calls to verify fair distribution."""
    queue = []

    for i in range(20):
        queue.append(
            Job.model_construct(
                job_id=f"lbprods_{i}",
                owner=JobOwner.LBPRODS,
                group=JobGroup.LHCB_MC,
                job_type=JobType.MCSIMULATION,
                submission_time=base_time,
            )
        )

    for i in range(5):
        queue.append(
            Job.model_construct(
                job_id=f"jdoe_{i}",
                owner="jdoe",
                group=JobGroup.LHCB_USER,
                job_type=JobType.MCSIMULATION,
                submission_time=base_time,
            )
        )

    running_by_owner = {JobOwner.LBPRODS: 1000, "jdoe": 10}
    running_by_group = {JobGroup.LHCB_MC: 1000, JobGroup.LHCB_USER: 10}
    running_by_site_and_type = {}

    selected_owners_order = []

    while queue:
        job = select_job(queue, "LCG.CERN.ch", running_by_site_and_type, running_by_owner, running_by_group, config)
        assert job is not None

        selected_owners_order.append(job.owner)
        queue.remove(job)

        running_by_owner[job.owner] += 1
        running_by_group[job.group] += 1

    assert selected_owners_order[:5] == ["jdoe", "jdoe", "jdoe", "jdoe", "jdoe"]
    assert selected_owners_order[5:] == [JobOwner.LBPRODS] * 20


def test_integration_type_priority_overrides_fair_share(config, base_time):
    """Verifies that JobType priority overrides the fair-share running count.
    Even if lbprods has 1000 running jobs, submitting a WGPRODUCTION job (highest priority)
    should be scheduled before a standard user submitting a MCSIMULATION job.
    """
    queue = [
        Job.model_construct(
            job_id="high_prio_lbprods",
            owner=JobOwner.LBPRODS,
            group=JobGroup.LHCB_MC,
            job_type=JobType.WGPRODUCTION,  # Highest priority in config
            submission_time=base_time,
        ),
        Job.model_construct(
            job_id="low_prio_jdoe",
            owner="jdoe",
            group=JobGroup.LHCB_USER,
            job_type=JobType.MCSIMULATION,
            submission_time=base_time,
        ),
    ]

    running_by_owner = {JobOwner.LBPRODS: 1000, "jdoe": 10}
    running_by_group = {JobGroup.LHCB_MC: 1000, JobGroup.LHCB_USER: 10}
    running_by_site_and_type = {}

    job1 = select_job(queue, "LCG.CERN.ch", running_by_site_and_type, running_by_owner, running_by_group, config)

    # WGPRODUCTION wins, despite lbprods monopolizing the cluster
    assert job1 is not None
    assert job1.job_id == "high_prio_lbprods"


def test_integration_dynamic_limits_stop_scheduling(config, base_time):
    """Simulates a queue of jobs filling up until it dynamically hits a site limit.
    The config defines a limit of 500 for WGPRODUCTION at LCG.CERN.ch.
    """
    queue = []
    for i in range(5):
        queue.append(
            Job.model_construct(
                job_id=f"wg_job_{i}",
                owner=JobOwner.LBPRODS,
                group=JobGroup.LHCB_MC,
                job_type=JobType.WGPRODUCTION,
                submission_time=base_time,
            )
        )

    running_by_owner = {}
    running_by_group = {}

    # We start with 498 running jobs. The limit is 500. Only 2 jobs should be scheduled.
    running_by_site_and_type = {"LCG.CERN.ch": {JobType.WGPRODUCTION: 498}}

    scheduled_jobs = []

    # Loop as long as the scheduler finds a valid job
    while queue:
        job = select_job(queue, "LCG.CERN.ch", running_by_site_and_type, running_by_owner, running_by_group, config)
        if job is None:
            break

        scheduled_jobs.append(job)
        queue.remove(job)

        # Real-time limit update (similar to the main logic)
        running_by_site_and_type["LCG.CERN.ch"][JobType.WGPRODUCTION] += 1

    # Only 2 jobs were scheduled, 3 remain stuck in the queue
    assert len(scheduled_jobs) == 2
    assert len(queue) == 3


def test_integration_fifo_tiebreaker_same_counts(config, base_time):
    """In case of a perfect tie (same job priority, same running counts for owner and group),
    the oldest job (FIFO) must win.
    """
    queue = [
        Job.model_construct(
            job_id="new_job",
            owner="alice",
            group=JobGroup.LHCB_USER,
            job_type=JobType.USER,
            submission_time=base_time,  # Newer job
        ),
        Job.model_construct(
            job_id="old_job",
            owner="bob",
            group=JobGroup.LHCB_USER,
            job_type=JobType.USER,
            submission_time=base_time - timedelta(hours=2),  # Older job (submitted 2 hours ago)
        ),
    ]

    # Alice and Bob have exactly the same usage
    running_by_owner = {"alice": 50, "bob": 50}
    running_by_group = {JobGroup.LHCB_USER: 100}
    running_by_site_and_type = {}

    job = select_job(queue, "LCG.CERN.ch", running_by_site_and_type, running_by_owner, running_by_group, config)

    assert job is not None
    assert job.job_id == "old_job"
