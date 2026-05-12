#!/usr/bin/env python3

from __future__ import annotations

from datetime import timedelta

from matchmaking.core.scheduler import select_job
from matchmaking.models.utils import JobGroup, JobOwner, JobType


def create_mock_job(load_job, job_id, owner, group, job_type, submission_time):
    job = load_job("job_01_mcsimulation_any_site")
    job.job_id = job_id
    job.owner = owner
    job.group = group
    job.job_type = job_type
    job.submission_time = submission_time

    return job


def test_integration_fair_distribution_round_robin_across_owners(example_config, base_time, load_job, load_node):
    """Simulates repeated calls to verify fair distribution."""
    queue = []
    node = load_node("node_01_cern_typical")

    for i in range(20):
        queue.append(
            create_mock_job(
                load_job,
                job_id=f"lbprods-{i}",
                owner=JobOwner.LBPRODS,
                group=JobGroup.LHCB_MC,
                job_type=JobType.MCSIMULATION,
                submission_time=base_time,
            )
        )

    for i in range(5):
        queue.append(
            create_mock_job(
                load_job,
                job_id=f"jdoe-{i}",
                owner="jdoe",
                group=JobGroup.LHCB_USER,
                job_type=JobType.MCSIMULATION,
                submission_time=base_time,
            )
        )

    selected_owners_order = []

    while queue:
        job = select_job(node, queue, example_config)

        assert job is not None

        selected_owners_order.append(job.owner)
        queue.remove(job)

    assert selected_owners_order[:5] == ["jdoe", "jdoe", "jdoe", "jdoe", "jdoe"]
    assert selected_owners_order[5:] == [JobOwner.LBPRODS] * 20


def test_integration_type_priority_overrides_fair_share(example_config, base_time, load_job, load_node):
    """Verifies that JobType priority overrides the fair-share running count."""
    node = load_node("node_01_cern_typical")

    queue = [
        create_mock_job(
            load_job,
            job_id="high-prio-lbprods",
            owner=JobOwner.LBPRODS,
            group=JobGroup.LHCB_MC,
            job_type=JobType.WGPRODUCTION,  # Highest priority
            submission_time=base_time,
        ),
        create_mock_job(
            load_job,
            job_id="low-prio-jdoe",
            owner="jdoe",
            group=JobGroup.LHCB_USER,
            job_type=JobType.MCSIMULATION,
            submission_time=base_time,
        ),
    ]

    job1 = select_job(node, queue, example_config)

    assert job1 is not None
    assert job1.job_id == "high-prio-lbprods"


def test_integration_dynamic_limits_stop_scheduling(example_config, base_time, load_job, load_node):
    """Simulates a queue of jobs filling up until it dynamically hits a site limit."""
    node = load_node("node_01_cern_typical")

    queue = []
    for i in range(25):
        queue.append(
            create_mock_job(
                load_job,
                job_id=f"user-job-{i}",
                owner=JobOwner.LBPRODS,
                group=JobGroup.LHCB_USER,
                job_type=JobType.USER,
                submission_time=base_time,
            )
        )

    job = select_job(node, queue, example_config)

    assert job is None

    job = select_job(node, queue[:19], example_config)

    assert job is not None


def test_integration_fifo_tiebreaker_same_counts(example_config, base_time, load_job, load_node):
    """In case of a perfect tie, the oldest job (FIFO) must win."""
    node = load_node("node_01_cern_typical")

    queue = [
        create_mock_job(
            load_job,
            job_id="new-job",
            owner="alice",
            group=JobGroup.LHCB_USER,
            job_type=JobType.USER,
            submission_time=base_time,
        ),
        create_mock_job(
            load_job,
            job_id="old-job",
            owner="bob",
            group=JobGroup.LHCB_USER,
            job_type=JobType.USER,
            submission_time=base_time - timedelta(hours=2),
        ),
    ]

    job = select_job(node, queue, example_config)

    assert job is not None
    assert job.job_id == "old-job"
