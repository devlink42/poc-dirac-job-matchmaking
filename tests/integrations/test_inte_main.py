#!/usr/bin/env python3

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

from matchmaking.core.main import select_job
from matchmaking.models.utils import JobStatus, Type


def create_mock_job(load_job, job_id, owner, group, type, submit_time, status=JobStatus.WAITING):
    job = load_job("job_01_mcsimulation_any_site")
    job.job_id = job_id
    job.owner = owner
    job.group = group
    job.type = type
    job.submit_time = submit_time
    job.status = status

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
                owner="lbprods",
                group="lhcb_mc",
                type=Type.MCSIMULATION,
                submit_time=base_time,
            )
        )

    for i in range(5):
        queue.append(
            create_mock_job(
                load_job,
                job_id=f"jdoe-{i}",
                owner="jdoe",
                group="lhcb_mc",
                type=Type.MCSIMULATION,
                submit_time=base_time,
            )
        )

    selected_owners_order = []
    running_jobs = []

    while queue:
        with (
            patch("matchmaking.core.utils.Path.glob") as mock_glob,
            patch("matchmaking.models.job.Job.load_from_yaml") as mock_load_job,
        ):
            all_jobs = running_jobs + queue
            mock_glob.return_value = [Path(f"job_{i}.yaml") for i in range(len(all_jobs))]
            mock_load_job.side_effect = all_jobs

            with patch("matchmaking.models.config.SchedulingConfig.load_from_yaml", return_value=example_config):
                job = select_job(node)

        assert job is not None

        selected_owners_order.append(job.owner)
        queue.remove(job)

        job.status = JobStatus.RUNNING
        running_jobs.append(job)

    jdoe_count = selected_owners_order.count("jdoe")
    lbprods_count = selected_owners_order.count("lbprods")

    assert jdoe_count == 5
    assert lbprods_count == 20
    assert "jdoe" in selected_owners_order[:10]


def test_integration_type_priority_overrides_fair_share(example_config, base_time, load_job, load_node):
    """Verifies that JobType priority overrides the fair-share running count."""
    node = load_node("node_01_cern_typical")

    queue = [
        create_mock_job(
            load_job,
            job_id="high-prio-lbprods",
            owner="lbprods",
            group="lhcb_mc",
            type=Type.WGPRODUCTION,  # Highest priority
            submit_time=base_time,
        ),
        create_mock_job(
            load_job,
            job_id="low-prio-jdoe",
            owner="jdoe",
            group="lhcb_user",
            type=Type.MCSIMULATION,
            submit_time=base_time,
        ),
    ]

    job1 = None

    with (
        patch("matchmaking.core.utils.Path.glob") as mock_glob,
        patch("matchmaking.models.job.Job.load_from_yaml") as mock_load_job,
        patch("matchmaking.models.config.SchedulingConfig.load_from_yaml", return_value=example_config),
    ):
        mock_glob.return_value = [Path(f"job_{i}.yaml") for i in range(len(queue))]
        mock_load_job.side_effect = queue
        job1 = select_job(node)

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
                owner="lbprods",
                group="lhcb_user",
                type=Type.USER,
                submit_time=base_time,
            )
        )

    job = None

    with (
        patch("matchmaking.core.utils.Path.glob") as mock_glob,
        patch("matchmaking.models.job.Job.load_from_yaml") as mock_load_job,
        patch("matchmaking.models.config.SchedulingConfig.load_from_yaml", return_value=example_config),
    ):
        mock_glob.return_value = [Path(f"job_{i}.yaml") for i in range(len(queue))]

        for i, j in enumerate(queue):
            if i < 20:
                j.status = JobStatus.RUNNING
                j.matching_specs[0].site = node.site
            else:
                j.status = JobStatus.WAITING

        mock_load_job.side_effect = queue
        job = select_job(node)

    assert job is None

    job = None

    with (
        patch("matchmaking.core.utils.Path.glob") as mock_glob,
        patch("matchmaking.models.job.Job.load_from_yaml") as mock_load_job,
        patch("matchmaking.models.config.SchedulingConfig.load_from_yaml", return_value=example_config),
    ):
        q = queue[:19]
        mock_glob.return_value = [Path(f"job_{i}.yaml") for i in range(len(q))]

        for i, j in enumerate(q):
            j.status = JobStatus.RUNNING if i < 18 else JobStatus.WAITING

        mock_load_job.side_effect = q
        job = select_job(node)

    assert job is not None


def test_integration_fifo_tiebreaker_same_counts(example_config, base_time, load_job, load_node):
    """In case of a perfect tie, the oldest job (FIFO) must win."""
    node = load_node("node_01_cern_typical")

    queue = [
        create_mock_job(
            load_job,
            job_id="new-job",
            owner="alice",
            group="lhcb_user",
            type=Type.USER,
            submit_time=base_time,
        ),
        create_mock_job(
            load_job,
            job_id="old-job",
            owner="bob",
            group="lhcb_user",
            type=Type.USER,
            submit_time=base_time - timedelta(hours=2),
        ),
    ]

    job = None

    with (
        patch("matchmaking.core.utils.Path.glob") as mock_glob,
        patch("matchmaking.models.job.Job.load_from_yaml") as mock_load_job,
        patch("matchmaking.models.config.SchedulingConfig.load_from_yaml", return_value=example_config),
    ):
        mock_glob.return_value = [Path(f"job_{i}.yaml") for i in range(len(queue))]
        mock_load_job.side_effect = queue
        job = select_job(node)

    assert job is not None
    assert job.job_id == "old-job"
