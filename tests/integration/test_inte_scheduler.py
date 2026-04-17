#!/usr/bin/env python3

from __future__ import annotations

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
