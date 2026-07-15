#!/usr/bin/env python3
"""Tests for the indexed in-memory job selector."""

from __future__ import annotations

import random
from datetime import timedelta
from unittest.mock import patch

from matchmaking.logic.indexed_selector import IndexedJobSelector
from matchmaking.models.config import SchedulingConfig, Site
from matchmaking.models.utils import JobStatus, Type


def test_indexed_selector_preserves_fifo_and_can_release(load_job, load_node, example_config):
    """Select the oldest compatible job and restore it without rebuilding the index."""
    node = load_node("node_01_cern_typical")
    newer = load_job("job_01_mcsimulation_any_site")
    older = newer.model_copy(deep=True)
    newer.job_id = "newer"
    older.job_id = "older"
    older.submit_time -= timedelta(hours=1)
    selector = IndexedJobSelector([newer, older], example_config)

    selected = selector.select(node)

    assert selected is older
    assert selected.status == JobStatus.RUNNING
    assert selected.assigned_site == node.site


def test_indexed_selector_preserves_fairshare(load_job, load_node, example_config):
    """Prefer a compatible job from the group with fewer running jobs."""
    node = load_node("node_01_cern_typical")
    busy = load_job("job_01_mcsimulation_any_site")
    idle = busy.model_copy(deep=True)
    running = busy.model_copy(deep=True)
    busy.job_id = "busy"
    busy.owner = "alice"
    busy.group = "busy-group"
    idle.job_id = "idle"
    idle.owner = "bob"
    idle.group = "idle-group"
    running.status = JobStatus.RUNNING
    running.owner = busy.owner
    running.group = busy.group
    running.assigned_site = node.site

    selected = IndexedJobSelector([running, busy, idle], example_config).select(node)

    assert selected is idle


def test_indexed_selector_respects_site_limit(load_job, load_node):
    """Exclude a type whose running limit has been reached at the requested site."""
    node = load_node("node_01_cern_typical")
    waiting = load_job("job_01_mcsimulation_any_site")
    running = waiting.model_copy(deep=True)
    running.status = JobStatus.RUNNING
    running.assigned_site = node.site
    config = SchedulingConfig(
        job_type_priorities=[waiting.type],
        by_site={node.site: Site(name=node.site, running_limits={waiting.type: 1})},
    )

    assert IndexedJobSelector([running, waiting], config).select(node) is None


def test_indexed_selector_does_not_scan_the_candidate_pool(load_job, load_node):
    """Keep matching work independent from the total pool when its FIFO head matches."""
    node = load_node("node_01_cern_typical")
    first = load_job("job_01_mcsimulation_any_site")
    jobs = [first]
    for index in range(10_000):
        job = first.model_copy(deep=False)
        job.job_id = f"job-{index}"
        job.submit_time += timedelta(seconds=index + 1)
        jobs.append(job)

    config = SchedulingConfig(job_type_priorities=[Type.MCSIMULATION])
    with patch("matchmaking.logic.indexed_selector.is_matching", return_value=True) as matching:
        selected = IndexedJobSelector(jobs, config).select(node)

    assert selected is first
    matching.assert_called_once_with(first, node)


def test_indexed_selector_supports_weighted_priorities(load_job, load_node):
    """Choose only between compatible types in a weighted priority level."""
    node = load_node("node_01_cern_typical")
    simulation = load_job("job_01_mcsimulation_any_site")
    user = simulation.model_copy(deep=True)
    user.type = Type.USER
    config = SchedulingConfig(
        job_type_priorities=[
            {Type.MERGE: 100},
            {Type.MCSIMULATION: 0, Type.USER: 100},
        ]
    )

    selected = IndexedJobSelector([simulation, user], config).select(node, random.Random(0))  # noqa: S311

    assert selected is user


def test_indexed_selector_falls_back_and_handles_no_match(load_job, load_node):
    """Use unlisted types as fallback and return None if none are compatible."""
    node = load_node("node_01_cern_typical")
    job = load_job("job_01_mcsimulation_any_site")
    config = SchedulingConfig(job_type_priorities=[Type.MERGE])
    selector = IndexedJobSelector([job], config)

    assert selector.select(node) is job

    with patch("matchmaking.logic.indexed_selector.is_matching", return_value=False):
        assert selector.select(node) is None
