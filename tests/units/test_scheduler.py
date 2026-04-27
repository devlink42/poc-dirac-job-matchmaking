#!/usr/bin/env python3

from __future__ import annotations

from datetime import timedelta
from unittest.mock import Mock

from matchmaking.core.scheduler import select_job
from matchmaking.models.utils import JobType


def test_select_job_respects_site_limits(example_config, load_job, load_node):
    job = load_job("job_04_wgproduction_with_ram")
    node = load_node("node_01_cern_typical")

    # Limit for WGProduction at CERN is 1000
    candidate_jobs = [job] * 1001
    selected = select_job(node, candidate_jobs, example_config)
    assert selected is None

    candidate_jobs = [job] * 999
    selected = select_job(node, candidate_jobs, example_config)
    assert selected == job


def test_select_job_respects_default_limits_fallback(example_config, load_job, load_node):
    job = load_job("job_05_user_with_banned_site")  # Type User
    node = load_node("node_02_tier2_older")

    # Default limit for User is 200
    candidate_jobs = [job] * 201
    selected = select_job(node, candidate_jobs, example_config)
    assert selected is None

    candidate_jobs = [job] * 199
    selected = select_job(node, candidate_jobs, example_config)
    assert selected == job


def test_select_job_prioritizes_by_job_type(example_config, load_job, load_node):
    # WGProduction (priority 0) vs MCSimulation (priority 1)
    job_mc = load_job("job_01_mcsimulation_any_site")
    job_mc.job_id = "mc"

    job_wg = load_job("job_04_wgproduction_with_ram")
    job_wg.job_id = "wg"

    node = load_node("node_01_cern_typical")

    # WGProduction should be selected before MCSimulation
    selected = select_job(node, [job_mc, job_wg], example_config)
    assert selected.job_id == "wg"


def test_select_job_tiebreaker_is_fifo(example_config, load_job, load_node):
    job_old = load_job("job_01_mcsimulation_any_site")
    job_old.job_id = "old"
    job_old.submission_time = job_old.submission_time - timedelta(hours=1)

    job_new = load_job("job_01_mcsimulation_any_site")
    job_new.job_id = "new"

    node = load_node("node_01_cern_typical")

    selected = select_job(node, [job_new, job_old], example_config)
    assert selected.job_id == "old"


def test_select_job_no_matching_jobs_returns_none(example_config, load_node):
    node = load_node("node_01_cern_typical")
    assert select_job(node, [], example_config) is None


def test_select_job_unknown_type_fallback(load_job, load_node):
    config = Mock()
    config.running_limits = {"default": {}}
    config.job_type_priorities = [JobType.WGPRODUCTION]

    job_unknown = load_job("job_01_mcsimulation_any_site")
    job_unknown.job_type = "UNKNOWN"
    job_unknown.job_id = "unknown"

    job_older = load_job("job_01_mcsimulation_any_site")
    job_older.job_type = "OTHER"
    job_older.job_id = "older"
    job_older.submission_time = job_unknown.submission_time - timedelta(days=1)

    node = load_node("node_01_cern_typical")

    selected = select_job(node, [job_unknown, job_older], config)
    assert selected.job_id == "older"
