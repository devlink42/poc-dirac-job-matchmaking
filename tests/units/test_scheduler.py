#!/usr/bin/env python3

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

import pytest

from matchmaking.core.scheduler import CONFIG_PATH, select_job
from matchmaking.models.config import SchedulingConfig, Site
from matchmaking.models.utils import JobStatus, Type


def test_select_job_respects_site_limits(example_config, load_job, load_node):
    node = load_node("node_01_cern_typical")

    job = load_job("job_04_wgproduction_with_ram")

    job.status = JobStatus.WAITING

    running_job = job.model_copy()
    running_job.status = JobStatus.RUNNING
    candidate_jobs = [running_job] * 1000 + [job]

    with (
        patch("matchmaking.core.scheduler.Path.glob") as mock_glob,
        patch("matchmaking.models.job.Job.load_from_yaml") as mock_load_job,
        patch("matchmaking.models.config.SchedulingConfig.load_from_yaml", return_value=example_config),
    ):
        mock_glob.return_value = [Path(f"job_{i}.yaml") for i in range(len(candidate_jobs))]
        mock_load_job.side_effect = candidate_jobs

        selected = select_job(node)

    assert selected is None

    candidate_jobs = [running_job] * 999 + [job]

    with (
        patch("matchmaking.core.scheduler.Path.glob") as mock_glob,
        patch("matchmaking.models.job.Job.load_from_yaml") as mock_load_job,
        patch("matchmaking.models.config.SchedulingConfig.load_from_yaml", return_value=example_config),
    ):
        mock_glob.return_value = [Path(f"job_{i}.yaml") for i in range(len(candidate_jobs))]
        mock_load_job.side_effect = candidate_jobs

        selected = select_job(node)

    assert selected == job


def test_select_job_respects_default_limits_fallback(example_config, load_job, load_node):
    node = load_node("node_02_tier2_older")

    job = load_job("job_05_user_with_banned_site")  # Type User

    job.status = JobStatus.WAITING

    running_job = job.model_copy()
    running_job.status = JobStatus.RUNNING
    candidate_jobs = [running_job] * 200 + [job]

    with (
        patch("matchmaking.core.scheduler.Path.glob") as mock_glob,
        patch("matchmaking.models.job.Job.load_from_yaml") as mock_load_job,
        patch("matchmaking.models.config.SchedulingConfig.load_from_yaml", return_value=example_config),
    ):
        mock_glob.return_value = [Path(f"job_{i}.yaml") for i in range(len(candidate_jobs))]
        mock_load_job.side_effect = candidate_jobs

        selected = select_job(node)

    assert selected is None

    candidate_jobs = [running_job] * 199 + [job]

    with (
        patch("matchmaking.core.scheduler.Path.glob") as mock_glob,
        patch("matchmaking.models.job.Job.load_from_yaml") as mock_load_job,
        patch("matchmaking.models.config.SchedulingConfig.load_from_yaml", return_value=example_config),
    ):
        mock_glob.return_value = [Path(f"job_{i}.yaml") for i in range(len(candidate_jobs))]
        mock_load_job.side_effect = candidate_jobs

        selected = select_job(node)

    assert selected == job


def test_select_job_prioritizes_by_job_type(example_config, load_job, load_node):
    job_mc = load_job("job_01_mcsimulation_any_site")
    job_mc.job_id = "mc"
    job_mc.status = JobStatus.WAITING

    job_wg = load_job("job_04_wgproduction_with_ram")
    job_wg.job_id = "wg"
    job_wg.status = JobStatus.WAITING

    node = load_node("node_01_cern_typical")

    candidate_jobs = [job_mc, job_wg]

    with (
        patch("matchmaking.core.scheduler.Path.glob") as mock_glob,
        patch("matchmaking.models.job.Job.load_from_yaml") as mock_load_job,
        patch("matchmaking.models.config.SchedulingConfig.load_from_yaml", return_value=example_config),
    ):
        mock_glob.return_value = [Path(f"job_{i}.yaml") for i in range(len(candidate_jobs))]
        mock_load_job.side_effect = candidate_jobs

        selected = select_job(node)

    assert selected.job_id == "wg"


def test_select_job_tiebreaker_is_fifo(example_config, load_job, load_node):
    job_old = load_job("job_01_mcsimulation_any_site")
    job_old.job_id = "old"
    job_old.submit_time = job_old.submit_time - timedelta(hours=1)
    job_old.status = JobStatus.WAITING

    job_new = load_job("job_01_mcsimulation_any_site")
    job_new.job_id = "new"
    job_new.status = JobStatus.WAITING

    node = load_node("node_01_cern_typical")

    candidate_jobs = [job_new, job_old]

    with (
        patch("matchmaking.core.scheduler.Path.glob") as mock_glob,
        patch("matchmaking.models.job.Job.load_from_yaml") as mock_load_job,
        patch("matchmaking.models.config.SchedulingConfig.load_from_yaml", return_value=example_config),
    ):
        mock_glob.return_value = [Path(f"job_{i}.yaml") for i in range(len(candidate_jobs))]
        mock_load_job.side_effect = candidate_jobs

        selected = select_job(node)

    assert selected.job_id == "old"


def test_select_job_no_node_returns_none():
    assert select_job(None) is None


def test_select_job_job_path_not_found(example_config, load_node):
    node = load_node("node_01_cern_typical")

    with patch("matchmaking.core.scheduler.Path.exists", return_value=False):
        with pytest.raises(ValueError, match="Job examples not found at:"):
            select_job(node)


def test_select_job_load_jobs_exception(example_config, load_node):
    node = load_node("node_01_cern_typical")

    with patch("matchmaking.core.scheduler.Path.glob", side_effect=Exception("Unexpected error")):
        with pytest.raises(ValueError, match="Failed to load job examples: Unexpected error"):
            select_job(node)


def test_select_job_no_matching_jobs_returns_none(example_config, load_node):
    node = load_node("node_01_cern_typical")

    with (
        patch("matchmaking.core.scheduler.Path.glob") as mock_glob,
        patch("matchmaking.models.config.SchedulingConfig.load_from_yaml", return_value=example_config),
    ):
        mock_glob.return_value = []
        assert select_job(node) is None


def test_select_job_unknown_type_fallback(load_job, load_node):
    node = load_node("node_01_cern_typical")

    job_unknown = load_job("job_01_mcsimulation_any_site")
    job_unknown.job_type = "UNKNOWN"
    job_unknown.job_id = "unknown"
    job_unknown.status = JobStatus.WAITING

    job_older = load_job("job_01_mcsimulation_any_site")
    job_older.job_type = "OTHER"
    job_older.job_id = "older"
    job_older.submit_time = job_unknown.submit_time - timedelta(days=1)
    job_older.status = JobStatus.WAITING

    candidate_jobs = [job_unknown, job_older]
    with (
        patch("matchmaking.core.scheduler.Path.glob") as mock_glob,
        patch("matchmaking.models.job.Job.load_from_yaml") as mock_load_job,
    ):
        mock_glob.return_value = [Path(f"job_{i}.yaml") for i in range(len(candidate_jobs))]
        mock_load_job.side_effect = candidate_jobs

        selected = select_job(node)

    assert selected.job_id == "older"


def test_select_job_loads_default_config(load_job, load_node):
    node = load_node("node_01_cern_typical")

    job = load_job("job_01_mcsimulation_any_site")
    job.status = JobStatus.WAITING

    mock_config = SchedulingConfig(
        job_type_priorities=[job.job_type], by_site={node.site: Site(running_limits={job.job_type: 10}, name=node.site)}
    )

    with (
        patch("matchmaking.models.config.SchedulingConfig.load_from_yaml") as mock_load,
        patch("matchmaking.core.scheduler.Path.glob") as mock_glob,
        patch("matchmaking.models.job.Job.load_from_yaml") as mock_load_job,
    ):
        mock_load.return_value = mock_config
        mock_glob.return_value = [Path("job.yaml")]
        mock_load_job.return_value = job

        selected = select_job(node)

        assert selected == job

        mock_load.assert_called_once_with(CONFIG_PATH)


def test_select_job_default_config_not_found(load_job, load_node):
    node = load_node("node_01_cern_typical")

    with patch("matchmaking.models.config.SchedulingConfig.load_from_yaml") as mock_load:
        mock_load.side_effect = FileNotFoundError("File not found")

        with pytest.raises(ValueError, match=f"Scheduling config not found at: '{CONFIG_PATH}'"):
            select_job(node)


def test_select_job_default_config_load_error(load_job, load_node):
    node = load_node("node_01_cern_typical")

    job = load_job("job_01_mcsimulation_any_site")
    job.status = JobStatus.WAITING

    with (
        patch("matchmaking.core.scheduler.Path.glob") as mock_glob,
        patch("matchmaking.models.job.Job.load_from_yaml", return_value=job),
        patch("matchmaking.models.config.SchedulingConfig.load_from_yaml") as mock_load,
    ):
        mock_glob.return_value = [Path("job.yaml")]
        mock_load.side_effect = RuntimeError("invalid config")

        with pytest.raises(ValueError, match="Failed to load scheduling config: invalid config"):
            select_job(node)


def test_select_job_skips_weighted_priority_without_relevant_types(load_job, load_node):
    node = load_node("node_01_cern_typical")

    job = load_job("job_01_mcsimulation_any_site")
    job.status = JobStatus.WAITING

    mock_config = SchedulingConfig(
        job_type_priorities=[{Type.USER: 1}, Type.MCSIMULATION], by_site={node.site: Site(name=node.site)}
    )

    with (
        patch("matchmaking.models.config.SchedulingConfig.load_from_yaml", return_value=mock_config),
        patch("matchmaking.core.scheduler.Path.glob") as mock_glob,
        patch("matchmaking.models.job.Job.load_from_yaml", return_value=job),
    ):
        mock_glob.return_value = [Path("job.yaml")]

        selected = select_job(node)

    assert selected == job


def test_select_job_weighted_priority(load_job, load_node):
    node = load_node("node_01_cern_typical")

    job_mc = load_job("job_01_mcsimulation_any_site")
    job_mc.job_id = "mc"
    job_mc.status = JobStatus.WAITING
    job_mc.job_type = "MCSimulation"

    job_user = load_job("job_01_mcsimulation_any_site")
    job_user.job_id = "user"
    job_user.status = JobStatus.WAITING
    job_user.job_type = "User"

    mock_config = SchedulingConfig(
        job_type_priorities=[{Type.MCSIMULATION: 100, Type.USER: 0}], by_site={node.site: Site(name=node.site)}
    )

    with (
        patch("matchmaking.models.config.SchedulingConfig.load_from_yaml", return_value=mock_config),
        patch("matchmaking.core.scheduler.Path.glob") as mock_glob,
        patch("matchmaking.models.job.Job.load_from_yaml") as mock_load_job,
    ):
        mock_glob.return_value = [Path("job1.yaml"), Path("job2.yaml")]
        mock_load_job.side_effect = [job_mc, job_user]

        selected = select_job(node)

        assert selected.job_id == "mc"

    mock_config.job_type_priorities = [{Type.MCSIMULATION: 0, Type.USER: 100}]

    with (
        patch("matchmaking.models.config.SchedulingConfig.load_from_yaml", return_value=mock_config),
        patch("matchmaking.core.scheduler.Path.glob") as mock_glob,
        patch("matchmaking.models.job.Job.load_from_yaml") as mock_load_job,
    ):
        mock_glob.return_value = [Path("job1.yaml"), Path("job2.yaml")]
        mock_load_job.side_effect = [job_mc, job_user]

        selected = select_job(node)

        assert selected.job_id == "user"
