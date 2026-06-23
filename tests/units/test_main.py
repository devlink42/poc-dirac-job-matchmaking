#!/usr/bin/env python3

from __future__ import annotations

import random
from datetime import timedelta
from unittest.mock import patch

import pytest

from matchmaking.core.main import select_job
from matchmaking.models.config import SchedulingConfig, Site
from matchmaking.models.utils import JobStatus, Type


def test_select_job_respects_site_limits(example_config, load_job, load_node):
    """Test that a job is skipped if its job type has reached the maximum allowed limit for the node's site."""
    node = load_node("node_03_gpu")
    job = load_job("job_06_gpu")
    job.status = JobStatus.WAITING

    running_job = job.model_copy()
    running_job.status = JobStatus.RUNNING
    running_job.assigned_site = node.site

    # Explicitly set the limit for this job type to 500
    example_config.by_site[node.site] = Site(name=node.site, running_limits={job.type: 500})

    # Limit reached -> raises ValueError
    candidate_jobs_limit_reached = [running_job] * 500 + [job]
    with (
        patch("matchmaking.core.main.get_jobs", return_value=candidate_jobs_limit_reached),
        patch("matchmaking.core.main.get_selection_configuration", return_value=example_config),
    ):
        with pytest.raises(ValueError):
            select_job(node)

    # Limit not reached -> returns Job
    candidate_jobs_limit_ok = [running_job] * 499 + [job]
    with (
        patch("matchmaking.core.main.get_jobs", return_value=candidate_jobs_limit_ok),
        patch("matchmaking.core.main.get_selection_configuration", return_value=example_config),
    ):
        selected = select_job(node)
        assert selected is not None
        assert selected.job_id == job.job_id


@pytest.mark.parametrize(
    "priorities, expected_job_id",
    [
        # Sequential priority matches MCSIMULATION first
        ([Type.MCSIMULATION, Type.WGPRODUCTION], "mc"),
        # Sequential priority matches WGPRODUCTION first
        ([Type.WGPRODUCTION, Type.MCSIMULATION], "wg"),
        # Weighted priority heavily favors MCSIMULATION
        ([{Type.MCSIMULATION: 100, Type.USER: 0}], "mc"),
        # Weighted priority heavily favors USER
        ([{Type.MCSIMULATION: 0, Type.USER: 100}], "user"),
    ],
    ids=["sequential_mc_first", "sequential_wg_first", "weighted_mc_100", "weighted_user_100"],
)
def test_select_job_priority_handling(priorities: list, expected_job_id: str, load_job, load_node):
    """Test that the scheduler respects both sequential and weighted job type priorities."""
    node = load_node("node_01_cern_typical")

    # Define candidate jobs with varying types
    jobs = {
        "mc": Type.MCSIMULATION,
        "wg": Type.WGPRODUCTION,
        "user": Type.USER,
    }

    candidate_jobs = []
    for j_id, j_type in jobs.items():
        job = load_job("job_01_mcsimulation_any_site")
        job.job_id = j_id
        job.status = JobStatus.WAITING
        job.type = j_type
        candidate_jobs.append(job)

    mock_config = SchedulingConfig(job_type_priorities=priorities, by_site={node.site: Site(name=node.site)})

    # Use a deterministic random seed for robust weighted algorithm testing
    rng = random.Random(42)  # noqa: S311

    with (
        patch("matchmaking.core.main.get_jobs", return_value=candidate_jobs),
        patch("matchmaking.core.main.get_selection_configuration", return_value=mock_config),
    ):
        selected = select_job(node, rng=rng)

        assert selected is not None
        assert selected.job_id == expected_job_id


def test_select_job_tiebreaker_is_fifo(load_job, load_node, example_config):
    """Test that jobs with identical priorities are selected based on submission time (FIFO)."""
    node = load_node("node_01_cern_typical")

    job_old = load_job("job_01_mcsimulation_any_site")
    job_old.job_id = "old"
    job_old.submit_time = job_old.submit_time - timedelta(hours=1)
    job_old.status = JobStatus.WAITING

    job_new = load_job("job_01_mcsimulation_any_site")
    job_new.job_id = "new"
    job_new.status = JobStatus.WAITING

    with (
        patch("matchmaking.core.main.get_jobs", return_value=[job_new, job_old]),
        patch("matchmaking.core.main.get_selection_configuration", return_value=example_config),
    ):
        selected = select_job(node)

        assert selected is not None
        assert selected.job_id == "old"


def test_select_job_applies_round_robin_fairshare(load_job, load_node, example_config):
    """Test that round-robin fairshare prioritizes jobs from groups/owners with fewer currently running jobs."""
    node = load_node("node_01_cern_typical")

    job_a = load_job("job_01_mcsimulation_any_site")
    job_a.job_id = "job_owner_a"
    job_a.owner = "Alice"
    job_a.group = "Group1"
    job_a.status = JobStatus.WAITING

    job_b = load_job("job_01_mcsimulation_any_site")
    job_b.job_id = "job_owner_b"
    job_b.owner = "Charlie"
    job_b.group = "Group2"
    job_b.status = JobStatus.WAITING

    # Simulate 2 running jobs for Group1/Alice
    running_jobs = []
    for _ in range(2):
        r_job = job_a.model_copy()
        r_job.status = JobStatus.RUNNING
        running_jobs.append(r_job)

    candidate_jobs = running_jobs + [job_a, job_b]

    with (
        patch("matchmaking.core.main.get_jobs", return_value=candidate_jobs),
        patch("matchmaking.core.main.get_selection_configuration", return_value=example_config),
    ):
        # Because Group2 has fewer running jobs (0) than Group1 (2), Charlie's job should be selected first!
        selected = select_job(node)

        assert selected is not None
        assert selected.job_id == "job_owner_b"


def test_select_job_unknown_type_fallback(load_job, load_node, example_config):
    """Test that if priority lists do not match any available job types, it falls back to allowed jobs."""
    node = load_node("node_01_cern_typical")

    job_unknown = load_job("job_01_mcsimulation_any_site")
    job_unknown.job_id = "unknown"
    job_unknown.type = "UNKNOWN"
    job_unknown.status = JobStatus.WAITING

    job_older = load_job("job_01_mcsimulation_any_site")
    job_older.job_id = "older"
    job_older.type = "OTHER"
    job_older.submit_time = job_unknown.submit_time - timedelta(days=1)
    job_older.status = JobStatus.WAITING

    # Override config so priorities only explicitly target MCSIMULATION
    example_config.job_type_priorities = [Type.MCSIMULATION]

    with (
        patch("matchmaking.core.main.get_jobs", return_value=[job_unknown, job_older]),
        patch("matchmaking.core.main.get_selection_configuration", return_value=example_config),
    ):
        selected = select_job(node)

        assert selected is not None
        # It should fallback to all allowed jobs and pick the oldest one
        assert selected.job_id == "older"


def test_select_job_propagates_loader_exceptions(load_node):
    """Test that exceptions from loader functions propagate naturally."""
    node = load_node("node_01_cern_typical")

    with patch("matchmaking.core.main.get_jobs", side_effect=ValueError("Failed to load jobs")):
        with pytest.raises(ValueError, match="Failed to load jobs"):
            select_job(node)


def test_select_job_ignores_running_limits_from_other_sites(example_config, load_job, load_node):
    """Test that running jobs on Site B do not affect the job limits of Site A."""
    node_site_a = load_node("node_01_cern_typical")

    waiting_job = load_job("job_01_mcsimulation_any_site")
    waiting_job.status = JobStatus.WAITING

    # Create a running job on a DIFFERENT site
    running_job_other_site = waiting_job.model_copy(deep=True)
    running_job_other_site.status = JobStatus.RUNNING
    running_job_other_site.matching_specs[0].site = "LCG.CSCS.ch"

    # Explicitly set the limit for this job type to 1 for the requesting node's site
    example_config.by_site[node_site_a.site] = Site(name=node_site_a.site, running_limits={waiting_job.type: 1})

    # We mock 10 running jobs on "LCG.CSCS.ch".
    # If the limit was global, the limit of 1 for node_site_a would be falsely triggered.
    candidate_jobs = [running_job_other_site] * 10 + [waiting_job]

    with (
        patch("matchmaking.core.main.get_jobs", return_value=candidate_jobs),
        patch("matchmaking.core.main.get_selection_configuration", return_value=example_config),
    ):
        selected = select_job(node_site_a)

        # The job should be selected because the 10 running jobs are on another site.
        assert selected is not None
        assert selected.job_id == waiting_job.job_id


@pytest.mark.parametrize(
    "job_file, node_file, expected_selected, isolate_hardware",
    [
        # Perfect standard match
        ("job_01_mcsimulation_any_site", "node_01_cern_typical", True, False),
        # Site / Policy restrictions (Must evaluate geographic constraints natively)
        ("job_05_user_with_banned_site", "node_02_tier2_older", False, False),
        # RAM requirements
        ("job_10_ram_tests", "node_04_low_ram", False, True),
        ("job_10_ram_tests", "node_01_cern_typical", True, True),
        # Glibc / OS restrictions
        ("job_09_high_glibc", "node_02_tier2_older", False, True),
        ("job_09_high_glibc", "node_05_high_glibc", True, True),
        # GPU constraints
        ("job_06_gpu", "node_03_gpu", True, True),
        ("job_06_gpu", "node_01_cern_typical", False, True),  # CPU node lacks GPU
        # Architecture restrictions (e.g. Darwin/macOS)
        # Note: Darwin jobs inherently test the empty tags bypass if configured without tags
        ("job_08_darwin", "node_01_cern_typical", False, True),
        ("job_08_darwin", "node_06_darwin", True, True),
        # Edge Case: Extreme I/O requirement mismatched with low I/O node
        ("job_11_high_io", "node_07_low_io", False, True),
        # Coverage edges for match.py conditional skips
        ("job_12_cov_user_namespaces", "node_02_tier2_older", False, False),
        ("job_13_cov_wall_time", "node_01_cern_typical", False, False),
        ("job_14_cov_cpu_work", "node_01_cern_typical", False, False),
        ("job_15_cov_num_cores", "node_01_cern_typical", False, False),
        ("job_16_cov_arch_name", "node_01_cern_typical", False, False),
        ("job_17_cov_microarch_min", "node_01_cern_typical", False, False),
        ("job_18_cov_microarch_max", "node_01_cern_typical", False, False),
        ("job_19_cov_gpu_count", "node_03_gpu", False, False),
        ("job_20_cov_gpu_ram", "node_03_gpu", False, False),
        ("job_21_cov_gpu_vendor", "node_03_gpu", False, False),
        ("job_22_cov_gpu_compute_min", "node_03_gpu", False, False),
        ("job_23_cov_gpu_compute_max", "node_03_gpu", False, False),
        ("job_24_cov_gpu_driver", "node_03_gpu", False, False),
        ("job_25_cov_tags", "node_01_cern_typical", False, False),
    ],
    ids=[
        "match_standard_job",
        "reject_unmatched_site",
        "reject_insufficient_ram",
        "match_sufficient_ram",
        "reject_low_glibc",
        "match_high_glibc",
        "match_gpu_job_to_gpu_node",
        "reject_gpu_job_on_cpu_node",
        "reject_darwin_job_on_linux_node",
        "match_darwin_job_to_darwin_node",
        "reject_insufficient_io_scratch",
        "reject_user_namespaces",
        "reject_wall_time",
        "reject_cpu_work",
        "reject_num_cores",
        "reject_arch_name",
        "reject_microarch_min",
        "reject_microarch_max",
        "reject_gpu_count_max",
        "reject_gpu_ram",
        "reject_gpu_vendor",
        "reject_gpu_compute_min",
        "reject_gpu_compute_max",
        "reject_gpu_driver",
        "reject_tags",
    ],
)
def test_select_job_hardware_and_system_matching(
    job_file: str,
    node_file: str,
    expected_selected: bool,
    isolate_hardware: bool,
    example_config: SchedulingConfig,
    load_job,
    load_node,
):
    """Test that hardware and system constraints (CPU, RAM, GPU, OS, Tags) and their edge cases
    are correctly evaluated strictly by asserting the output of the 'select_job' entrypoint.
    """
    node = load_node(node_file)
    job = load_job(job_file)

    # OVERRIDE: Isolate specific hardware limits by bypassing environmental/workload constraints
    if isolate_hardware:
        for spec in job.matching_specs:
            spec.site = node.site
            spec.wall_time = 1  # Bypass maximum wall time limit
            spec.cpu_work = 1  # Bypass maximum cpu work limit

    with (
        patch("matchmaking.core.main.get_jobs", return_value=[job]),
        patch("matchmaking.core.main.get_selection_configuration", return_value=example_config),
    ):
        if expected_selected:
            selected = select_job(node)

            assert selected is not None
            assert selected.job_id == job.job_id
        else:
            with pytest.raises(ValueError):
                select_job(node)
