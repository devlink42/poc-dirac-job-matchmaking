#!/usr/bin/env python3

from __future__ import annotations

import sys

import pytest

from matchmaking.cli import scheduler
from matchmaking.models.config import SchedulingConfig
from matchmaking.models.job import Job
from matchmaking.models.node import Node

JOB_01 = "tests/examples/jobs/job_01_mcsimulation_any_site.yaml"
JOB_04 = "tests/examples/jobs/job_04_wgproduction_with_ram.yaml"
NODE_01 = "tests/examples/nodes/node_01_cern_typical.yaml"
CONFIG_01 = "tests/examples/config/config_01_scheduling_valid.yaml"


def _run_main(monkeypatch: pytest.MonkeyPatch, args: list[str]) -> None:
    monkeypatch.setattr(sys, "argv", ["scheduler.py", *args])
    scheduler.main()


def test_main_without_args_prints_help(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    _run_main(monkeypatch, [])
    captured = capsys.readouterr()

    assert "usage:" in captured.out.lower() or "usage:" in captured.err.lower()


def test_main_scheduler_no_config_branch(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    _run_main(monkeypatch, [NODE_01, JOB_01])
    captured = capsys.readouterr()

    output = captured.out + captured.err

    assert "selected for execution on" in output


def test_main_scheduler_success_branch(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    _run_main(monkeypatch, [NODE_01, JOB_01, CONFIG_01])
    captured = capsys.readouterr()

    output = captured.out + captured.err

    assert "selected for execution on" in output


def test_main_scheduler_no_match_branch(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    _run_main(monkeypatch, [NODE_01, JOB_04, CONFIG_01])
    captured = capsys.readouterr()

    output = captured.out + captured.err

    assert "No valid jobs from the job file can run on this node." in output


def test_main_scheduler_no_allowed_job_branch(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    def mock_match_jobs_with_node(job_path, node_path):
        try:
            job = Job.load_from_yaml(job_path)
            node = Node.load_from_yaml(node_path)
        except Exception as exc:
            raise RuntimeError(f"Failed to load job or node from YAML: {exc}") from exc

        return [job], node

    def mock_select_job(node, jobs, config):
        return None

    monkeypatch.setattr(scheduler, "match_jobs_with_node", mock_match_jobs_with_node)
    monkeypatch.setattr(scheduler, "select_job", mock_select_job)

    _run_main(monkeypatch, [NODE_01, JOB_01, CONFIG_01])
    captured = capsys.readouterr()

    output = captured.out + captured.err

    assert "No allowed job from the job file can run on this node." in output


def test_main_scheduler_exception_branch(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    def _raise_error(*_args, **_kwargs):
        raise RuntimeError("forced error")

    monkeypatch.setattr(scheduler, "match_jobs_with_node", _raise_error)

    with pytest.raises(SystemExit) as exc:
        _run_main(monkeypatch, [NODE_01, JOB_01, CONFIG_01])

    assert exc.value.code == 1

    captured = capsys.readouterr()
    output = captured.out + captured.err

    assert "Error during matchmaking: forced error" in output


def test_main_scheduler_config_exception_branch(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    def _raise_error(*_args, **_kwargs):
        raise RuntimeError("config parse error")

    monkeypatch.setattr(SchedulingConfig, "load_from_yaml", _raise_error)

    with pytest.raises(SystemExit) as exc:
        _run_main(monkeypatch, [NODE_01, JOB_01, CONFIG_01])

    assert exc.value.code == 1

    captured = capsys.readouterr()
    output = captured.out + captured.err

    assert "Failed to load scheduling config: config parse error" in output
