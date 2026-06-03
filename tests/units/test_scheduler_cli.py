#!/usr/bin/env python3

from __future__ import annotations

import sys

import pytest

from matchmaking.cli import scheduler
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

    assert "selected for execution" in output


def test_main_scheduler_success_branch(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    _run_main(monkeypatch, [NODE_01, JOB_01])
    captured = capsys.readouterr()

    output = captured.out + captured.err

    assert "selected for execution" in output


def test_main_scheduler_no_match_branch(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    _run_main(monkeypatch, [NODE_01, JOB_04])
    captured = capsys.readouterr()

    output = captured.out + captured.err

    assert ("selected for execution" in output) or ("No allowed job can run on this node." in output)


def test_main_scheduler_no_allowed_job_branch(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    def mock_select_job(node):
        return None

    monkeypatch.setattr(scheduler, "select_job", mock_select_job)

    _run_main(monkeypatch, [NODE_01, JOB_01])
    captured = capsys.readouterr()

    output = captured.out + captured.err

    assert "No allowed job can run on this node." in output


def test_main_scheduler_exception_branch(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    def _raise_error(*_args, **_kwargs):
        raise RuntimeError("forced error")

    monkeypatch.setattr(scheduler, "select_job", _raise_error)

    with pytest.raises(SystemExit) as exc:
        _run_main(monkeypatch, [NODE_01, JOB_01])

    assert exc.value.code == 1

    captured = capsys.readouterr()
    output = captured.out + captured.err

    assert "Error during matchmaking: forced error" in output


def test_main_scheduler_config_exception_branch(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    def _raise_error(*_args, **_kwargs):
        raise RuntimeError("config parse error")

    monkeypatch.setattr(Node, "load_from_yaml", _raise_error)

    with pytest.raises(SystemExit) as exc:
        _run_main(monkeypatch, [NODE_01, JOB_01])

    assert exc.value.code == 1

    captured = capsys.readouterr()
    output = captured.out + captured.err

    assert "Error during matchmaking: config parse error" in output
