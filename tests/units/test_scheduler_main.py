#!/usr/bin/env python3

from __future__ import annotations

import sys

import pytest

from src.core import scheduler as sched

JOB_01 = "tests/examples/jobs/job_01_mcsimulation_any_site.yaml"
JOB_04 = "tests/examples/jobs/job_04_wgproduction_with_ram.yaml"
PILOT_01 = "tests/examples/nodes/pilot_01_cern_typical.yaml"
CONFIG_01 = "tests/examples/config/config_01_scheduling_valid.yaml"


def _run_main(monkeypatch: pytest.MonkeyPatch, args: list[str]) -> None:
    monkeypatch.setattr(sys, "argv", ["scheduler.py", *args])
    sched.main()


def test_main_without_args_prints_help(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    _run_main(monkeypatch, [])
    captured = capsys.readouterr()

    assert "usage:" in captured.out.lower() or "usage:" in captured.err.lower()


def test_main_missing_config_arg_prints_help(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    _run_main(monkeypatch, [PILOT_01, JOB_01])
    captured = capsys.readouterr()

    assert "usage:" in captured.out.lower() or "usage:" in captured.err.lower()


def test_main_scheduler_success_branch(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    _run_main(monkeypatch, [PILOT_01, JOB_01, CONFIG_01])
    captured = capsys.readouterr()

    output = captured.out + captured.err
    assert "selected for execution on" in output


def test_main_scheduler_no_match_branch(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    _run_main(monkeypatch, [PILOT_01, JOB_04, CONFIG_01])
    captured = capsys.readouterr()

    output = captured.out + captured.err
    assert "No jobs from the job file can run on this node." in output


def test_main_scheduler_exception_branch(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    def _raise_error(*_args, **_kwargs):
        raise RuntimeError("forced error")

    monkeypatch.setattr(sched, "valid_pilot", _raise_error)

    with pytest.raises(SystemExit) as exc:
        _run_main(monkeypatch, [PILOT_01, JOB_01, CONFIG_01])

    assert exc.value.code == 1
    captured = capsys.readouterr()
    output = captured.out + captured.err
    assert "Error during matchmaking: forced error" in output
