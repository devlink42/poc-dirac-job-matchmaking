#!/usr/bin/env python3

from __future__ import annotations

import sys

import pytest

from src.core import valid_pilot as vp

JOB_01 = "tests/examples/jobs/job_01_mcsimulation_any_site.yaml"
JOB_04 = "tests/examples/jobs/job_04_wgproduction_with_ram.yaml"
JOB_INVALID = "tests/examples/jobs/invalid_01_min_gt_max.yaml"
PILOT_01 = "tests/examples/nodes/pilot_01_cern_typical.yaml"


def _run_main(monkeypatch: pytest.MonkeyPatch, args: list[str]) -> None:
    monkeypatch.setattr(sys, "argv", ["valid_pilot.py", *args])
    vp.main()


def test_main_without_args_prints_help(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    _run_main(monkeypatch, [])
    captured = capsys.readouterr()

    assert "Matchmaking and validation for DIRAC jobs and pilots" in captured.out


def test_main_validate_job_requires_path(monkeypatch: pytest.MonkeyPatch):
    with pytest.raises(SystemExit) as exc:
        _run_main(monkeypatch, ["--validate-job"])

    assert exc.value.code == 1


def test_main_validate_node_requires_path(monkeypatch: pytest.MonkeyPatch):
    with pytest.raises(SystemExit) as exc:
        _run_main(monkeypatch, ["--validate-node"])

    assert exc.value.code == 1


def test_main_validate_job_success_output(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    _run_main(monkeypatch, [JOB_01, "--validate-job"])
    captured = capsys.readouterr()

    assert "Job job_01_mcsimulation_any_site is VALID." in captured.out


def test_main_validate_job_invalid_file_content_logs_error(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
):
    _run_main(monkeypatch, [JOB_INVALID, "--validate-job"])
    captured = capsys.readouterr()

    assert "Error validating job" in captured.out
    assert "max must be greater than or equal to min" in captured.out


def test_main_validate_job_missing_file_logs_error(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    _run_main(monkeypatch, ["tests/examples/jobs/does_not_exist.yaml", "--validate-job"])
    captured = capsys.readouterr()

    assert "Error validating job" in captured.out
    assert "No such file or directory" in captured.out


def test_main_validate_node_uses_job_positional_as_fallback(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
):
    _run_main(monkeypatch, [PILOT_01, "--validate-node"])
    captured = capsys.readouterr()

    assert f"Node file {PILOT_01} is VALID." in captured.out


def test_main_matchmaking_success_branch(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    _run_main(monkeypatch, [JOB_01, PILOT_01])
    captured = capsys.readouterr()

    assert "Match found!" in captured.out


def test_main_matchmaking_no_match_branch(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    _run_main(monkeypatch, [JOB_04, PILOT_01])
    captured = capsys.readouterr()

    assert "No jobs from the job file can run on this node." in captured.out


def test_main_matchmaking_exception_branch(monkeypatch: pytest.MonkeyPatch):
    def _raise_error(*_args, **_kwargs):
        raise RuntimeError("forced error")

    monkeypatch.setattr(vp, "valid_pilot", _raise_error)

    with pytest.raises(SystemExit) as exc:
        _run_main(monkeypatch, [JOB_01, PILOT_01])

    assert exc.value.code == 1
