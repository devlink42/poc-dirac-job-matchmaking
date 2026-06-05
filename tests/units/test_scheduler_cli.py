#!/usr/bin/env python3

from __future__ import annotations

import sys
from datetime import UTC, datetime
from unittest.mock import Mock

import pytest

from matchmaking.cli import scheduler
from matchmaking.models.node import Node

NODE_01 = "tests/examples/nodes/node_01_cern_typical.yaml"


def _run_main(monkeypatch: pytest.MonkeyPatch, args: list[str] | None = None) -> None:
    monkeypatch.setattr(sys, "argv", ["scheduler.py", *(args or [])])
    scheduler.main()


def test_main_without_args_prints_help(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    _run_main(monkeypatch)
    captured = capsys.readouterr()

    assert "usage:" in captured.out.lower() or "usage:" in captured.err.lower()


def test_main_scheduler_success_branch(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    selected_job = Mock()
    selected_job.job_id = "job_01"
    selected_job.submit_time = datetime(2024, 1, 1, tzinfo=UTC)

    monkeypatch.setattr(scheduler, "select_job", lambda _node: selected_job)

    _run_main(monkeypatch, [NODE_01])
    captured = capsys.readouterr()

    output = captured.out + captured.err

    assert "Job job_01 selected for execution on LCG.CERN.cern." in output


def test_main_scheduler_no_allowed_job_branch(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    monkeypatch.setattr(scheduler, "select_job", lambda _node: None)

    _run_main(monkeypatch, [NODE_01])
    captured = capsys.readouterr()

    output = captured.out + captured.err

    assert "No allowed job can run on this node." in output


def test_main_scheduler_exception_branch(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    def _raise_error(*_args, **_kwargs):
        raise RuntimeError("forced error")

    monkeypatch.setattr(scheduler, "select_job", _raise_error)

    with pytest.raises(SystemExit) as exc:
        _run_main(monkeypatch, [NODE_01])

    assert exc.value.code == 1

    captured = capsys.readouterr()
    output = captured.out + captured.err

    assert "Error during matchmaking: forced error" in output


def test_main_scheduler_node_load_exception_branch(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    def _raise_error(*_args, **_kwargs):
        raise RuntimeError("node parse error")

    monkeypatch.setattr(Node, "load_from_yaml", _raise_error)

    with pytest.raises(SystemExit) as exc:
        _run_main(monkeypatch, [NODE_01])

    assert exc.value.code == 1

    captured = capsys.readouterr()
    output = captured.out + captured.err

    assert "Error during matchmaking: node parse error" in output
