#!/usr/bin/env python3

from __future__ import annotations

import sys
from unittest.mock import MagicMock

import pytest
import redis
import yaml

from matchmaking.cli.py_redis import scheduler
from matchmaking.models.job import Job

NODE_01 = "tests/examples/nodes/node_01_cern_typical.yaml"
CONFIG_01 = "tests/examples/config/config_01_scheduling_valid.yaml"
JOB_01 = "tests/examples/jobs/job_01_mcsimulation_any_site.yaml"


def _run_main(monkeypatch: pytest.MonkeyPatch, args: list[str]) -> None:
    monkeypatch.setattr(sys, "argv", ["scheduler.py", *args])
    scheduler.main()


def test_main_without_args_prints_help(monkeypatch, capsys):
    _run_main(monkeypatch, [])
    captured = capsys.readouterr()

    assert "usage:" in captured.out.lower() or "usage:" in captured.err.lower()


def test_main_redis_connection_error(monkeypatch, capsys):
    def _raise(*args, **kwargs):
        raise redis.ConnectionError("err")

    monkeypatch.setattr(redis.Redis, "ping", _raise)

    with pytest.raises(SystemExit) as exc:
        _run_main(monkeypatch, [NODE_01, CONFIG_01])

    captured = capsys.readouterr()

    assert exc.value.code == 1
    assert "Could not connect to Redis" in (captured.err + captured.out)


def test_main_config_load_error(monkeypatch, capsys):
    from matchmaking.models.config import SchedulingConfig

    monkeypatch.setattr(SchedulingConfig, "load_from_yaml", MagicMock(side_effect=RuntimeError("err")))
    monkeypatch.setattr("redis.Redis.ping", MagicMock())

    with pytest.raises(SystemExit) as exc:
        _run_main(monkeypatch, [NODE_01, CONFIG_01])

    captured = capsys.readouterr()

    assert exc.value.code == 1
    assert "Failed to load scheduling config" in (captured.err + captured.out)


def test_main_matchmaking_error(monkeypatch, capsys):
    monkeypatch.setattr("redis.Redis.ping", MagicMock())
    monkeypatch.setattr(scheduler, "fetch_candidate_jobs", MagicMock(side_effect=RuntimeError("err")))

    with pytest.raises(SystemExit) as exc:
        _run_main(monkeypatch, [NODE_01, CONFIG_01])

    captured = capsys.readouterr()

    assert exc.value.code == 1
    assert "Error during Redis matchmaking" in (captured.err + captured.out)


def test_main_success_branch(monkeypatch, capsys):
    monkeypatch.setattr("redis.Redis.ping", MagicMock())

    with open(JOB_01) as f:
        job = Job.model_validate(yaml.safe_load(f))

    monkeypatch.setattr(scheduler, "fetch_candidate_jobs", MagicMock(return_value=[job]))

    _run_main(monkeypatch, [NODE_01, CONFIG_01])

    captured = capsys.readouterr()

    assert "selected for execution on" in (captured.err + captured.out)


def test_main_no_allowed_job_branch(monkeypatch, capsys):
    monkeypatch.setattr("redis.Redis.ping", MagicMock())

    with open(JOB_01) as f:
        job = Job.model_validate(yaml.safe_load(f))

    monkeypatch.setattr(scheduler, "fetch_candidate_jobs", MagicMock(return_value=[job]))
    monkeypatch.setattr(scheduler, "select_job", MagicMock(return_value=None))

    _run_main(monkeypatch, [NODE_01, CONFIG_01])

    captured = capsys.readouterr()

    assert "No allowed job found for node" in (captured.err + captured.out)


def test_main_no_valid_jobs(monkeypatch, capsys):
    monkeypatch.setattr("redis.Redis.ping", MagicMock())
    monkeypatch.setattr(scheduler, "fetch_candidate_jobs", MagicMock(return_value=[]))

    _run_main(monkeypatch, [NODE_01, CONFIG_01])

    assert "No valid jobs" not in capsys.readouterr().err
