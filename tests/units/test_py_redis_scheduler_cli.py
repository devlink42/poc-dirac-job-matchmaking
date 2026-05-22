# #!/usr/bin/env python3
#
# from __future__ import annotations
#
# import sys
# from unittest.mock import MagicMock
#
# import pytest
# import redis
#
# from matchmaking.cli.py_redis import scheduler
# from matchmaking.models.job import Job
#
# NODE_01 = "tests/examples/nodes/node_01_cern_typical.yaml"
# JOB_01 = "tests/examples/jobs/job_01_mcsimulation_any_site.yaml"
# JOB_04 = "tests/examples/jobs/job_04_wgproduction_with_ram.yaml"
# CONFIG_01 = "tests/examples/config/config_01_scheduling_valid.yaml"
#
#
# def _run_main(monkeypatch: pytest.MonkeyPatch, args: list[str]) -> None:
#     monkeypatch.setattr(sys, "argv", ["scheduler.py", *args])
#     scheduler.main()
#
#
# def _patch_redis(monkeypatch: pytest.MonkeyPatch, job_path: str | None) -> MagicMock:
#     """Patch the CLI's Redis constructor to return a mock seeded with a job."""
#     client = MagicMock()
#     client.ping.return_value = True
#
#     if job_path is not None:
#         raw = Job.load_from_yaml(job_path).model_dump_json()
#         client.hrandfield.return_value = ["job"]
#         client.hmget.return_value = [raw]
#     else:
#         client.hrandfield.return_value = []
#
#     monkeypatch.setattr(scheduler.redis, "Redis", lambda **_kwargs: client)
#
#     return client
#
#
# def test_main_without_args_prints_help(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
#     _run_main(monkeypatch, [])
#     captured = capsys.readouterr()
#
#     assert "usage:" in (captured.out + captured.err).lower()
#
#
# def test_main_invalid_node_exits(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
#     with pytest.raises(SystemExit) as exc:
#         _run_main(monkeypatch, ["tests/examples/nodes/does_not_exist.yaml", CONFIG_01])
#
#     assert exc.value.code == 1
#     assert "Invalid node specification" in (captured := capsys.readouterr()).out + captured.err
#
#
# def test_main_redis_connection_error_exits(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
#     def _raise(**_kwargs):
#         client = MagicMock()
#         client.ping.side_effect = redis.ConnectionError("down")
#         return client
#
#     monkeypatch.setattr(scheduler.redis, "Redis", _raise)
#
#     with pytest.raises(SystemExit) as exc:
#         _run_main(monkeypatch, [NODE_01, CONFIG_01])
#
#     assert exc.value.code == 1
#     assert "Could not connect to Redis" in (captured := capsys.readouterr()).out + captured.err
#
#
# def test_main_invalid_config_exits(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
#     _patch_redis(monkeypatch, JOB_01)
#
#     with pytest.raises(SystemExit) as exc:
#         _run_main(monkeypatch, [NODE_01, "tests/examples/config/does_not_exist.yaml"])
#
#     assert exc.value.code == 1
#     assert "Failed to load scheduling config" in (captured := capsys.readouterr()).out + captured.err
#
#
# def test_main_matchmaking_error_exits(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
#     _patch_redis(monkeypatch, JOB_01)
#     monkeypatch.setattr(scheduler, "select_job_from_redis", MagicMock(side_effect=RuntimeError("boom")))
#
#     with pytest.raises(SystemExit) as exc:
#         _run_main(monkeypatch, [NODE_01, CONFIG_01])
#
#     assert exc.value.code == 1
#     assert "Error during Redis matchmaking" in (captured := capsys.readouterr()).out + captured.err
#
#
# def test_main_selects_job(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
#     _patch_redis(monkeypatch, JOB_01)
#
#     _run_main(monkeypatch, [NODE_01, CONFIG_01])
#
#     assert "selected for execution on" in (captured := capsys.readouterr()).out + captured.err
#
#
# def test_main_no_compatible_job(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
#     _patch_redis(monkeypatch, JOB_04)
#
#     _run_main(monkeypatch, [NODE_01, CONFIG_01])
#
#     assert "No compatible job found" in (captured := capsys.readouterr()).out + captured.err
