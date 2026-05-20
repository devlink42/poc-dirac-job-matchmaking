#!/usr/bin/env python3

from __future__ import annotations

import sys
from unittest.mock import MagicMock

import pytest
import redis

from matchmaking.cli.py_redis import data_loader
from matchmaking.core.py_redis.match_making import JOBS_KEY, NODES_KEY


def _run_main(monkeypatch: pytest.MonkeyPatch, args: list[str]) -> None:
    monkeypatch.setattr(sys, "argv", ["data_loader.py", *args])
    data_loader.main()


def test_load_data_batches_and_flushes_remainder(monkeypatch: pytest.MonkeyPatch):
    # A small batch size forces both the in-loop flush and the trailing flush.
    monkeypatch.setattr(data_loader, "_BATCH_SIZE", 2)

    pipe = MagicMock()
    client = MagicMock()
    client.pipeline.return_value = pipe

    data_loader.load_data(client, num_jobs=3, num_nodes=3)

    # 3 jobs + 3 nodes with batch size 2 => one mid-loop flush + one trailing
    # flush per entity type = 4 executes total.
    assert pipe.execute.call_count == 4
    assert pipe.hset.call_count == 6
    client.pipeline.assert_called_with(transaction=False)


def test_main_connection_error_exits(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    def _raise(**_kwargs):
        client = MagicMock()
        client.ping.side_effect = redis.ConnectionError("down")
        return client

    monkeypatch.setattr(data_loader.redis, "Redis", _raise)

    with pytest.raises(SystemExit) as exc:
        _run_main(monkeypatch, ["--num-jobs", "1", "--num-nodes", "1"])

    assert exc.value.code == 1
    assert "Could not connect to Redis" in (captured := capsys.readouterr()).out + captured.err


def test_main_loads_data(monkeypatch: pytest.MonkeyPatch):
    client = MagicMock()
    client.ping.return_value = True
    client.pipeline.return_value = MagicMock()

    monkeypatch.setattr(data_loader.redis, "Redis", lambda **_kwargs: client)

    _run_main(monkeypatch, ["--num-jobs", "2", "--num-nodes", "2"])

    # Stale keys are wiped before loading the fresh dataset.
    client.delete.assert_any_call(JOBS_KEY)
    client.delete.assert_any_call(NODES_KEY)
    assert client.pipeline.called
