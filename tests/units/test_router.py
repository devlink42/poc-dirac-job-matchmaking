#!/usr/bin/env python3

from __future__ import annotations

from matchmaking.core.router import MatchMode


def test_match_mode_enum():
    assert MatchMode.PYTHON == "python"
    assert MatchMode.PYTHON_REDIS == "python_redis"
