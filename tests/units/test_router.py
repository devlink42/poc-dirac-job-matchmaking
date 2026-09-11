#!/usr/bin/env python3

from __future__ import annotations

from matchmaking.core.router import MatchMode


def test_match_mode_enum():
    assert MatchMode.PYTHON == "python"
    assert MatchMode.LUA_ALT_A == "lua_alt_a"
    assert MatchMode.LUA_ALT_C == "lua_alt_c"
