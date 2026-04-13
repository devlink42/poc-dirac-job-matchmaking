#!/usr/bin/env python3

from __future__ import annotations

from src.core.valid_pilot import configure_logger


def pytest_configure() -> None:
    """Apply a consistent logger configuration for the whole test suite."""
    configure_logger("DEBUG")
