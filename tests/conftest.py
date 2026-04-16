#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path

from config import configure_logger

PROJECT_ROOT = Path(__file__).parent.parent.absolute()


def pytest_configure() -> None:
    """Apply a consistent logger configuration for the whole test suite."""
    configure_logger("DEBUG")
