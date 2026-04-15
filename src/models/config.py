#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, Field, NonNegativeInt


class SchedulingConfig(BaseModel):
    job_type_priorities: list[str] = Field(
        default_factory=list, description="A sorted list of job types, from highest to lowest priority."
    )
    running_limits: dict[str, dict[str, NonNegativeInt]] = Field(
        default_factory=dict, description="Limits on the number of concurrent jobs per site and per job type."
    )

    @classmethod
    def load_from_yaml(cls, path: str | Path) -> SchedulingConfig:
        """Load and apply the configuration from a YAML file."""
        file_path = Path(path)
        if not file_path.exists():
            raise FileNotFoundError(f"Le fichier de configuration {file_path} est introuvable.")

        with open(file_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        return cls(**(data or {}))
