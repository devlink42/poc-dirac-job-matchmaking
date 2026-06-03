#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, Field, NonNegativeInt

from matchmaking.models.utils import Type


class Site(BaseModel):
    """Configuration for a specific computing site."""

    ce_s: list | None = Field(
        default=None, validation_alias="CEs", description="List of compute elements associated with the site."
    )
    associated_se_s: list | None = Field(
        default=None, validation_alias="AssociatedSEs", description="List of storage elements associated with the site."
    )
    running_limits: dict[Type, NonNegativeInt] = Field(
        default_factory=dict, description="Limits on the number of concurrent jobs per site and per job type."
    )
    name: str = Field(default=None, description="The name of the site.")


class SchedulingConfig(BaseModel):
    """Global scheduling configuration."""

    job_type_priorities: list[Type] = Field(
        default_factory=list, description="A sorted list of job types, from highest to lowest priority."
    )
    by_site: dict[str, Site] = Field(default_factory=dict, description="Configuration per site.")

    @classmethod
    def load_from_yaml(cls, path: str | Path) -> SchedulingConfig:
        """Load and apply the configuration from a YAML file."""
        file_path = Path(path)
        if not file_path.exists():
            raise FileNotFoundError(f"No such file or directory: '{file_path}'")

        with open(file_path, encoding="utf-8") as f:
            data = yaml.safe_load(f)

        return cls.model_validate(data or {})
