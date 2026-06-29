#!/usr/bin/env python3

from __future__ import annotations

from pydantic import BaseModel, Field, NonNegativeInt

from matchmaking.models.base import YamlLoadableModel
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
    name: str | None = Field(default=None, description="The name of the site.")


class SchedulingConfig(YamlLoadableModel):
    """Global scheduling configuration."""

    job_type_priorities: list[Type | dict[Type, NonNegativeInt]] = Field(
        default_factory=list,
        description="A sorted list of job types or weighted groups, from highest to lowest priority.",
    )
    by_site: dict[str, Site] = Field(default_factory=dict, description="Configuration per site.")
