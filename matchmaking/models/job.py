#!/usr/bin/env python3

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, NonNegativeInt, PositiveInt, field_validator, model_validator

from matchmaking.logic.tags import validate_tag_expression
from matchmaking.models.utils import (
    ArchitectureName,
    CustomVersion,
    Io,
    JobStatus,
    Range,
    ResourceSpec,
    StrictRange,
    SystemName,
    Type,
)


class System(BaseModel):
    """System requirements for a job."""

    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)

    name: SystemName
    glibc: CustomVersion | None = None
    user_namespaces: bool | None = Field(default=None, validation_alias="user-namespaces")


class ComputeMemory(BaseModel):
    """Memory requirements for computation."""

    # TODO: .
    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)

    request: ResourceSpec
    limit: ResourceSpec


class Architecture(BaseModel):
    """CPU architecture requirements."""

    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)

    name: ArchitectureName
    microarchitecture_level: Range[PositiveInt] = Field(validation_alias="microarchitecture-level")


class Cpu(BaseModel):
    """CPU core and RAM requirements."""

    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)

    num_cores: StrictRange[NonNegativeInt] = Field(validation_alias="num-cores")
    ram_mb: ComputeMemory | None = Field(default=None, validation_alias="ram-mb")
    architecture: Architecture


class Gpu(BaseModel):
    """GPU requirements for a job."""

    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)

    count: StrictRange[NonNegativeInt]
    ram_mb: PositiveInt = Field(validation_alias="ram-mb")
    vendor: str
    compute_capability: Range[CustomVersion] = Field(validation_alias="compute-capability")
    driver_version: CustomVersion | None = Field(default=None, validation_alias="driver-version")


class MatchingSpecs(BaseModel):
    """Specification of requirements for matching a job with a node."""

    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)

    site: str | None = None
    system: System
    wall_time: PositiveInt | None = Field(default=None, validation_alias="wall-time")
    cpu_work: PositiveInt | None = Field(default=None, validation_alias="cpu-work")
    cpu: Cpu
    gpu: Gpu | None = None
    io: Io | None = None
    tags: str

    @field_validator("tags")
    @classmethod
    def validate_tags(cls, v: str) -> str:
        if not v:
            return v

        try:
            validate_tag_expression(v)
        except ValueError as e:
            raise ValueError(f"Invalid tag expression: {e}") from e

        return v

    @model_validator(mode="after")
    def validate_job(self):
        if self.wall_time is None and self.cpu_work is None:
            raise ValueError("At least one of 'wall-time' or 'cpu-work' must be provided")

        return self


class Job(BaseModel):
    """Data model representing a job in the matchmaking system."""

    version: CustomVersion = Field(default=CustomVersion("0.1"))
    job_id: str | None = None
    submit_time: datetime

    # Job information
    owner: str
    group: str
    type: Type
    status: JobStatus = JobStatus.WAITING

    # Matching specs
    matching_specs: list[MatchingSpecs] = Field(min_length=1)

    @classmethod
    def load_from_yaml(cls, path: str | Path) -> Job:
        """Load and apply the configuration from a YAML file."""
        file_path = Path(path)
        if not file_path.exists():
            raise FileNotFoundError(f"No such file or directory: '{file_path}'")

        with open(file_path, encoding="utf-8") as f:
            data = yaml.safe_load(f)

        return cls.model_validate(data or {})
