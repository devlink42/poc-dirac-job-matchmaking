#!/usr/bin/env python3

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field, NonNegativeInt, PositiveInt

from src.models.utils import (
    ArchitectureName,
    CustomVersion,
    Io,
    JobGroup,
    JobOwner,
    JobType,
    Range,
    ResourceSpec,
    StrictRange,
    SystemName,
)


class JobInfo(BaseModel):
    job_id: str | None = None
    owner: JobOwner | str
    group: JobGroup
    job_type: JobType
    submission_time: datetime


class System(BaseModel):
    name: SystemName
    glibc: CustomVersion | None = None
    user_namespaces: bool | None = Field(default=None, validation_alias="user-namespaces")


class ComputeMemory(BaseModel):
    request: ResourceSpec
    limit: ResourceSpec


class Architecture(BaseModel):
    name: ArchitectureName
    microarchitecture_level: Range[PositiveInt] = Field(validation_alias="microarchitecture-level")


class Cpu(BaseModel):
    num_cores: StrictRange[NonNegativeInt] = Field(validation_alias="num-cores")
    ram_mb: ComputeMemory | None = Field(default=None, validation_alias="ram-mb")
    architecture: Architecture


class Gpu(BaseModel):
    count: StrictRange[NonNegativeInt]
    ram_mb: PositiveInt = Field(validation_alias="ram-mb")
    vendor: str
    compute_capability: Range[CustomVersion] = Field(validation_alias="compute-capability")
    driver_version: CustomVersion | None = Field(default=None, validation_alias="driver-version")


class Job(BaseModel):
    job_id: str | None = None
    site: str | None = None
    system: System
    wall_time: PositiveInt = Field(validation_alias="wall-time")
    cpu_work: PositiveInt = Field(validation_alias="cpu-work")
    cpu: Cpu
    gpu: Gpu | None = None
    io: Io | None = None
    tags: str
