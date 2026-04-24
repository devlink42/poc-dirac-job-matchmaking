#!/usr/bin/env python3

from __future__ import annotations

from pydantic import BaseModel, Field, NonNegativeInt, PositiveInt, model_validator

from src.models.utils import ArchitectureName, CustomVersion, Io, Range, ResourceSpec, StrictRange


class System(BaseModel):
    name: str
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
    wall_time: PositiveInt | None = Field(default=None, validation_alias="wall-time")
    cpu_work: PositiveInt | None = Field(default=None, validation_alias="cpu-work")
    cpu: Cpu
    gpu: Gpu | None = None
    io: Io | None = None
    tags: str

    @model_validator(mode="after")
    def validate_job(self):
        if self.wall_time is None and self.cpu_work is None:
            raise ValueError("At least one of 'wall-time' or 'cpu-work' must be provided")

        if self.wall_time and self.wall_time <= 0:
            raise ValueError("wall_time must be positive")
        if self.cpu_work and self.cpu_work <= 0:
            raise ValueError("cpu_work must be positive")

        return self
