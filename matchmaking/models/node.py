#!/usr/bin/env python3

from __future__ import annotations

from pydantic import BaseModel, Field, NonNegativeInt, PositiveInt, model_validator

from matchmaking.models.utils import ArchitectureName, CustomVersion, Io


class System(BaseModel):
    name: str
    glibc: CustomVersion
    user_namespaces: bool = Field(validation_alias="user-namespaces")


class Architecture(BaseModel):
    name: ArchitectureName
    microarchitecture_level: PositiveInt = Field(validation_alias="microarchitecture-level")


class Cpu(BaseModel):
    num_cores: PositiveInt = Field(validation_alias="num-cores")
    ram_mb: PositiveInt = Field(validation_alias="ram-mb")
    architecture: Architecture


class Gpu(BaseModel):
    count: NonNegativeInt
    ram_mb: PositiveInt | None = Field(default=None, validation_alias="ram-mb")
    vendor: str | None = None
    compute_capability: CustomVersion | None = Field(default=None, validation_alias="compute-capability")
    driver_version: CustomVersion | None = Field(default=None, validation_alias="driver-version")

    @model_validator(mode="after")
    def check_gpu_fields(self) -> "Gpu":
        if self.count > 0:
            missing = []
            if self.ram_mb is None:
                missing.append("ram_mb")
            if self.vendor is None:
                missing.append("vendor")
            if self.compute_capability is None:
                missing.append("compute_capability")
            if self.driver_version is None:
                missing.append("driver_version")

            if missing:
                raise ValueError(f"The following fields are required because count > 0: {', '.join(missing)}")

        return self


class Node(BaseModel):
    node_id: str | None = None
    site: str
    system: System
    wall_time: PositiveInt = Field(validation_alias="wall-time")
    cpu_work: PositiveInt = Field(validation_alias="cpu-work")
    cpu: Cpu
    gpu: Gpu
    io: Io | None = None
    tags: list[str]
