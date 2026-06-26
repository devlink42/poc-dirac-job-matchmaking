#!/usr/bin/env python3

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, NonNegativeInt, PositiveInt, model_validator

from matchmaking.models.base import YamlLoadableModel
from matchmaking.models.utils import ArchitectureName, CustomVersion, Io, SystemName


class System(BaseModel):
    """System information of a node."""

    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)

    name: SystemName
    glibc: CustomVersion
    user_namespaces: bool = Field(validation_alias="user-namespaces")


class Architecture(BaseModel):
    """CPU architecture of a node."""

    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)

    name: ArchitectureName
    microarchitecture_level: PositiveInt = Field(validation_alias="microarchitecture-level")


class Cpu(BaseModel):
    """CPU resources of a node."""

    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)

    num_cores: PositiveInt = Field(validation_alias="num-cores")
    ram_mb: PositiveInt = Field(validation_alias="ram-mb")
    architecture: Architecture


class Gpu(BaseModel):
    """GPU resources of a node."""

    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)

    count: NonNegativeInt
    ram_mb: PositiveInt | None = Field(default=None, validation_alias="ram-mb")
    vendor: str | None = None
    compute_capability: CustomVersion | None = Field(default=None, validation_alias="compute-capability")
    driver_version: CustomVersion | None = Field(default=None, validation_alias="driver-version")

    @model_validator(mode="after")
    def check_gpu_fields(self) -> Gpu:
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


class Node(YamlLoadableModel):
    """Data model representing a compute node."""

    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)

    version: CustomVersion = Field(default=CustomVersion("0.1"))
    node_id: str | None = None

    site: str
    system: System
    wall_time: PositiveInt = Field(validation_alias="wall-time")
    cpu_work: PositiveInt = Field(validation_alias="cpu-work")
    cpu: Cpu
    gpu: Gpu
    io: Io | None = None
    tags: list[str]
