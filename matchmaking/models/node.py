#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, NonNegativeInt, PositiveInt, model_validator

from matchmaking.models.utils import ArchitectureName, CustomVersion, Io, SystemName


class System(BaseModel):
    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)

    name: SystemName
    glibc: CustomVersion
    user_namespaces: bool = Field(validation_alias="user-namespaces")


class Architecture(BaseModel):
    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)

    name: ArchitectureName
    microarchitecture_level: PositiveInt = Field(validation_alias="microarchitecture-level")


class Cpu(BaseModel):
    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)

    num_cores: PositiveInt = Field(validation_alias="num-cores")
    ram_mb: PositiveInt = Field(validation_alias="ram-mb")
    architecture: Architecture


class Gpu(BaseModel):
    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)

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
    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)

    node_id: str | None = None
    site: str
    system: System
    wall_time: PositiveInt = Field(validation_alias="wall-time")
    cpu_work: PositiveInt = Field(validation_alias="cpu-work")
    cpu: Cpu
    gpu: Gpu
    io: Io | None = None
    tags: list[str]

    @classmethod
    def load_from_yaml(cls, path: str | Path) -> Node:
        """Load and apply the configuration from a YAML file."""
        file_path = Path(path)
        if not file_path.exists():
            raise FileNotFoundError(f"No such file or directory: '{file_path}'")

        with open(file_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        return cls.model_validate(data or {})
