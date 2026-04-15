#!/usr/bin/env python3

from __future__ import annotations

from pydantic import BaseModel, Field, NonNegativeInt, PositiveInt

from src.models.utils import ArchitectureName, CustomVersion, Io


class System(BaseModel):
    name: str
    glibc: CustomVersion
    user_namespaces: bool = Field(validation_alias="user-namespaces")


class Architecture(BaseModel):
    name: ArchitectureName
    microarchitecture_level: PositiveInt = Field(validation_alias="microarchitecture-level")


class Cpu(BaseModel):
    num_nodes: PositiveInt = Field(validation_alias="num-nodes")
    num_cores: PositiveInt = Field(validation_alias="num-cores")
    ram_mb: PositiveInt = Field(validation_alias="ram-mb")
    architecture: Architecture


class Gpu(BaseModel):
    count: NonNegativeInt
    ram_mb: PositiveInt = Field(validation_alias="ram-mb")
    vendor: str | None = None
    compute_capability: CustomVersion | None = Field(default=None, validation_alias="compute-capability")
    driver_version: CustomVersion | None = Field(default=None, validation_alias="driver-version")


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
