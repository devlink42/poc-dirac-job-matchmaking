from __future__ import annotations

from pydantic import BaseModel, Field, PositiveFloat, PositiveInt, NonNegativeInt, NonNegativeFloat

from src.models.utils import Range, StrictRange, ResourceSpec


class System(BaseModel):
    name: str
    glibc: PositiveFloat | None = None
    user_namespaces: bool | None = Field(default=None, validation_alias="user-namespaces")


class ComputeMemory(BaseModel):
    request: ResourceSpec
    limit: ResourceSpec


class Architecture(BaseModel):
    name: str
    microarchitecture_level: Range[PositiveInt] = Field(validation_alias="microarchitecture-level")


class Cpu(BaseModel):
    num_cores: StrictRange[NonNegativeInt] = Field(validation_alias="num-cores")
    ram_mb: ComputeMemory | None = Field(default=None, validation_alias="ram-mb")
    architecture: Architecture


class Gpu(BaseModel):
    count: StrictRange[NonNegativeInt]
    ram_mb: PositiveInt = Field(validation_alias="ram-mb")
    vendor: str
    compute_capability: Range[NonNegativeFloat] = Field(validation_alias="compute-capability")


class Io(BaseModel):
    scratch_mb: PositiveInt = Field(validation_alias="scratch-mb")
    lan_mbitps: PositiveInt | None = Field(default=None, validation_alias="lan-mbitps")


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
