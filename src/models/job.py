from typing import Union

from pydantic import BaseModel, Field, PositiveFloat, PositiveInt


class System(BaseModel):
    name: str
    glibc: PositiveFloat
    user_namespaces: bool = Field(validation_alias="user-namespaces")


class Request(BaseModel):
    overhead: PositiveInt
    per_core: PositiveInt = Field(validation_alias="per-core")


class Limit(BaseModel):
    overhead: PositiveInt
    per_core: PositiveInt = Field(validation_alias="per-core")


class Architecture(BaseModel):
    name: str
    # TODO: what it is the min and max for the range?
    microarchitecture_level: dict[str, PositiveInt] = Field(validation_alias="microarchitecture-level")


class Cpu(BaseModel):
    # TODO: what it is the min for the range?
    num_cores: dict[str, PositiveInt] = Field(validation_alias="num-cores")
    ram_mb: Union[Request, Limit] = Field(validation_alias="ram-mb")
    architecture: Architecture


class Gpu(BaseModel):
    # TODO: what it is the min and max for the range?
    count: dict[str, PositiveInt]
    ram_mb: PositiveInt = Field(validation_alias="ram-mb")
    vendor: str
    # TODO: what it is the min for the range?
    compute_capability: dict[str, PositiveInt] = Field(validation_alias="compute-capability")


class Io(BaseModel):
    scratch_mb: PositiveInt
    lan_mbitps: PositiveInt = Field(validation_alias="lan-mbitps")


class Job(BaseModel):
    job_id: str
    site: str
    system: System
    wall_time: PositiveInt = Field(validation_alias="wall-time")
    cpu_work: PositiveInt = Field(validation_alias="cpu-work")
    cpu: Cpu
    gpu: Gpu
    io: Io
    tags: str
