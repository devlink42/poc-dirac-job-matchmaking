from typing import Union

from pydantic import BaseModel


class System(BaseModel):
    name: str
    glibc: float
    user_namespaces: bool


class Request(BaseModel):
    overhead: int
    per_core: int


class Limit(BaseModel):
    overhead: int
    per_core: int


class Architecture(BaseModel):
    name: str
    microarchitecture_level: dict[str, int]


class Cpu(BaseModel):
    num_cores: dict[str, int]
    ram_mb: Union[Request, Limit]
    architecture: Architecture


class Gpu(BaseModel):
    count: dict[str, int]
    ram_mb: int
    vendor: str
    compute_capability: dict[str, int]


class Io(BaseModel):
    scratch_mb: int
    lan_mbitps: int


class Job(BaseModel):
    jobID: str
    site: str
    system: System
    wall_time: int
    cpu_work: int
    cpu: Cpu
    gpu: Gpu
    io: Io
    tags: str
