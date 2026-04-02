from pydantic import BaseModel


class System(BaseModel):
    name: str
    glibc: float
    user_namespaces: bool


class Architecture(BaseModel):
    name: str
    microarchitecture_level: int


class Cpu(BaseModel):
    num_nodes: int
    num_cores: int
    ram_mb: int
    architecture: Architecture


class Gpu(BaseModel):
    count: dict[str, int]


class Node(BaseModel):
    nodeID: str
    system: System
    wall_time: int
    cpu_work: int
    cpu: Cpu
    gpu: Gpu
    tags: list[str]
