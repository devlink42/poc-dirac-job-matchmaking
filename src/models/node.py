from pydantic import BaseModel, Field, PositiveFloat, PositiveInt, NonNegativeInt


class System(BaseModel):
    name: str
    glibc: PositiveFloat
    user_namespaces: bool = Field(validation_alias="user-namespaces")


class Architecture(BaseModel):
    name: str
    microarchitecture_level: PositiveInt = Field(validation_alias="microarchitecture-level")


class Cpu(BaseModel):
    num_nodes: PositiveInt = Field(validation_alias="num-nodes")
    num_cores: PositiveInt = Field(validation_alias="num-cores")
    ram_mb: PositiveInt = Field(validation_alias="ram-mb")
    architecture: Architecture


class Gpu(BaseModel):
    count: NonNegativeInt


class Node(BaseModel):
    node_id: str | None = None
    system: System
    wall_time: PositiveInt = Field(validation_alias="wall-time")
    cpu_work: PositiveInt = Field(validation_alias="cpu-work")
    cpu: Cpu
    gpu: Gpu
    tags: list[str]
