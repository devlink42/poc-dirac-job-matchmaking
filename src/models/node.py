from pydantic import BaseModel, Field, PositiveFloat, PositiveInt


class System(BaseModel):
    name: str
    glibc: PositiveFloat
    user_namespaces: bool = Field(validation_alias="user-namespaces")


class Architecture(BaseModel):
    name: str
    # TODO: if it as a range, from whichever is the lowest to the highest?
    microarchitecture_level: PositiveInt = Field(validation_alias="microarchitecture-level")


class Cpu(BaseModel):
    num_nodes: PositiveInt = Field(validation_alias="num-nodes")
    num_cores: PositiveInt = Field(validation_alias="num-cores")
    ram_mb: PositiveInt = Field(validation_alias="ram-mb")
    architecture: Architecture


class Gpu(BaseModel):
    count: dict[str, PositiveInt]


class Node(BaseModel):
    node_id: str
    system: System
    wall_time: PositiveInt = Field(validation_alias="wall-time")
    cpu_work: PositiveInt = Field(validation_alias="cpu-work")
    cpu: Cpu
    gpu: Gpu
    tags: list[str]
