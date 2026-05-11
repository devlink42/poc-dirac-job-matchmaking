#!/usr/bin/env python3
from __future__ import annotations

from pydantic import BaseModel, Field, NonNegativeInt, PositiveInt, field_validator, model_validator

from matchmaking.logic.tags import validate_tag_expression


class System(BaseModel):
    name: str
    glibc: str
    user_namespaces: bool


class CpuRamRequest(BaseModel):
    overhead: NonNegativeInt = 0
    per_core: NonNegativeInt = 0


class CpuRamLimit(BaseModel):
    overhead: NonNegativeInt = 0
    per_core: NonNegativeInt = 0


class CpuRamMb(BaseModel):
    request: CpuRamRequest = Field(default_factory=CpuRamRequest)
    limit: CpuRamLimit = Field(default_factory=CpuRamLimit)


class CpuArchitecture(BaseModel):
    name: str
    microarchitecture_level_min: PositiveInt
    microarchitecture_level_max: PositiveInt | None = None


class Cpu(BaseModel):
    num_cores_min: NonNegativeInt
    num_cores_max: NonNegativeInt
    ram_mb: CpuRamMb = Field(default_factory=CpuRamMb)
    architecture: CpuArchitecture


class Gpu(BaseModel):
    count_min: NonNegativeInt = 0
    count_max: NonNegativeInt | None = None
    ram_mb: PositiveInt | None = None
    vendor: str | None = None
    compute_capability_min: str | None = None
    compute_capability_max: str | None = None
    driver_version: str | None = None


class Io(BaseModel):
    scratch_mb: PositiveInt | None = None
    scratch_iops: PositiveInt | None = None


class Job(BaseModel):
    """Clean, nested Job model for Alternative A.
    While the Python interface uses clean nested sub-models, the
    `to_redis_hash()` method dynamically flattens it for Vanilla Redis.
    """

    job_id: str = "Unknown"
    priority: int = 0

    # Job information
    owner: str
    group: str
    job_type: str
    submission_time: float  # Unix timestamp, Redis/Lua don't accept datetime

    site: str = "ANY"

    wall_time: PositiveInt | None = None
    cpu_work: PositiveInt | None = None

    # Nested Sub-models
    system: System
    cpu: Cpu
    gpu: Gpu = Field(default_factory=Gpu)
    io: Io = Field(
        default_factory=Io,
    )

    # Tags stored as a raw string (e.g. "cvmfs:lhcb & (os:el9 | os:ubuntu)")
    tags: str = ""

    @field_validator("tags")
    @classmethod
    def validate_tags(cls, v: str) -> str:
        if not v:
            return v

        try:
            validate_tag_expression(v)
        except ValueError as e:
            raise ValueError(f"Invalid tag expression: {e}") from e

        return v

    @model_validator(mode="after")
    def validate_job(self):
        if self.wall_time is None and self.cpu_work is None:
            raise ValueError("At least one of 'wall_time' or 'cpu_work' must be provided")

        return self

    def to_redis_hash(self) -> dict[str, str]:
        """Dynamically convert the nested model to a 1D flat dictionary for HSET.
        Example: `self.cpu.ram_mb.request.overhead` becomes `cpu_ram_mb_request_overhead`.
        Excludes job_id and submission_time. Casts everything to strings.
        """
        raw_dict = self.model_dump(exclude={"job_id", "submission_time"})

        def flatten(d: dict, parent_key: str = "") -> dict:
            items = []
            for k, v in d.items():
                new_key = f"{parent_key}_{k}" if parent_key else k

                if isinstance(v, dict):
                    items.extend(flatten(v, new_key).items())
                elif v is not None:
                    # Redis HSET cannot store None. We only store actual values as strings.
                    items.append((new_key, str(v)))

            return dict(items)

        return flatten(raw_dict)
