#!/usr/bin/env python3
from __future__ import annotations

from pydantic import BaseModel, NonNegativeInt, PositiveInt, field_validator, model_validator

from matchmaking.logic.tags import validate_tag_expression


class Job(BaseModel):
    """Complete flattened Job model for Alternative A (Vanilla Redis Hash).
    No nested dictionaries or lists are allowed.
    """

    job_id: str = "Unknown"
    priority: int = 0

    # Job information
    owner: str
    group: str
    job_type: str
    submission_time: float  # Unix timestamp, Redis/Lua don't accept datetime

    site: str = "ANY"

    # System
    system_name: str
    system_glibc: str
    system_user_namespaces: bool

    wall_time: PositiveInt | None
    cpu_work: PositiveInt | None

    # CPU
    cpu_num_cores_min: NonNegativeInt
    cpu_num_cores_max: NonNegativeInt

    # RAM
    cpu_ram_mb_request_overhead: NonNegativeInt = 0
    cpu_ram_mb_request_per_core: NonNegativeInt = 0
    cpu_ram_mb_limit_overhead: NonNegativeInt = 0
    cpu_ram_mb_limit_per_core: NonNegativeInt = 0

    # Architecture
    cpu_architecture_name: str
    cpu_architecture_microarchitecture_level_min: PositiveInt
    cpu_architecture_microarchitecture_level_max: PositiveInt | None = None

    # GPU
    gpu_count_min: NonNegativeInt = 0
    gpu_count_max: NonNegativeInt | None = None
    gpu_ram_mb: PositiveInt | None = None
    gpu_vendor: str | None = None
    gpu_compute_capability_min: str | None = None
    gpu_compute_capability_max: str | None = None
    gpu_driver_version: str | None = None

    # IO
    io_scratch_mb: PositiveInt | None = None
    io_scratch_iops: PositiveInt | None = None

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
            raise ValueError("At least one of 'wall-time' or 'cpu-work' must be provided")

        return self

    def to_redis_hash(self) -> dict:
        """Convert the model to a dictionary for HSET.
        Exclude job_id (which is in the Redis key) and submission_time (which is used as the ZSET score).
        Cast everything to string/int for Redis.
        """
        return {k: str(v) for k, v in self.model_dump(exclude={"job_id", "submission_time"}).items()}
