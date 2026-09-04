#!/usr/bin/env python3
"""Canonical requirement-group model for Alternative C routing."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, NonNegativeInt, PositiveInt, field_validator, model_validator

from matchmaking.models.utils import ArchitectureName, CustomVersion, SystemName, Type


class RequirementGroup(BaseModel):
    """Normalized matching requirements shared by a site-set job queue."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: PositiveInt = 1
    eligible_sites: tuple[str, ...] = Field(min_length=1)
    job_type: Type
    system_name: SystemName
    min_system_glibc: CustomVersion | None = None
    requires_user_namespaces: bool | None = None
    min_wall_time: PositiveInt | None = None
    min_cpu_work: PositiveInt | None = None
    min_cpu_cores: NonNegativeInt
    max_cpu_cores: NonNegativeInt
    min_ram_mb: NonNegativeInt = 0
    architecture: ArchitectureName
    min_microarchitecture_level: PositiveInt
    max_microarchitecture_level: PositiveInt | None = None
    has_gpu: bool
    min_gpu_count: NonNegativeInt = 0
    max_gpu_count: NonNegativeInt = 0
    min_gpu_ram_mb: PositiveInt | None = None
    gpu_vendor: str | None = None
    min_gpu_compute_capability: CustomVersion | None = None
    max_gpu_compute_capability: CustomVersion | None = None
    min_gpu_driver_version: CustomVersion | None = None
    min_scratch_mb: PositiveInt | None = None
    tags: str

    @field_validator("eligible_sites", mode="before")
    @classmethod
    def normalize_eligible_sites(cls, value: object) -> tuple[str, ...]:
        """Normalize sites into a stable, non-empty tuple.

        Args:
            value: Iterable of site names supplied by the ingestion layer.

        Returns:
            Site names stripped of surrounding whitespace, deduplicated, and
            sorted lexicographically.

        Raises:
            ValueError: If the value is not an iterable of non-empty strings.
        """
        if isinstance(value, (str, bytes)) or not isinstance(value, Iterable):
            raise ValueError("eligible_sites must be an iterable of non-empty strings")

        normalized_sites: set[str] = set()
        for site in value:
            if not isinstance(site, str) or not (normalized_site := site.strip()):
                raise ValueError("eligible_sites must contain only non-empty strings")
            normalized_sites.add(normalized_site)

        if not normalized_sites:
            raise ValueError("eligible_sites must not be empty")

        return tuple(sorted(normalized_sites))

    @model_validator(mode="after")
    def validate_ranges(self) -> Self:
        """Validate every bounded requirement range.

        Returns:
            The validated requirement group.

        Raises:
            ValueError: If a maximum is lower than its corresponding minimum.
        """
        ranges = (
            ("CPU core", self.min_cpu_cores, self.max_cpu_cores),
            (
                "microarchitecture level",
                self.min_microarchitecture_level,
                self.max_microarchitecture_level,
            ),
            ("GPU count", self.min_gpu_count, self.max_gpu_count),
            (
                "GPU compute capability",
                self.min_gpu_compute_capability,
                self.max_gpu_compute_capability,
            ),
        )
        for name, minimum, maximum in ranges:
            if maximum is not None and minimum is not None and maximum < minimum:
                raise ValueError(f"maximum {name} must be greater than or equal to its minimum")

        return self

    def canonical_json(self) -> str:
        """Serialize the normalized requirements deterministically.

        Returns:
            Compact JSON with sorted keys and omitted null fields.
        """
        payload = self.model_dump(mode="json", exclude_none=True)
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)

    @property
    def req_group_id(self) -> str:
        """Return the SHA-256 identifier of the canonical requirements.

        Returns:
            Lowercase hexadecimal SHA-256 digest.
        """
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()

    def to_redis_hash(self) -> dict[str, str]:
        """Convert the group to values accepted by a Redis HASH.

        Returns:
            Mapping with null fields omitted, booleans encoded as ``1`` or
            ``0``, and eligible sites encoded as compact JSON.
        """
        payload = self.model_dump(mode="json", exclude_none=True)
        redis_hash: dict[str, str] = {}

        for field_name, value in payload.items():
            if field_name == "eligible_sites":
                redis_hash[field_name] = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
            elif isinstance(value, bool):
                redis_hash[field_name] = "1" if value else "0"
            else:
                redis_hash[field_name] = str(value)

        return redis_hash
