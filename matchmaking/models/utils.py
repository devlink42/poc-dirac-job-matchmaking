#!/usr/bin/env python3

from __future__ import annotations

import re
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Any, Generic, Self, TypeVar

from packaging.version import InvalidVersion, Version
from pydantic import (
    BaseModel,
    Field,
    GetCoreSchemaHandler,
    NonNegativeInt,
    PlainSerializer,
    PositiveInt,
    model_validator,
)
from pydantic_core import core_schema


class Type(StrEnum):
    MCSIMULATION = "MCSimulation"
    MCFASTSIMULATION = "MCFastSimulation"
    WGPRODUCTION = "WGProduction"
    USER = "User"
    SPRUCING = "Sprucing"
    MERGE = "Merge"
    MCRECONSTRUCTION = "MCRConstruction"
    APMERGE = "APMerge"
    APPOSTPROC = "APPostProc"
    MCMERGE = "MCMerge"
    LBAPI = "LbAPI"


class OwnerGroup(StrEnum):
    LHCB_MC = "lhcb_mc"
    LHCB_DATA = "lhcb_data"
    LHCB_MCPROC = "lhcb_mproc"
    LHCB_USER = "lhcb_user"


class SystemName(StrEnum):
    LINUX = "Linux"
    GNU = "GNU"
    FREEBSD = "FreeBSD"
    OPENBSD = "OpenBSD"
    WINDOWS_NT = "Windows_NT"
    MSDOS = "MS-DOS"
    DARWIN = "Darwin"


class VersionPydanticAnnotation:
    @classmethod
    def __get_pydantic_core_schema__(cls, source_type: Any, handler: GetCoreSchemaHandler) -> core_schema.CoreSchema:
        def validate(value: Any) -> Version:
            try:
                return Version(str(value))
            except InvalidVersion as e:
                raise ValueError(f"Invalid version format: {value} (type: {type(value)})") from e

        return core_schema.no_info_plain_validator_function(validate)


CustomVersion = Annotated[Version, VersionPydanticAnnotation, PlainSerializer(lambda v: str(v), return_type=str)]


def get_current_schema_version() -> CustomVersion:
    schema_path = Path(__file__).parents[2] / "docs" / "schema_design.md"

    if not schema_path.exists():
        raise FileNotFoundError(f"No such file or directory: '{schema_path}'")

    content = schema_path.read_text(encoding="utf-8")
    versions = [CustomVersion(match) for match in re.findall(r"\bv?(\d+\.\d+(?:\.\d+)?)\b", content)]

    if not versions:
        raise ValueError(f"No schema version found in '{schema_path}'")

    latest = max(versions)
    current = (*latest.release, 0, 0)[:3]

    return CustomVersion(".".join(str(part) for part in current))


T = TypeVar("T")


class StrictRange(BaseModel, Generic[T]):
    min: T
    max: T

    @model_validator(mode="after")
    def check_min_max(self) -> Self:
        if self.max is not None and self.max < self.min:
            raise ValueError("max must be greater than or equal to min")

        return self


class Range(StrictRange, Generic[T]):
    max: T | None = None


class ResourceSpec(BaseModel):
    overhead: NonNegativeInt = 0
    per_core: NonNegativeInt = Field(default=0, validation_alias="per-core")


class ArchitectureName(StrEnum):
    # Intel/AMD 64-bit
    x86_64 = "x86_64"
    # ARM/AArch64 64-bit
    aarch64 = "aarch64"
    arm64 = "arm64"


class Io(BaseModel):
    scratch_mb: PositiveInt = Field(validation_alias="scratch-mb")
    # We don't test scratch IOPS because we are unable to accurately obtain
    # and use this data at the moment.
    scratch_iops: PositiveInt = Field(validation_alias="scratch-iops")
