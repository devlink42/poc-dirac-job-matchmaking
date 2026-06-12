#!/usr/bin/env python3

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Any, Generic, Self, TypeVar

from packaging.version import InvalidVersion, Version
from pydantic import (
    BaseModel,
    ConfigDict,
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


class JobStatus(StrEnum):
    WAITING = "waiting"
    STAGING = "staging"
    HOLD = "hold"
    FAIL = "fail"
    RUNNING = "running"
    DONE = "done"


class SystemName(StrEnum):
    LINUX = "Linux"
    GNU = "GNU"
    FREEBSD = "FreeBSD"
    OPENBSD = "OpenBSD"
    WINDOWS_NT = "Windows_NT"
    MSDOS = "MS-DOS"
    DARWIN = "Darwin"


class VersionPydanticAnnotation:
    """Pydantic annotation for the Version class from packaging.version."""

    @classmethod
    def __get_pydantic_core_schema__(cls, source_type: Any, handler: GetCoreSchemaHandler) -> core_schema.CoreSchema:
        def validate(value: Any) -> Version:
            try:
                return Version(str(value))
            except InvalidVersion as e:
                raise ValueError(f"Invalid version format: {value} (type: {type(value)})") from e

        return core_schema.no_info_plain_validator_function(validate)


CustomVersion = Annotated[Version, VersionPydanticAnnotation, PlainSerializer(lambda v: str(v), return_type=str)]


T = TypeVar("T")


class StrictRange(BaseModel, Generic[T]):
    """A range with mandatory min and max values."""

    min: T
    max: T

    @model_validator(mode="after")
    def check_min_max(self) -> Self:
        if self.max is not None and self.max < self.min:
            raise ValueError("max must be greater than or equal to min")

        return self


class Range(StrictRange, Generic[T]):
    """A range with mandatory min and optional max value."""

    max: T | None = None


class ResourceSpec(BaseModel):
    """Specification of resources, potentially per-core."""

    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)

    overhead: NonNegativeInt = 0
    per_core: NonNegativeInt = Field(default=0, validation_alias="per-core")


class ArchitectureName(StrEnum):
    # Intel/AMD 64-bit
    x86_64 = "x86_64"
    # ARM/AArch64 64-bit
    aarch64 = "aarch64"
    arm64 = "arm64"


class Io(BaseModel):
    """Input/Output resource requirements."""

    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)

    scratch_mb: PositiveInt = Field(validation_alias="scratch-mb")
    # We don't test scratch IOPS because we are unable to accurately obtain
    # and use this data at the moment.
    scratch_iops: PositiveInt = Field(validation_alias="scratch-iops")
