#!/usr/bin/env python3

from __future__ import annotations

from enum import Enum
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


class JobType(Enum):
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


class JobOwner(Enum):
    LBPRODS = "lbprods"


class JobGroup(Enum):
    LHCB_MC = "lhcb_mc"
    LHCB_DATA = "lhcb_data"
    LHCB_MCPROC = "lhcb_mproc"
    LHCB_USER = "lhcb_user"


class SystemName(Enum):
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
    overhead: NonNegativeInt | None = None
    per_core: NonNegativeInt | None = Field(default=None, validation_alias="per-core")


class ArchitectureName(Enum):
    # Intel/AMD 64-bit
    x86_64 = "x86_64"
    # ARM/AArch64 64-bit
    aarch64 = "aarch64"
    arm64 = "arm64"


class Io(BaseModel):
    scratch_mb: PositiveInt = Field(validation_alias="scratch-mb")
    scratch_iops: PositiveInt = Field(validation_alias="scratch-iops")
