#!/usr/bin/env python3

from __future__ import annotations

from enum import Enum
from typing import Annotated, Any, Generic, Self, TypeVar

from packaging.version import Version
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


class VersionPydanticAnnotation:
    @classmethod
    def __get_pydantic_core_schema__(cls, source_type: Any, handler: GetCoreSchemaHandler) -> core_schema.CoreSchema:
        def validate(value: Any) -> Version:
            if isinstance(value, Version):
                return value

            return Version(str(value))

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


class Io(BaseModel):
    scratch_mb: PositiveInt = Field(validation_alias="scratch-mb")
    lan_mbitps: PositiveInt | None = Field(default=None, validation_alias="lan-mbitps")


class ArchitectureName(Enum):
    # Intel/AMD 64-bit
    x86_64 = "x86_64"
    # ARM/AArch64 64-bit
    aarch64 = "aarch64"
    # PowerPC 64-bit
    ppc64 = "ppc64"
