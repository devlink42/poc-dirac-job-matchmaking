#!/usr/bin/env python3

from __future__ import annotations

from typing import Generic, Self, TypeVar

from pydantic import BaseModel, Field, NonNegativeInt, model_validator

T = TypeVar("T")


class ResourceSpec(BaseModel):
    overhead: NonNegativeInt | None = None
    per_core: NonNegativeInt | None = Field(default=None, validation_alias="per-core")


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
