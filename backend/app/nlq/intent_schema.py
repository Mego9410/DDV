from __future__ import annotations

from enum import Enum
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, field_validator


class Metric(str, Enum):
    associate_cost_amount = "associate_cost_amount"
    associate_cost_pct = "associate_cost_pct"


class Agg(str, Enum):
    avg = "avg"
    median = "median"
    min = "min"
    max = "max"
    count = "count"


class FilterField(str, Enum):
    county = "county"
    surgery_count = "surgery_count"
    accounts_period_end = "accounts_period_end"


class FilterOp(str, Enum):
    eq = "="
    in_ = "in"
    gte = ">="
    lte = "<="
    between = "between"


class Filter(BaseModel):
    field: FilterField
    op: FilterOp
    value: Any


class QueryIntent(BaseModel):
    metric: Metric
    agg: Agg
    filters: list[Filter] = Field(default_factory=list)
    group_by: list[FilterField] = Field(default_factory=list)
    limit: int = 100

    @field_validator("limit")
    @classmethod
    def _limit_bounds(cls, v: int) -> int:
        return max(1, min(int(v), 1000))

