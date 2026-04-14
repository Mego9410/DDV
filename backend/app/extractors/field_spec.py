from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FieldSpec:
    field_name: str
    labels: list[str]  # synonyms / variants to search for
    required: bool = False
    value_type: str = "text"  # "text" | "number" | "date" | "percent"

