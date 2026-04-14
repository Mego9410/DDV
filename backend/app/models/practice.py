from __future__ import annotations

from datetime import date, datetime
from typing import Any, Optional
from uuid import UUID, uuid4

from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field, SQLModel


class Practice(SQLModel, table=True):
    __tablename__ = "practices"

    id: UUID = Field(default_factory=uuid4, primary_key=True, index=True)

    # Best-effort identity keys (can be corrected later via review UI)
    display_name: str = Field(index=True)
    address_text: Optional[str] = None
    postcode: Optional[str] = Field(default=None, index=True)

    # Stable upsert key (latest-only ingestion)
    practice_key: str = Field(index=True, unique=True)

    # Analytics-friendly fields
    practice_name: Optional[str] = Field(default=None, index=True)
    county: Optional[str] = Field(default=None, index=True)
    surgery_count: Optional[int] = Field(default=None, index=True)

    associate_cost_amount: Optional[float] = Field(default=None)
    associate_cost_pct: Optional[float] = Field(default=None)  # 0..100
    accounts_period_end: Optional[date] = Field(default=None, index=True)

    source_file: Optional[str] = None

    raw_json: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSONB, nullable=False))

    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)
    updated_at: datetime = Field(default_factory=datetime.utcnow, index=True)

