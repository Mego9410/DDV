from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Optional
from uuid import UUID, uuid4

from sqlmodel import Field, SQLModel


class ExtractedRecord(SQLModel, table=True):
    """
    Normalized master record (expandable).
    For MVP, we store a single normalized row per sheet (or per detected entity block).
    """

    __tablename__ = "extracted_records"

    id: UUID = Field(default_factory=uuid4, primary_key=True, index=True)

    source_file_id: UUID = Field(index=True, foreign_key="uploaded_files.id")
    sheet_name: str = Field(index=True)

    reporting_date: Optional[date] = Field(default=None, index=True)
    entity_name: Optional[str] = Field(default=None, index=True)
    category: Optional[str] = Field(default=None, index=True)

    revenue: Optional[Decimal] = None
    cost: Optional[Decimal] = None
    gross_profit: Optional[Decimal] = None
    margin: Optional[float] = None  # normalized 0..1 for MVP

    notes: Optional[str] = None
    extraction_confidence: float = Field(default=0.0, index=True)

    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)

