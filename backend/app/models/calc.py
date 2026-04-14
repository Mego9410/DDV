from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Optional
from uuid import UUID, uuid4

from sqlmodel import Field, SQLModel


class CalcSheetVersion(SQLModel, table=True):
    """
    One normalized "calc" sheet snapshot, usually the latest Update/Calc/Calculation tab.
    """

    __tablename__ = "calc_sheet_versions"

    id: UUID = Field(default_factory=uuid4, primary_key=True, index=True)
    source_file_id: UUID = Field(index=True, foreign_key="uploaded_files.id")
    practice_id: Optional[UUID] = Field(default=None, index=True, foreign_key="practices.id")

    sheet_name: str = Field(index=True)
    as_of_date: Optional[date] = Field(default=None, index=True)
    as_of_date_source: Optional[str] = None  # sheet_name | top_left | inferred

    practice_name_raw: Optional[str] = None
    practice_address_raw: Optional[str] = None

    extraction_confidence: float = Field(default=0.0, index=True)
    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)


class CalcMetric(SQLModel, table=True):
    """
    Long-form metric store to support robust querying even as templates drift.
    """

    __tablename__ = "calc_metrics"

    id: UUID = Field(default_factory=uuid4, primary_key=True, index=True)
    sheet_version_id: UUID = Field(index=True, foreign_key="calc_sheet_versions.id")

    # canonical metric key (stable for queries)
    metric_key: str = Field(index=True)
    # original label text as seen in the sheet
    metric_label: Optional[str] = None

    value_number: Optional[Decimal] = None
    value_text: Optional[str] = None
    unit: Optional[str] = None  # e.g. "gbp", "percent", "x"

    confidence: float = Field(default=0.0, index=True)
    row: Optional[int] = None
    col: Optional[int] = None

    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)

