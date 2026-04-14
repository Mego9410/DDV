from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Optional
from uuid import UUID

from pydantic import BaseModel


class PracticeRead(BaseModel):
    id: UUID
    display_name: str
    address_text: Optional[str]
    postcode: Optional[str]
    created_at: datetime


class CalcSheetVersionRead(BaseModel):
    id: UUID
    source_file_id: UUID
    practice_id: Optional[UUID]
    sheet_name: str
    as_of_date: Optional[date]
    as_of_date_source: Optional[str]
    practice_name_raw: Optional[str]
    practice_address_raw: Optional[str]
    extraction_confidence: float
    created_at: datetime


class CalcMetricRead(BaseModel):
    id: UUID
    sheet_version_id: UUID
    metric_key: str
    metric_label: Optional[str]
    value_number: Optional[Decimal]
    value_text: Optional[str]
    unit: Optional[str]
    confidence: float
    row: Optional[int]
    col: Optional[int]
    created_at: datetime


class CalcMetricPoint(BaseModel):
    as_of_date: Optional[date]
    value_number: Optional[Decimal]
    value_text: Optional[str]
    unit: Optional[str]
    practice_id: Optional[UUID]
    practice_name: Optional[str]
    sheet_version_id: UUID

