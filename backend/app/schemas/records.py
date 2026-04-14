from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Optional
from uuid import UUID

from pydantic import BaseModel


class ExtractedRecordRead(BaseModel):
    id: UUID
    source_file_id: UUID
    sheet_name: str

    reporting_date: Optional[date]
    entity_name: Optional[str]
    category: Optional[str]

    revenue: Optional[Decimal]
    cost: Optional[Decimal]
    gross_profit: Optional[Decimal]
    margin: Optional[float]

    notes: Optional[str]
    extraction_confidence: float
    created_at: datetime

