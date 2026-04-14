from __future__ import annotations

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel

from app.models.enums import IssueSeverity


class ExtractionIssueRead(BaseModel):
    id: UUID
    source_file_id: UUID
    record_id: Optional[UUID]
    sheet_name: Optional[str]
    field_name: Optional[str]
    severity: IssueSeverity
    code: str
    message: str
    details_json: Optional[str]
    created_at: datetime

