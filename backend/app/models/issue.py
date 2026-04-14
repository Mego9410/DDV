from __future__ import annotations

from datetime import datetime
from typing import Optional
from uuid import UUID, uuid4

from sqlmodel import Field, SQLModel

from app.models.enums import IssueSeverity


class ExtractionIssue(SQLModel, table=True):
    __tablename__ = "extraction_issues"

    id: UUID = Field(default_factory=uuid4, primary_key=True, index=True)

    source_file_id: UUID = Field(index=True, foreign_key="uploaded_files.id")
    record_id: Optional[UUID] = Field(default=None, index=True)  # may refer to different entities in future

    sheet_name: Optional[str] = Field(default=None, index=True)
    field_name: Optional[str] = Field(default=None, index=True)

    severity: IssueSeverity = Field(default=IssueSeverity.warning, index=True)
    code: str = Field(index=True)
    message: str
    details_json: Optional[str] = None

    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)

