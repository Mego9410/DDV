from __future__ import annotations

from datetime import datetime
from typing import Optional
from uuid import UUID, uuid4

from sqlmodel import Field, SQLModel

from app.models.enums import ProcessingStatus


class UploadedFile(SQLModel, table=True):
    __tablename__ = "uploaded_files"

    id: UUID = Field(default_factory=uuid4, primary_key=True, index=True)

    original_filename: str
    content_type: Optional[str] = None
    byte_size: Optional[int] = None
    sha256: Optional[str] = None

    storage_path: str

    status: ProcessingStatus = Field(default=ProcessingStatus.uploaded, index=True)
    status_message: Optional[str] = None

    uploaded_at: datetime = Field(default_factory=datetime.utcnow, index=True)
    processed_at: Optional[datetime] = Field(default=None, index=True)

