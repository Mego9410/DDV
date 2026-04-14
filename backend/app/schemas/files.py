from __future__ import annotations

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel

from app.models.enums import ProcessingStatus


class UploadedFileRead(BaseModel):
    id: UUID
    original_filename: str
    content_type: Optional[str]
    byte_size: Optional[int]
    sha256: Optional[str]
    status: ProcessingStatus
    status_message: Optional[str]
    uploaded_at: datetime
    processed_at: Optional[datetime]


class UploadResponse(BaseModel):
    file: UploadedFileRead

