from __future__ import annotations

from datetime import datetime, date
from typing import Any, Optional
from uuid import UUID, uuid4

from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlmodel import Field, SQLModel


class ExtractionLog(SQLModel, table=True):
    __tablename__ = "extraction_log"

    id: UUID = Field(default_factory=uuid4, primary_key=True, index=True)
    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)

    practice_key: str = Field(index=True)
    source_file: Optional[str] = None
    accounts_period_end: Optional[date] = Field(default=None, index=True)

    field_confidence: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSONB, nullable=False))
    missing_fields: list[str] = Field(default_factory=list, sa_column=Column(ARRAY(str), nullable=False))
    low_conf_fields: list[str] = Field(default_factory=list, sa_column=Column(ARRAY(str), nullable=False))
    evidence: Optional[dict[str, Any]] = Field(default=None, sa_column=Column(JSONB, nullable=True))
    notes: Optional[str] = None


class RequestLog(SQLModel, table=True):
    __tablename__ = "request_log"

    id: UUID = Field(default_factory=uuid4, primary_key=True, index=True)
    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)

    request_type: str = Field(default="nlq", index=True)
    query_text: str

    intent: Optional[dict[str, Any]] = Field(default=None, sa_column=Column(JSONB, nullable=True))
    sql_template: Optional[str] = None
    params: Optional[dict[str, Any]] = Field(default=None, sa_column=Column(JSONB, nullable=True))

    status: str = Field(default="ok", index=True)  # ok|no_results|error|blocked
    row_count: Optional[int] = None
    latency_ms: Optional[int] = None
    warnings: list[str] = Field(default_factory=list, sa_column=Column(ARRAY(str), nullable=False))
    error_message: Optional[str] = None

