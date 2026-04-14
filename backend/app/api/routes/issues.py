from __future__ import annotations

from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends
from sqlmodel import Session, select

from app.db.session import get_session
from app.models import ExtractionIssue
from app.schemas.issues import ExtractionIssueRead

router = APIRouter()


@router.get("", response_model=list[ExtractionIssueRead])
def list_issues(
    file_id: Optional[UUID] = None,
    record_id: Optional[UUID] = None,
    session: Session = Depends(get_session),
) -> list[ExtractionIssueRead]:
    stmt = select(ExtractionIssue)
    if file_id is not None:
        stmt = stmt.where(ExtractionIssue.source_file_id == file_id)
    if record_id is not None:
        stmt = stmt.where(ExtractionIssue.record_id == record_id)
    rows = session.exec(stmt).all()
    return [
        ExtractionIssueRead(
            id=i.id,
            source_file_id=i.source_file_id,
            record_id=i.record_id,
            sheet_name=i.sheet_name,
            field_name=i.field_name,
            severity=i.severity,
            code=i.code,
            message=i.message,
            details_json=i.details_json,
            created_at=i.created_at,
        )
        for i in rows
    ]

