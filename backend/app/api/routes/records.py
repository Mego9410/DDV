from __future__ import annotations

from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends
from sqlmodel import Session, select

from app.db.session import get_session
from app.models import ExtractedRecord
from app.schemas.records import ExtractedRecordRead

router = APIRouter()


@router.get("", response_model=list[ExtractedRecordRead])
def list_records(
    file_id: Optional[UUID] = None,
    session: Session = Depends(get_session),
) -> list[ExtractedRecordRead]:
    stmt = select(ExtractedRecord)
    if file_id is not None:
        stmt = stmt.where(ExtractedRecord.source_file_id == file_id)
    rows = session.exec(stmt).all()
    return [
        ExtractedRecordRead(
            id=r.id,
            source_file_id=r.source_file_id,
            sheet_name=r.sheet_name,
            reporting_date=r.reporting_date,
            entity_name=r.entity_name,
            category=r.category,
            revenue=r.revenue,
            cost=r.cost,
            gross_profit=r.gross_profit,
            margin=r.margin,
            notes=r.notes,
            extraction_confidence=r.extraction_confidence,
            created_at=r.created_at,
        )
        for r in rows
    ]

