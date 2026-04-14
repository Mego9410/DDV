from __future__ import annotations

from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import Session, select

from app.db.session import get_session
from app.models import CalcMetric, CalcSheetVersion, Practice
from app.schemas.calc import CalcMetricPoint, CalcMetricRead, CalcSheetVersionRead, PracticeRead

router = APIRouter()


@router.get("/practices", response_model=list[PracticeRead])
def list_practices(session: Session = Depends(get_session)) -> list[PracticeRead]:
    rows = session.exec(select(Practice)).all()
    return [
        PracticeRead(
            id=p.id,
            display_name=p.display_name,
            address_text=p.address_text,
            postcode=p.postcode,
            created_at=p.created_at,
        )
        for p in rows
    ]


@router.get("/sheet-versions", response_model=list[CalcSheetVersionRead])
def list_calc_sheet_versions(
    practice_id: Optional[UUID] = None,
    file_id: Optional[UUID] = None,
    session: Session = Depends(get_session),
) -> list[CalcSheetVersionRead]:
    stmt = select(CalcSheetVersion)
    if practice_id is not None:
        stmt = stmt.where(CalcSheetVersion.practice_id == practice_id)
    if file_id is not None:
        stmt = stmt.where(CalcSheetVersion.source_file_id == file_id)
    rows = session.exec(stmt).all()
    return [
        CalcSheetVersionRead(
            id=r.id,
            source_file_id=r.source_file_id,
            practice_id=r.practice_id,
            sheet_name=r.sheet_name,
            as_of_date=r.as_of_date,
            as_of_date_source=r.as_of_date_source,
            practice_name_raw=r.practice_name_raw,
            practice_address_raw=r.practice_address_raw,
            extraction_confidence=r.extraction_confidence,
            created_at=r.created_at,
        )
        for r in rows
    ]


@router.get("/metrics", response_model=list[CalcMetricRead])
def list_metrics(
    sheet_version_id: UUID,
    prefix: Optional[str] = None,
    session: Session = Depends(get_session),
) -> list[CalcMetricRead]:
    stmt = select(CalcMetric).where(CalcMetric.sheet_version_id == sheet_version_id)
    rows = session.exec(stmt).all()
    if prefix:
        rows = [m for m in rows if m.metric_key.startswith(prefix)]
    return [
        CalcMetricRead(
            id=m.id,
            sheet_version_id=m.sheet_version_id,
            metric_key=m.metric_key,
            metric_label=m.metric_label,
            value_number=m.value_number,
            value_text=m.value_text,
            unit=m.unit,
            confidence=m.confidence,
            row=m.row,
            col=m.col,
            created_at=m.created_at,
        )
        for m in rows
    ]


@router.get("/metrics/timeseries", response_model=list[CalcMetricPoint])
def metric_timeseries(
    metric_key: str,
    practice_id: Optional[UUID] = None,
    session: Session = Depends(get_session),
) -> list[CalcMetricPoint]:
    # Join via two-step for SQLModel simplicity in MVP
    versions_stmt = select(CalcSheetVersion)
    if practice_id is not None:
        versions_stmt = versions_stmt.where(CalcSheetVersion.practice_id == practice_id)
    versions = session.exec(versions_stmt).all()
    if not versions:
        return []

    version_by_id = {v.id: v for v in versions}
    practice_by_id: dict[UUID, Practice] = {p.id: p for p in session.exec(select(Practice)).all()}

    metrics = session.exec(select(CalcMetric).where(CalcMetric.metric_key == metric_key)).all()
    metrics = [m for m in metrics if m.sheet_version_id in version_by_id]

    points: list[CalcMetricPoint] = []
    for m in metrics:
        v = version_by_id[m.sheet_version_id]
        p = practice_by_id.get(v.practice_id) if v.practice_id else None
        points.append(
            CalcMetricPoint(
                as_of_date=v.as_of_date,
                value_number=m.value_number,
                value_text=m.value_text,
                unit=m.unit,
                practice_id=v.practice_id,
                practice_name=p.display_name if p else v.practice_name_raw,
                sheet_version_id=v.id,
            )
        )

    points.sort(key=lambda x: (x.practice_name or "", x.as_of_date or _min_date()))
    return points


@router.get("/metrics/latest", response_model=list[CalcMetricPoint])
def metric_latest_across_practices(
    metric_key: str,
    session: Session = Depends(get_session),
) -> list[CalcMetricPoint]:
    versions = session.exec(select(CalcSheetVersion)).all()
    if not versions:
        return []
    practice_by_id: dict[UUID, Practice] = {p.id: p for p in session.exec(select(Practice)).all()}

    # pick latest version per practice (or per file if practice unknown)
    latest_version: dict[str, CalcSheetVersion] = {}
    for v in versions:
        key = str(v.practice_id) if v.practice_id else f"file:{v.source_file_id}"
        prev = latest_version.get(key)
        if prev is None or (v.as_of_date or _min_date()) > (prev.as_of_date or _min_date()):
            latest_version[key] = v

    version_ids = {v.id for v in latest_version.values()}
    metrics = session.exec(select(CalcMetric).where(CalcMetric.metric_key == metric_key)).all()
    metrics = [m for m in metrics if m.sheet_version_id in version_ids]
    metric_by_version = {m.sheet_version_id: m for m in metrics}

    out: list[CalcMetricPoint] = []
    for v in latest_version.values():
        m = metric_by_version.get(v.id)
        if not m:
            continue
        p = practice_by_id.get(v.practice_id) if v.practice_id else None
        out.append(
            CalcMetricPoint(
                as_of_date=v.as_of_date,
                value_number=m.value_number,
                value_text=m.value_text,
                unit=m.unit,
                practice_id=v.practice_id,
                practice_name=p.display_name if p else v.practice_name_raw,
                sheet_version_id=v.id,
            )
        )

    out.sort(key=lambda x: (x.value_number or 0), reverse=True)
    return out


def _min_date():
    from datetime import date

    return date(1900, 1, 1)

