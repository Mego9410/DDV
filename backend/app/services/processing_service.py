from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

import pandas as pd
from sqlmodel import Session, select

from app.core.config import get_settings
from app.extractors import FieldSpec, LabelSearchExtractor
from app.models import (
    CalcMetric,
    CalcSheetVersion,
    ExtractionIssue,
    ExtractedRecord,
    IssueSeverity,
    Practice,
    ProcessingStatus,
    UploadedFile,
)
from app.services.calc_metrics_extractor import CalcMetricsExtractor
from app.services.calc_sheet_selector import CalcSheetSelector
from app.services.workbook_reader import WorkbookReader
from app.validators import RecordValidator, to_date, to_decimal, to_percent_0_1, to_text


@dataclass(frozen=True)
class ProcessResult:
    file_id: UUID
    records_created: int
    issues_created: int
    status: ProcessingStatus


class ProcessingService:
    """
    Orchestrates: read workbook -> extract fields -> normalize -> validate -> persist.
    MVP runs synchronously in API request; can be moved to a queue later.
    """

    def __init__(self) -> None:
        self._settings = get_settings()
        self._reader = WorkbookReader()
        self._extractor = LabelSearchExtractor()
        self._validator = RecordValidator()
        self._calc_selector = CalcSheetSelector()
        self._calc_extractor = CalcMetricsExtractor()

        # MVP specs; expand later or make configurable per template.
        self._specs: list[FieldSpec] = [
            FieldSpec("reporting_date", ["reporting date", "date", "period end", "as at"], required=True, value_type="date"),
            FieldSpec("entity_name", ["entity", "entity name", "company", "customer", "organisation", "organization"], required=True),
            FieldSpec("category", ["category", "segment", "division", "type"]),
            FieldSpec("revenue", ["revenue", "sales", "turnover", "income"], value_type="number"),
            FieldSpec("cost", ["cost", "cogs", "cost of sales", "expenses"], value_type="number"),
            FieldSpec("gross_profit", ["gross profit", "profit", "gross margin"], value_type="number"),
            FieldSpec("margin", ["margin", "gross margin %", "gross margin"], value_type="percent"),
            FieldSpec("notes", ["notes", "comment", "comments", "remark", "remarks"], value_type="text"),
        ]

    def process_file(self, *, session: Session, file_id: UUID) -> ProcessResult:
        f = session.exec(select(UploadedFile).where(UploadedFile.id == file_id)).one()
        f.status = ProcessingStatus.processing
        f.status_message = None
        session.add(f)
        session.commit()

        issues_created = 0
        records_created = 0

        try:
            path = self._resolve_storage_path(f.storage_path)
            # MVP v2: Calc-only processing (ignore Staff/EF&F/Clinical/etc.)
            selected = self._calc_selector.select(path)
            if selected is None:
                session.add(
                    ExtractionIssue(
                        source_file_id=f.id,
                        record_id=None,
                        sheet_name=None,
                        field_name=None,
                        severity=IssueSeverity.error,
                        code="no_calc_sheet_found",
                        message="Could not find a Calc/Calculation/Update sheet with practice header in top-left.",
                    )
                )
                issues_created += 1
            else:
                # Read selected sheet grid
                xl = pd.ExcelFile(path)
                df = xl.parse(sheet_name=selected.sheet_name, header=None, dtype=object)

                practice_name, practice_address, header_conf = self._calc_extractor.extract_practice_header(df)
                practice = self._upsert_practice(
                    session=session,
                    practice_name=practice_name,
                    practice_address=practice_address,
                )

                sheet_version = CalcSheetVersion(
                    source_file_id=f.id,
                    practice_id=practice.id if practice else None,
                    sheet_name=selected.sheet_name,
                    as_of_date=selected.as_of_date,
                    as_of_date_source=selected.as_of_date_source,
                    practice_name_raw=practice_name,
                    practice_address_raw=practice_address,
                    extraction_confidence=float((0.5 * header_conf) + (0.5 * min(1.0, selected.score / 12.0))),
                )
                session.add(sheet_version)
                session.commit()
                session.refresh(sheet_version)
                records_created += 1

                metrics = self._calc_extractor.extract_metrics(df)
                for m in metrics:
                    session.add(
                        CalcMetric(
                            sheet_version_id=sheet_version.id,
                            metric_key=m.metric_key,
                            metric_label=m.metric_label,
                            value_number=m.value_number,
                            value_text=m.value_text,
                            unit=m.unit,
                            confidence=m.confidence,
                            row=m.row,
                            col=m.col,
                        )
                    )
                session.commit()

                # Warnings if header is missing
                if not practice_name:
                    session.add(
                        ExtractionIssue(
                            source_file_id=f.id,
                            record_id=None,
                            sheet_name=selected.sheet_name,
                            field_name="practice_name",
                            severity=IssueSeverity.warning,
                            code="missing_practice_name",
                            message="Practice name not detected in top-left header block.",
                        )
                    )
                    issues_created += 1
                if not practice_address:
                    session.add(
                        ExtractionIssue(
                            source_file_id=f.id,
                            record_id=None,
                            sheet_name=selected.sheet_name,
                            field_name="practice_address",
                            severity=IssueSeverity.warning,
                            code="missing_practice_address",
                            message="Practice address not detected in top-left header block.",
                        )
                    )
                    issues_created += 1

                # Low-confidence canonical metrics warning (only for known keys)
                for m in metrics:
                    if not m.metric_key.startswith("raw:") and m.confidence < self._settings.low_confidence_threshold:
                        session.add(
                            ExtractionIssue(
                                source_file_id=f.id,
                                record_id=None,
                                sheet_name=selected.sheet_name,
                                field_name=m.metric_key,
                                severity=IssueSeverity.warning,
                                code="low_confidence_metric",
                                message=f"Low confidence for metric '{m.metric_key}' ({m.confidence:.2f}).",
                                details_json=_safe_json({"label": m.metric_label, "row": m.row, "col": m.col}),
                            )
                        )
                        issues_created += 1

                session.commit()

            # determine file-level status
            final_status = ProcessingStatus.completed if issues_created == 0 else ProcessingStatus.completed_with_warnings
            f.status = final_status
            f.processed_at = datetime.utcnow()
            session.add(f)
            session.commit()

            return ProcessResult(file_id=file_id, records_created=records_created, issues_created=issues_created, status=final_status)
        except Exception as e:
            session.add(
                ExtractionIssue(
                    source_file_id=f.id,
                    record_id=None,
                    sheet_name=None,
                    field_name=None,
                    severity=IssueSeverity.error,
                    code="processing_failed",
                    message=str(e),
                )
            )
            f.status = ProcessingStatus.failed
            f.status_message = str(e)
            f.processed_at = datetime.utcnow()
            session.add(f)
            session.commit()
            return ProcessResult(file_id=file_id, records_created=records_created, issues_created=issues_created + 1, status=ProcessingStatus.failed)

    def _upsert_practice(
        self,
        *,
        session: Session,
        practice_name: str | None,
        practice_address: str | None,
    ) -> Practice | None:
        name = to_text(practice_name)
        addr = to_text(practice_address)
        if not name and not addr:
            return None
        # naive MVP identity: exact match on name+address
        stmt = select(Practice)
        if name:
            stmt = stmt.where(Practice.display_name == name)
        if addr:
            stmt = stmt.where(Practice.address_text == addr)
        existing = session.exec(stmt).one_or_none()
        if existing:
            return existing
        p = Practice(display_name=name or "Unknown practice", address_text=addr, postcode=_extract_postcode(addr))
        session.add(p)
        session.commit()
        session.refresh(p)
        return p

    def _normalize_and_validate(
        self,
        *,
        source_file_id: UUID,
        sheet_name: str,
        extracted: list[Any],
    ) -> tuple[ExtractedRecord, list[Any]]:
        by_name = {e.field_name: e for e in extracted}

        reporting_date = to_date(by_name.get("reporting_date").raw_value if "reporting_date" in by_name else None)
        entity_name = to_text(by_name.get("entity_name").raw_value if "entity_name" in by_name else None)
        category = to_text(by_name.get("category").raw_value if "category" in by_name else None)

        revenue = to_decimal(by_name.get("revenue").raw_value if "revenue" in by_name else None)
        cost = to_decimal(by_name.get("cost").raw_value if "cost" in by_name else None)
        gross_profit = to_decimal(by_name.get("gross_profit").raw_value if "gross_profit" in by_name else None)
        margin = to_percent_0_1(by_name.get("margin").raw_value if "margin" in by_name else None)

        notes = to_text(by_name.get("notes").raw_value if "notes" in by_name else None)

        conf = float(sum(e.confidence for e in extracted) / max(1, len(extracted)))

        record = ExtractedRecord(
            source_file_id=source_file_id,
            sheet_name=sheet_name,
            reporting_date=reporting_date,
            entity_name=entity_name,
            category=category,
            revenue=revenue,
            cost=cost,
            gross_profit=gross_profit,
            margin=margin,
            notes=notes,
            extraction_confidence=conf,
        )

        issues = self._validator.validate(
            reporting_date_present=reporting_date is not None,
            entity_name_present=entity_name is not None,
            revenue=revenue,
            cost=cost,
            gross_profit=gross_profit,
            margin_0_1=margin,
        )
        return record, issues

    def _resolve_storage_path(self, storage_path: str):
        # storage_path is persisted as string; treat as absolute/volume path inside container.
        from pathlib import Path

        p = Path(storage_path)
        if p.is_absolute():
            return p
        return self._settings.uploads_dir.parent / p


def _jsonable(v: Any) -> Any:
    # Best-effort make value serializable for details_json.
    try:
        if v is None:
            return None
        if isinstance(v, (str, int, float, bool)):
            return v
        return str(v)
    except Exception:
        return "<unserializable>"


def _safe_json(obj: Any) -> str:
    import json

    return json.dumps(obj, ensure_ascii=False, default=str)


def _extract_postcode(addr: str | None) -> str | None:
    if not addr:
        return None
    m = re.search(r"\b([A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})\b", addr.upper())
    return m.group(1) if m else None

