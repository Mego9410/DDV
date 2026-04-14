from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from sqlmodel import Session, select

from app.db.session import get_engine
from app.models.logs import ExtractionLog
from app.models.practice import Practice
from app.validators.normalizers import to_date, to_text, to_decimal


def _load_payload(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _get_materialized(payload: dict[str, Any]) -> dict[str, Any]:
    return payload.get("materialized") or payload.get("extraction", {}).get("materialized") or {}


def _get_practice(payload: dict[str, Any]) -> dict[str, Any]:
    return payload.get("practice") or payload.get("extraction", {}).get("practice") or {}


def _incoming_period_end(payload: dict[str, Any]) -> Optional[datetime.date]:
    mat = payload.get("materialized") or {}
    d = mat.get("accounts_period_end")
    return to_date(d)


def _to_float(v: Any) -> Optional[float]:
    d = to_decimal(v)
    return float(d) if d is not None else None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-dir", default="out/practices", help="Directory of per-practice JSON files")
    ap.add_argument("--glob", default="*.json", help="Glob pattern within in-dir")
    ap.add_argument("--dry-run", action="store_true", help="Do not write to the database")
    args = ap.parse_args()

    in_dir = Path(args.in_dir)
    files = sorted(in_dir.glob(args.glob))
    if not files:
        raise SystemExit(f"No files found in {in_dir} matching {args.glob}")

    engine = get_engine()
    now = datetime.utcnow()

    updated = 0
    skipped = 0
    errors = 0

    with Session(engine) as session:
        for f in files:
            try:
                payload = _load_payload(f)
                mat = payload.get("materialized") or {}
                pr = payload.get("practice") or {}
                extraction = payload.get("extraction") or {}
                field_confidence = extraction.get("field_confidence") or {}
                evidence = extraction.get("evidence") or {}

                practice_key = to_text(mat.get("practice_key")) or to_text(payload.get("practice_key"))
                if not practice_key:
                    raise ValueError("Missing practice_key")

                incoming_end = to_date(mat.get("accounts_period_end"))

                existing = session.exec(select(Practice).where(Practice.practice_key == practice_key)).first()
                should_update = False
                if existing is None:
                    should_update = True
                else:
                    if existing.accounts_period_end is None and incoming_end is not None:
                        should_update = True
                    elif existing.accounts_period_end is None and incoming_end is None:
                        # Update anyway to capture raw_json refresh
                        should_update = True
                    elif incoming_end is not None and existing.accounts_period_end is not None and incoming_end > existing.accounts_period_end:
                        should_update = True

                # Always write extraction log entry
                elog = ExtractionLog(
                    practice_key=practice_key,
                    source_file=to_text(payload.get("source_file")),
                    accounts_period_end=incoming_end,
                    field_confidence=field_confidence,
                    missing_fields=extraction.get("missing_fields") or [],
                    low_conf_fields=extraction.get("low_conf_fields") or [],
                    evidence=evidence,
                    notes=None,
                )

                if not args.dry_run:
                    session.add(elog)

                if should_update:
                    if existing is None:
                        existing = Practice(
                            practice_key=practice_key,
                            display_name=to_text(pr.get("practice_name")) or to_text(payload.get("display_name")) or practice_key.split("|")[0],
                        )
                        if not args.dry_run:
                            session.add(existing)

                    existing.practice_name = to_text(pr.get("practice_name"))
                    existing.address_text = to_text(pr.get("address_text"))
                    existing.postcode = to_text(pr.get("postcode"))
                    existing.county = to_text(pr.get("county"))
                    existing.surgery_count = mat.get("surgery_count")
                    existing.associate_cost_amount = _to_float(mat.get("associate_cost_amount"))
                    existing.associate_cost_pct = _to_float(mat.get("associate_cost_pct"))
                    existing.accounts_period_end = incoming_end
                    existing.source_file = to_text(payload.get("source_file"))
                    existing.raw_json = payload
                    existing.updated_at = now

                    updated += 1
                else:
                    skipped += 1

                if not args.dry_run:
                    session.commit()
            except Exception:
                errors += 1
                if not args.dry_run:
                    session.rollback()

        print(json.dumps({"updated": updated, "skipped": skipped, "errors": errors}, indent=2))


if __name__ == "__main__":
    main()

