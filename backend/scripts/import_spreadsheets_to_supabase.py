from __future__ import annotations

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from sqlmodel import Session, select

# Ensure `backend/` is on sys.path so `import app...` works even when invoked as:
#   python scripts/<file>.py
BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from app.db.session import get_engine
from app.models.logs import ExtractionLog
from app.models.practice import Practice
from app.services.practice_latest_extractor import PracticeLatestExtractor, PracticeLatestResult
from app.validators.normalizers import to_date, to_decimal, to_text


def _iter_input_paths(input_path: Path, *, glob: str, recursive: bool) -> list[Path]:
    """
    Accept either:
    - a single file path (e.g. one .xlsx)
    - a directory (scan by glob)
    """
    if not input_path.exists():
        raise SystemExit(f"Input path not found: {input_path}")
    if input_path.is_file():
        return [input_path]
    if not input_path.is_dir():
        raise SystemExit(f"Input path must be a file or directory: {input_path}")
    it = input_path.rglob(glob) if recursive else input_path.glob(glob)
    return sorted([p for p in it if p.is_file()])


def _to_float(v: Any) -> Optional[float]:
    d = to_decimal(v)
    return float(d) if d is not None else None


def _extract_one(
    p: Path, *, canonical_mapping_path: str | None, low_conf_threshold: float
) -> tuple[Path, str, Optional[str], str, Optional[PracticeLatestResult]]:
    """
    Returns: (path, status, error, elapsed_ms, result)
    """
    t0 = time.time()
    extractor = PracticeLatestExtractor(
        canonical_mapping_path=canonical_mapping_path,
        low_conf_threshold=low_conf_threshold,
    )
    try:
        r = extractor.extract(p)
        return p, "ok", None, str(int((time.time() - t0) * 1000)), r
    except Exception as e:
        return p, "error", str(e), str(int((time.time() - t0) * 1000)), None


def _should_update(existing: Optional[Practice], incoming_end: Optional[datetime.date]) -> bool:
    if existing is None:
        return True
    if existing.accounts_period_end is None and incoming_end is not None:
        return True
    if existing.accounts_period_end is None and incoming_end is None:
        # Update anyway to refresh raw_json / extracted fields
        return True
    if incoming_end is not None and existing.accounts_period_end is not None and incoming_end > existing.accounts_period_end:
        return True
    return False


def _ingest_one(session: Session, result: PracticeLatestResult, *, now: datetime, dry_run: bool, force: bool) -> str:
    payload = result.raw_json
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
    should_update = force or _should_update(existing, incoming_end)
    if (not should_update) and existing is not None:
        # Backfill newly-added columns even when period_end hasn't advanced.
        backfill_fields = [
            "city",
            "address_line1",
            "address_line2",
            "goodwill",
            "efandf",
            "total",
            "freehold",
            "grand_total",
            "nhs_contract_number",
            "uda_contract_value_gbp",
            "uda_count",
            "uda_rate_gbp",
            "uda_uplift_value_gbp",
            "income_split_fpi_percent",
            "income_split_fpi_value",
            "income_split_fpi_applied_percent",
            "income_split_fpi_applied_value",
            "income_split_nhs_percent",
            "income_split_nhs_value",
            "income_split_nhs_applied_percent",
            "income_split_nhs_applied_value",
            "income_split_denplan_percent",
            "income_split_denplan_value",
            "income_split_denplan_applied_percent",
            "income_split_denplan_applied_value",
            "income_split_rent_percent",
            "income_split_rent_value",
            "income_split_rent_applied_percent",
            "income_split_rent_applied_value",
        ]
        for f in backfill_fields:
            incoming_val = mat.get(f) if f in mat else pr.get(f)
            if incoming_val is None:
                continue
            if getattr(existing, f, None) is None:
                should_update = True
                break

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
    if not dry_run:
        session.add(elog)

    if not should_update:
        return "skipped"

    if existing is None:
        existing = Practice(
            practice_key=practice_key,
            display_name=to_text(pr.get("practice_name")) or to_text(payload.get("display_name")) or practice_key.split("|")[0],
        )
        if not dry_run:
            session.add(existing)

    existing.practice_name = to_text(pr.get("practice_name"))
    existing.address_text = to_text(pr.get("address_text"))
    existing.postcode = to_text(pr.get("postcode"))
    existing.county = to_text(pr.get("county"))
    existing.city = to_text(pr.get("city"))
    existing.address_line1 = to_text(pr.get("address_line1"))
    existing.address_line2 = to_text(pr.get("address_line2"))
    existing.visited_on = to_date(mat.get("visited_on"))
    existing.surgery_count = mat.get("surgery_count")

    # Core valuation metrics
    existing.goodwill = _to_float(mat.get("goodwill"))
    existing.efandf = _to_float(mat.get("efandf"))
    existing.total = _to_float(mat.get("total"))
    existing.freehold = _to_float(mat.get("freehold"))
    existing.grand_total = _to_float(mat.get("grand_total"))

    # NHS contract details
    existing.nhs_contract_number = to_text(mat.get("nhs_contract_number"))
    existing.uda_contract_value_gbp = _to_float(mat.get("uda_contract_value_gbp"))
    existing.uda_count = _to_float(mat.get("uda_count"))
    existing.uda_rate_gbp = _to_float(mat.get("uda_rate_gbp"))
    existing.uda_uplift_value_gbp = _to_float(mat.get("uda_uplift_value_gbp"))

    # Income split (selected common types)
    existing.income_split_fpi_percent = _to_float(mat.get("income_split_fpi_percent"))
    existing.income_split_fpi_value = _to_float(mat.get("income_split_fpi_value"))
    existing.income_split_fpi_applied_percent = _to_float(mat.get("income_split_fpi_applied_percent"))
    existing.income_split_fpi_applied_value = _to_float(mat.get("income_split_fpi_applied_value"))

    existing.income_split_nhs_percent = _to_float(mat.get("income_split_nhs_percent"))
    existing.income_split_nhs_value = _to_float(mat.get("income_split_nhs_value"))
    existing.income_split_nhs_applied_percent = _to_float(mat.get("income_split_nhs_applied_percent"))
    existing.income_split_nhs_applied_value = _to_float(mat.get("income_split_nhs_applied_value"))

    existing.income_split_denplan_percent = _to_float(mat.get("income_split_denplan_percent"))
    existing.income_split_denplan_value = _to_float(mat.get("income_split_denplan_value"))
    existing.income_split_denplan_applied_percent = _to_float(mat.get("income_split_denplan_applied_percent"))
    existing.income_split_denplan_applied_value = _to_float(mat.get("income_split_denplan_applied_value"))

    existing.income_split_rent_percent = _to_float(mat.get("income_split_rent_percent"))
    existing.income_split_rent_value = _to_float(mat.get("income_split_rent_value"))
    existing.income_split_rent_applied_percent = _to_float(mat.get("income_split_rent_applied_percent"))
    existing.income_split_rent_applied_value = _to_float(mat.get("income_split_rent_applied_value"))

    existing.associate_cost_amount = _to_float(mat.get("associate_cost_amount"))
    existing.associate_cost_pct = _to_float(mat.get("associate_cost_pct"))
    existing.accounts_period_end = incoming_end

    # Certified accounts (latest + prev)
    existing.certified_accounts_period_end_prev = to_date(mat.get("certified_accounts_period_end_prev"))

    existing.cert_income_gbp = _to_float(mat.get("cert_income_gbp"))
    existing.cert_income_percent = _to_float(mat.get("cert_income_percent"))
    existing.cert_income_gbp_prev = _to_float(mat.get("cert_income_gbp_prev"))
    existing.cert_income_percent_prev = _to_float(mat.get("cert_income_percent_prev"))

    existing.cert_other_inc_gbp = _to_float(mat.get("cert_other_inc_gbp"))
    existing.cert_other_inc_percent = _to_float(mat.get("cert_other_inc_percent"))
    existing.cert_other_inc_gbp_prev = _to_float(mat.get("cert_other_inc_gbp_prev"))
    existing.cert_other_inc_percent_prev = _to_float(mat.get("cert_other_inc_percent_prev"))

    existing.cert_associates_gbp = _to_float(mat.get("cert_associates_gbp"))
    existing.cert_associates_percent = _to_float(mat.get("cert_associates_percent"))
    existing.cert_associates_gbp_prev = _to_float(mat.get("cert_associates_gbp_prev"))
    existing.cert_associates_percent_prev = _to_float(mat.get("cert_associates_percent_prev"))

    existing.cert_wages_gbp = _to_float(mat.get("cert_wages_gbp"))
    existing.cert_wages_percent = _to_float(mat.get("cert_wages_percent"))
    existing.cert_wages_gbp_prev = _to_float(mat.get("cert_wages_gbp_prev"))
    existing.cert_wages_percent_prev = _to_float(mat.get("cert_wages_percent_prev"))

    existing.cert_hygiene_gbp = _to_float(mat.get("cert_hygiene_gbp"))
    existing.cert_hygiene_percent = _to_float(mat.get("cert_hygiene_percent"))
    existing.cert_hygiene_gbp_prev = _to_float(mat.get("cert_hygiene_gbp_prev"))
    existing.cert_hygiene_percent_prev = _to_float(mat.get("cert_hygiene_percent_prev"))

    existing.cert_materials_gbp = _to_float(mat.get("cert_materials_gbp"))
    existing.cert_materials_percent = _to_float(mat.get("cert_materials_percent"))
    existing.cert_materials_gbp_prev = _to_float(mat.get("cert_materials_gbp_prev"))
    existing.cert_materials_percent_prev = _to_float(mat.get("cert_materials_percent_prev"))

    existing.cert_labs_gbp = _to_float(mat.get("cert_labs_gbp"))
    existing.cert_labs_percent = _to_float(mat.get("cert_labs_percent"))
    existing.cert_labs_gbp_prev = _to_float(mat.get("cert_labs_gbp_prev"))
    existing.cert_labs_percent_prev = _to_float(mat.get("cert_labs_percent_prev"))

    existing.cert_net_profit_gbp = _to_float(mat.get("cert_net_profit_gbp"))
    existing.cert_net_profit_percent = _to_float(mat.get("cert_net_profit_percent"))
    existing.cert_net_profit_gbp_prev = _to_float(mat.get("cert_net_profit_gbp_prev"))
    existing.cert_net_profit_percent_prev = _to_float(mat.get("cert_net_profit_percent_prev"))

    existing.source_file = to_text(payload.get("source_file"))
    existing.raw_json = payload
    existing.updated_at = now

    return "updated"


def main() -> None:
    ap = argparse.ArgumentParser(description="Parse .xlsx spreadsheets and upsert latest-only rows into Supabase/Postgres.")
    ap.add_argument("--input-path", required=True, help="A spreadsheet file path or a directory containing spreadsheets")
    ap.add_argument("--glob", default="*.xlsx", help="Glob within input-dir (default: *.xlsx)")
    ap.add_argument("--recursive", action="store_true", help="Recurse into subdirectories")
    ap.add_argument("--canonical-mapping", default=None, help="Optional canonical mapping JSON path")
    ap.add_argument("--max-workers", type=int, default=6, help="Parallel extract workers (default: 6)")
    ap.add_argument("--dry-run", action="store_true", help="Do not write to the database")
    ap.add_argument("--force", action="store_true", help="Force update even if accounts_period_end is unchanged")
    ap.add_argument("--report-json", default="out/import_report.json", help="Write a JSON report here")
    args = ap.parse_args()

    from app.core.config import get_settings

    settings = get_settings()
    low_conf = float(settings.low_confidence_threshold)

    input_path = Path(args.input_path)
    paths = _iter_input_paths(input_path, glob=args.glob, recursive=bool(args.recursive))
    if not paths:
        raise SystemExit(f"No files found in {input_path} matching {args.glob} (recursive={args.recursive})")

    t_all = time.time()
    extracted: list[dict[str, Any]] = []
    ok_results: list[PracticeLatestResult] = []

    with ThreadPoolExecutor(max_workers=int(args.max_workers)) as ex:
        futs = {
            ex.submit(_extract_one, p, canonical_mapping_path=args.canonical_mapping, low_conf_threshold=low_conf): p for p in paths
        }
        for fut in as_completed(futs):
            p, status, error, elapsed_ms, res = fut.result()
            extracted.append(
                {
                    "source_file": str(p),
                    "status": status,
                    "error": error,
                    "elapsed_ms": int(elapsed_ms),
                    "practice_key": res.practice_key if res else None,
                    "accounts_period_end": res.accounts_period_end.isoformat() if (res and res.accounts_period_end) else None,
                }
            )
            if res is not None:
                ok_results.append(res)

    # Ingest (single session, commit per file; mirrors existing ingest script behavior).
    engine = get_engine()
    now = datetime.utcnow()
    updated = 0
    skipped = 0
    errors = 0

    with Session(engine) as session:
        for r in ok_results:
            try:
                status = _ingest_one(session, r, now=now, dry_run=bool(args.dry_run), force=bool(args.force))
                if status == "updated":
                    updated += 1
                else:
                    skipped += 1
                if not args.dry_run:
                    session.commit()
            except Exception as e:
                errors += 1
                if not args.dry_run:
                    session.rollback()
                extracted.append(
                    {
                        "source_file": str(r.source_file),
                        "status": "ingest_error",
                        "error": str(e),
                        "elapsed_ms": None,
                        "practice_key": r.practice_key,
                        "accounts_period_end": r.accounts_period_end.isoformat() if r.accounts_period_end else None,
                    }
                )

    report_path = Path(args.report_json)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "settings": {
            "database_url_configured": bool(settings.database_url),
            "supabase_url_configured": bool(settings.supabase_url),
        },
        "input": {"input_path": str(input_path), "glob": args.glob, "recursive": bool(args.recursive), "count": len(paths)},
        "summary": {"updated": updated, "skipped": skipped, "errors": errors, "elapsed_ms": int((time.time() - t_all) * 1000)},
        "items": sorted(extracted, key=lambda x: (x.get("status") != "ok", x.get("source_file", ""))),
    }
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    print(json.dumps(report["summary"], indent=2))
    print(str(report_path))


if __name__ == "__main__":
    main()

