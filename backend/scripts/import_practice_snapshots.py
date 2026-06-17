from __future__ import annotations

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from sqlmodel import Session, select

# Ensure `backend/` is importable when invoked as `python scripts/<file>.py`.
BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from app.db.session import get_engine
from app.models.practice_snapshot import PracticeSnapshot
from app.services.practice_latest_extractor import PracticeLatestExtractor, PracticeSnapshotResult
from app.validators.normalizers import to_date, to_decimal, to_text


# Numeric columns copied straight from the materialized payload.
_NUM_FIELDS = [
    "goodwill", "efandf", "total", "freehold", "grand_total",
    "uda_contract_value_gbp", "uda_count", "uda_rate_gbp", "uda_uplift_value_gbp",
    "income_split_fpi_percent", "income_split_fpi_value", "income_split_fpi_applied_percent", "income_split_fpi_applied_value",
    "income_split_nhs_percent", "income_split_nhs_value", "income_split_nhs_applied_percent", "income_split_nhs_applied_value",
    "income_split_denplan_percent", "income_split_denplan_value", "income_split_denplan_applied_percent", "income_split_denplan_applied_value",
    "income_split_rent_percent", "income_split_rent_value", "income_split_rent_applied_percent", "income_split_rent_applied_value",
    "associate_cost_amount", "associate_cost_pct",
    "cert_income_gbp", "cert_income_percent", "cert_income_gbp_prev", "cert_income_percent_prev",
    "cert_other_inc_gbp", "cert_other_inc_percent", "cert_other_inc_gbp_prev", "cert_other_inc_percent_prev",
    "cert_associates_gbp", "cert_associates_percent", "cert_associates_gbp_prev", "cert_associates_percent_prev",
    "cert_wages_gbp", "cert_wages_percent", "cert_wages_gbp_prev", "cert_wages_percent_prev",
    "cert_hygiene_gbp", "cert_hygiene_percent", "cert_hygiene_gbp_prev", "cert_hygiene_percent_prev",
    "cert_materials_gbp", "cert_materials_percent", "cert_materials_gbp_prev", "cert_materials_percent_prev",
    "cert_labs_gbp", "cert_labs_percent", "cert_labs_gbp_prev", "cert_labs_percent_prev",
    "cert_net_profit_gbp", "cert_net_profit_percent", "cert_net_profit_gbp_prev", "cert_net_profit_percent_prev",
    "accountancy_bookkeeping_gbp", "light_heat_gbp", "phone_telecoms_gbp", "it_software_gbp",
    "professional_subs_gbp", "bank_charges_gbp", "therapist_gross_fees_gbp",
]


def _to_float(v: Any) -> Optional[float]:
    d = to_decimal(v)
    return float(d) if d is not None else None


def _extract_one(p: Path) -> tuple[Path, str, Optional[str], list[PracticeSnapshotResult]]:
    extractor = PracticeLatestExtractor()
    try:
        snaps = extractor.extract_snapshots(p)
        return p, "ok", None, snaps
    except Exception as e:  # noqa: BLE001
        return p, "error", str(e), []


def _apply(row: PracticeSnapshot, r: PracticeSnapshotResult) -> None:
    mat = r.raw_json.get("materialized") or {}
    pr = r.raw_json.get("practice") or {}

    row.snapshot_key = r.snapshot_key
    row.practice_key = r.practice_key
    row.display_name = r.display_name or r.practice_key.split("|")[0]
    row.practice_name = to_text(pr.get("practice_name"))
    row.postcode = to_text(pr.get("postcode"))
    row.city = to_text(pr.get("city"))
    row.county = to_text(pr.get("county"))

    row.as_of_date = r.as_of_date
    row.as_of_date_source = r.as_of_date_source
    row.sheet_name = r.sheet_name

    _sc = mat.get("surgery_count")
    try:
        _sc = int(_sc) if _sc is not None else None
    except (TypeError, ValueError):
        _sc = None
    row.surgery_count = _sc if (_sc is not None and 1 <= _sc <= 50) else None

    row.nhs_contract_number = to_text(mat.get("nhs_contract_number"))
    for f in _NUM_FIELDS:
        setattr(row, f, _to_float(mat.get(f)))

    row.accounts_period_end = to_date(mat.get("accounts_period_end"))
    row.certified_accounts_period_end_prev = to_date(mat.get("certified_accounts_period_end_prev"))

    row.source_file = to_text(r.source_file)
    row.raw_json = r.raw_json
    row.updated_at = datetime.utcnow()


def main() -> None:
    ap = argparse.ArgumentParser(description="Extract historical snapshot sheets and upsert into public.practice_snapshots.")
    ap.add_argument("--input-path", required=False, help="A spreadsheet file or directory")
    ap.add_argument("--paths-json", default=None, help="JSON array of .xlsx file paths (overrides --input-path)")
    ap.add_argument("--glob", default="*.xlsx")
    ap.add_argument("--recursive", action="store_true")
    ap.add_argument("--max-workers", type=int, default=6)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--report-json", default="out/snapshots_import_report.json")
    args = ap.parse_args()

    if args.paths_json:
        raw = json.loads(Path(args.paths_json).read_text(encoding="utf-8"))
        if not isinstance(raw, list) or not all(isinstance(x, str) for x in raw):
            raise SystemExit("--paths-json must be a JSON array of file path strings")
        paths = [Path(p) for p in raw if Path(p).is_file()]
        if not paths:
            raise SystemExit(f"No existing files listed in {args.paths_json}")
    else:
        if not args.input_path:
            raise SystemExit("Provide either --input-path or --paths-json")
        ip = Path(args.input_path)
        if not ip.exists():
            raise SystemExit(f"Input path not found: {ip}")
        if ip.is_file():
            paths = [ip]
        else:
            it = ip.rglob(args.glob) if args.recursive else ip.glob(args.glob)
            paths = sorted([p for p in it if p.is_file()])
        if not paths:
            raise SystemExit(f"No files found in {ip}")

    t_all = time.time()
    all_snaps: list[PracticeSnapshotResult] = []
    files_ok = 0
    files_err = 0
    extract_errors: list[dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=int(args.max_workers)) as ex:
        futs = {ex.submit(_extract_one, p): p for p in paths}
        for fut in as_completed(futs):
            p, status, error, snaps = fut.result()
            if status == "ok":
                files_ok += 1
                all_snaps.extend(snaps)
            else:
                files_err += 1
                extract_errors.append({"source_file": str(p), "error": error})

    # Dedup across files on snapshot_key: keep the latest as_of_date / richest.
    by_key: dict[str, PracticeSnapshotResult] = {}
    for s in all_snaps:
        prev = by_key.get(s.snapshot_key)
        if prev is None:
            by_key[s.snapshot_key] = s
            continue
        # Prefer the one whose materialized payload has more non-null financials.
        def richness(x: PracticeSnapshotResult) -> int:
            mat = x.raw_json.get("materialized") or {}
            return sum(1 for v in mat.values() if v is not None)
        if richness(s) > richness(prev):
            by_key[s.snapshot_key] = s

    snaps_unique = list(by_key.values())

    engine = get_engine()
    inserted = 0
    updated = 0
    errors = 0
    with Session(engine) as session:
        for r in snaps_unique:
            try:
                existing = session.exec(
                    select(PracticeSnapshot).where(PracticeSnapshot.snapshot_key == r.snapshot_key)
                ).first()
                if existing is None:
                    existing = PracticeSnapshot(snapshot_key=r.snapshot_key, practice_key=r.practice_key, display_name=r.display_name)
                    _apply(existing, r)
                    if not args.dry_run:
                        session.add(existing)
                    inserted += 1
                else:
                    _apply(existing, r)
                    updated += 1
                if not args.dry_run:
                    session.commit()
            except Exception as e:  # noqa: BLE001
                errors += 1
                if not args.dry_run:
                    session.rollback()
                extract_errors.append({"snapshot_key": r.snapshot_key, "source_file": r.source_file, "error": str(e)})

    report_path = Path(args.report_json)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    summary = {
        "files_total": len(paths),
        "files_ok": files_ok,
        "files_error": files_err,
        "snapshots_extracted": len(all_snaps),
        "snapshots_unique": len(snaps_unique),
        "inserted": inserted,
        "updated": updated,
        "ingest_errors": errors,
        "elapsed_ms": int((time.time() - t_all) * 1000),
        "dry_run": bool(args.dry_run),
    }
    report_path.write_text(
        json.dumps({"summary": summary, "errors": extract_errors[:200]}, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2))
    print(str(report_path))


if __name__ == "__main__":
    main()
