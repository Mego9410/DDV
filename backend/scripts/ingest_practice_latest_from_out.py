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

                    # Backfill newly-added columns even when period_end hasn't advanced.
                    if not should_update:
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

