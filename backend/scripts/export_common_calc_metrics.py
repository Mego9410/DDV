from __future__ import annotations

import csv
import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
from uuid import uuid4

import pandas as pd

from app.services.calc_metrics_extractor import CalcMetricsExtractor
from app.services.calc_sheet_selector import CalcSheetSelector
from app.utils.address_normalizer import normalize_uk_address


@dataclass(frozen=True)
class PracticeRow:
    practice_id: str
    display_name: str
    address_text: Optional[str]
    postcode: Optional[str]
    city: Optional[str]
    county: Optional[str]
    address_line1: Optional[str]
    address_line2: Optional[str]


def _normalize_address_fields(addr: Optional[str]) -> dict[str, Optional[str]]:
    n = normalize_uk_address(addr)
    if not n:
        return {"postcode": None, "city": None, "county": None, "address_line1": None, "address_line2": None}
    return {
        "postcode": n.postcode,
        "city": n.city,
        "county": n.county,
        "address_line1": n.address_line1,
        "address_line2": n.address_line2,
    }


def main() -> None:
    import argparse

    ap = argparse.ArgumentParser(description="Export common Calc-sheet metrics across many practices.")
    ap.add_argument("files", nargs="+", help="Paths to xlsx files")
    ap.add_argument("--outdir", default="exports", help="Output directory (relative or absolute)")
    ap.add_argument(
        "--canonical_mapping",
        default=None,
        help="Path to JSON file with canonical_specs to align labels across spreadsheets",
    )
    ap.add_argument(
        "--include_raw",
        action="store_true",
        help="Also consider raw:* metrics when computing common keys (usually noisy).",
    )
    ap.add_argument(
        "--common_threshold",
        type=float,
        default=1.0,
        help="Fraction of files a metric must appear in to be considered 'common' (1.0 = in all).",
    )
    args = ap.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    selector = CalcSheetSelector()
    extractor = CalcMetricsExtractor(canonical_mapping_path=args.canonical_mapping)

    practices: dict[str, PracticeRow] = {}
    sheet_versions: list[dict[str, Any]] = []
    metrics_rows: list[dict[str, Any]] = []

    # For determining common metrics
    metric_presence: dict[str, set[str]] = {}

    for file_path in args.files:
        p = Path(file_path)
        file_id = str(uuid4())

        selected = selector.select(p)
        if selected is None:
            sheet_versions.append(
                {
                    "sheet_version_id": str(uuid4()),
                    "source_file_id": file_id,
                    "file_path": str(p),
                    "sheet_name": None,
                    "as_of_date": None,
                    "as_of_date_source": None,
                    "practice_id": None,
                    "practice_name": None,
                    "practice_address": None,
                    "extraction_confidence": 0.0,
                    "status": "no_calc_sheet_found",
                }
            )
            continue

        xl = pd.ExcelFile(p)
        df = xl.parse(sheet_name=selected.sheet_name, header=None, dtype=object)

        practice_name, practice_address, header_conf = extractor.extract_practice_header(df)
        practice_id = str(uuid4())
        addr_fields = _normalize_address_fields(practice_address)
        practices[practice_id] = PracticeRow(
            practice_id=practice_id,
            display_name=practice_name or p.stem,
            address_text=practice_address,
            postcode=addr_fields["postcode"],
            city=addr_fields["city"],
            county=addr_fields["county"],
            address_line1=addr_fields["address_line1"],
            address_line2=addr_fields["address_line2"],
        )

        sheet_version_id = str(uuid4())
        sheet_versions.append(
            {
                "sheet_version_id": sheet_version_id,
                "source_file_id": file_id,
                "file_path": str(p),
                "sheet_name": selected.sheet_name,
                "as_of_date": selected.as_of_date.isoformat() if selected.as_of_date else None,
                "as_of_date_source": selected.as_of_date_source,
                "practice_id": practice_id,
                "practice_name": practice_name,
                "practice_address": practice_address,
                "extraction_confidence": float((0.5 * header_conf) + (0.5 * min(1.0, selected.score / 12.0))),
                "status": "ok",
            }
        )

        metrics = extractor.extract_metrics(df)
        for m in metrics:
            if (not args.include_raw) and m.metric_key.startswith("raw:"):
                continue
            metrics_rows.append(
                {
                    "metric_id": str(uuid4()),
                    "sheet_version_id": sheet_version_id,
                    "practice_id": practice_id,
                    "as_of_date": selected.as_of_date.isoformat() if selected.as_of_date else None,
                    "metric_key": m.metric_key,
                    "metric_label": m.metric_label,
                    "value_number": str(m.value_number) if m.value_number is not None else None,
                    "value_text": m.value_text,
                    "unit": m.unit,
                    "confidence": m.confidence,
                    "row": m.row,
                    "col": m.col,
                }
            )
            metric_presence.setdefault(m.metric_key, set()).add(sheet_version_id)

    # Compute common metric keys across processed sheet versions (status=ok)
    ok_versions = [sv for sv in sheet_versions if sv["status"] == "ok"]
    ok_version_ids = {sv["sheet_version_id"] for sv in ok_versions}
    total_ok = len(ok_versions)

    common_keys: list[str] = []
    if total_ok > 0:
        for key, present_ids in metric_presence.items():
            present_ok = len(present_ids & ok_version_ids)
            if present_ok / total_ok >= args.common_threshold:
                common_keys.append(key)
    common_keys.sort()

    # Export CSVs (Supabase-friendly)
    _write_csv(
        outdir / "practices.csv",
        ["practice_id", "display_name", "address_text", "postcode", "city", "county", "address_line1", "address_line2"],
        [pr.__dict__ for pr in practices.values()],
    )
    _write_csv(
        outdir / "calc_sheet_versions.csv",
        [
            "sheet_version_id",
            "source_file_id",
            "file_path",
            "sheet_name",
            "as_of_date",
            "as_of_date_source",
            "practice_id",
            "practice_name",
            "practice_address",
            "extraction_confidence",
            "status",
        ],
        sheet_versions,
    )
    _write_csv(
        outdir / "calc_metrics.csv",
        [
            "metric_id",
            "sheet_version_id",
            "practice_id",
            "as_of_date",
            "metric_key",
            "metric_label",
            "value_number",
            "value_text",
            "unit",
            "confidence",
            "row",
            "col",
        ],
        metrics_rows,
    )

    # Certified accounts exports:
    # - long normalized table for easy SQL querying
    # - latest-per-practice wide table for "most recent year end" use cases
    certified_long_rows: list[dict[str, Any]] = []
    certified_latest_by_practice: dict[str, dict[str, Any]] = {}

    for r in metrics_rows:
        key = r.get("metric_key") or ""
        if not isinstance(key, str) or not key.startswith("certified_") or "__" not in key:
            continue
        try:
            base, yend = key.split("__", 1)
            field = base.replace("certified_", "")
        except Exception:
            continue
        practice_id = r.get("practice_id")
        if not practice_id:
            continue
        year_end = yend

        certified_long_rows.append(
            {
                "practice_id": practice_id,
                "sheet_version_id": r.get("sheet_version_id"),
                "year_end": year_end,
                "field": field,
                "unit": r.get("unit"),
                "value_number": r.get("value_number"),
                "value_text": r.get("value_text"),
                "confidence": r.get("confidence"),
            }
        )

        # latest: keep max year_end string (ISO date sortable)
        prev = certified_latest_by_practice.get(practice_id)
        if prev is None or (year_end and year_end > (prev.get("year_end") or "")):
            # initialize row skeleton
            certified_latest_by_practice[practice_id] = {"practice_id": practice_id, "year_end": year_end}

    # Fill latest rows with values for their chosen year_end
    latest_rows: list[dict[str, Any]] = []
    for practice_id, row in certified_latest_by_practice.items():
        yend = row["year_end"]
        # Collect all certified fields for this practice+year_end
        for rr in certified_long_rows:
            if rr["practice_id"] != practice_id or rr["year_end"] != yend:
                continue
            row[rr["field"]] = rr["value_number"] if rr["value_number"] is not None else rr["value_text"]
        latest_rows.append(row)

    # Stable column order for latest export (includes both gbp and percent variants if present)
    latest_headers = [
        "practice_id",
        "year_end",
        "income_gbp",
        "income_percent",
        "other_inc_gbp",
        "other_inc_percent",
        "associates_gbp",
        "associates_percent",
        "wages_gbp",
        "wages_percent",
        "hygiene_gbp",
        "hygiene_percent",
        "materials_gbp",
        "materials_percent",
        "labs_gbp",
        "labs_percent",
        "net_profit_gbp",
        "net_profit_percent",
    ]

    _write_csv(
        outdir / "certified_accounts_long.csv",
        ["practice_id", "sheet_version_id", "year_end", "field", "unit", "value_number", "value_text", "confidence"],
        certified_long_rows,
    )
    _write_csv(outdir / "certified_accounts_latest.csv", latest_headers, latest_rows)

    common_rows = [r for r in metrics_rows if r["metric_key"] in set(common_keys)]
    _write_csv(
        outdir / "calc_metrics_common.csv",
        [
            "metric_id",
            "sheet_version_id",
            "practice_id",
            "as_of_date",
            "metric_key",
            "metric_label",
            "value_number",
            "value_text",
            "unit",
            "confidence",
            "row",
            "col",
        ],
        common_rows,
    )

    summary = {
        "generated_at": datetime.utcnow().isoformat(),
        "input_files": len(args.files),
        "ok_versions": total_ok,
        "common_threshold": args.common_threshold,
        "include_raw": args.include_raw,
        "common_metric_keys": common_keys,
        "certified_accounts": {
            "long_rows": len(certified_long_rows),
            "latest_rows": len(latest_rows),
        },
    }
    (outdir / "common_metric_keys.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(str(outdir))


def _write_csv(path: Path, headers: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            w.writerow({h: r.get(h) for h in headers})


if __name__ == "__main__":
    main()

