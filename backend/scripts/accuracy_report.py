from __future__ import annotations

import csv
import json
import math
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional


@dataclass(frozen=True)
class MetricCell:
    value_number: Optional[str]
    value_text: Optional[str]
    confidence: float

    @property
    def has_value(self) -> bool:
        if self.value_number is not None and str(self.value_number).strip() != "":
            return True
        if self.value_text is not None and str(self.value_text).strip() != "":
            return True
        return False


def _f(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        s = str(v).strip()
        if not s:
            return None
        x = float(s)
        if math.isnan(x):
            return None
        return x
    except Exception:
        return None


def main() -> None:
    import argparse

    ap = argparse.ArgumentParser(description="Compute coverage/completeness/confidence accuracy proxy for exports.")
    ap.add_argument("--export_dir", required=True, help="Folder containing calc_metrics.csv, practices.csv, etc.")
    ap.add_argument("--min_conf", type=float, default=0.8, help="Confidence threshold for 'good' extractions")
    ap.add_argument(
        "--scope",
        default="all",
        choices=["all", "core", "certified", "income_split", "uda", "latest_schema"],
        help="Which metric families to include",
    )
    args = ap.parse_args()

    export_dir = Path(args.export_dir)
    metrics_path = export_dir / "calc_metrics.csv"
    practices_path = export_dir / "practices.csv"
    if not metrics_path.exists():
        raise SystemExit(f"Missing {metrics_path}")
    if not practices_path.exists():
        raise SystemExit(f"Missing {practices_path}")

    practices = list(csv.DictReader(practices_path.open(encoding="utf-8")))
    practice_ids = [p["practice_id"] for p in practices if p.get("practice_id")]
    practice_id_set = set(practice_ids)

    rows = list(csv.DictReader(metrics_path.open(encoding="utf-8")))
    certified_latest_path = export_dir / "certified_accounts_latest.csv"
    certified_latest = None
    if certified_latest_path.exists():
        certified_latest = list(csv.DictReader(certified_latest_path.open(encoding="utf-8")))

    # Build best cell per (practice_id, metric_key) based on confidence
    best: dict[tuple[str, str], MetricCell] = {}
    present_practices_by_key: dict[str, set[str]] = defaultdict(set)

    # For latest_schema we still ingest metrics rows, then evaluate against a fixed key list.
    # So we treat everything as in-scope during ingestion.
    def in_scope(k: str) -> bool:
        if args.scope == "all":
            return True
        if args.scope == "core":
            return not (k.startswith("certified_") or k.startswith("income_split_") or k.startswith("uda_"))
        if args.scope == "certified":
            return k.startswith("certified_")
        if args.scope == "income_split":
            return k.startswith("income_split_")
        if args.scope == "uda":
            return k.startswith("uda_")
        if args.scope == "latest_schema":
            return True
        return True

    for r in rows:
        pid = r.get("practice_id") or ""
        if pid not in practice_id_set:
            continue
        k = r.get("metric_key") or ""
        if not k or not in_scope(k):
            continue
        conf = _f(r.get("confidence")) or 0.0
        cell = MetricCell(
            value_number=r.get("value_number") or None,
            value_text=r.get("value_text") or None,
            confidence=conf,
        )
        key = (pid, k)
        prev = best.get(key)
        if prev is None or cell.confidence > prev.confidence:
            best[key] = cell
        present_practices_by_key[k].add(pid)

    if args.scope == "latest_schema":
        # Define a query-friendly schema focusing on "latest" values:
        # - core canonical metrics (one per practice)
        # - UDA block values
        # - income split (selected common types)
        # - certified latest accounts (wide table) if present
        core_keys = [
            "goodwill",
            "efandf",
            "freehold",
            "total",
            "grand_total",
            "gdwill_perc",
            "fpi",
            "number_of_surgeries",
        ]
        uda_keys = ["uda_contract_value_gbp", "uda_count", "uda_rate_gbp"]
        income_types = ["fpi", "nhs", "denplan", "rent"]
        income_split_keys = []
        for t in income_types:
            income_split_keys.extend(
                [
                    f"income_split_{t}_percent",
                    f"income_split_{t}_value",
                    f"income_split_{t}_applied_percent",
                    f"income_split_{t}_applied_value",
                ]
            )

        certified_fields = [
            "income_gbp",
            "other_inc_gbp",
            "associates_gbp",
            "wages_gbp",
            "hygiene_gbp",
            "materials_gbp",
            "labs_gbp",
            "net_profit_gbp",
        ]

        metric_keys = core_keys + uda_keys + income_split_keys

        # Add certified fields as pseudo-keys sourced from certified_accounts_latest.csv
        certified_by_practice: dict[str, dict[str, str]] = {}
        if certified_latest is not None:
            for r in certified_latest:
                pid = r.get("practice_id") or ""
                if pid in practice_id_set:
                    certified_by_practice[pid] = r
            for f in certified_fields:
                metric_keys.append(f"cert_latest:{f}")

        # Build best cells for these pseudo-keys from certified_latest
        for pid, row in (certified_by_practice or {}).items():
            for f in certified_fields:
                v = row.get(f)
                if v is None or str(v).strip() == "":
                    continue
                best[(pid, f"cert_latest:{f}")] = MetricCell(value_number=v, value_text=None, confidence=0.9)
                present_practices_by_key[f"cert_latest:{f}"].add(pid)
    else:
        metric_keys = sorted(present_practices_by_key.keys())

    # Define "expected metrics" as those present in at least one practice in this export
    # and compute how well they cover all practices.
    per_key = []
    for k in metric_keys:
        pids_present = present_practices_by_key[k]
        cov = len(pids_present) / max(1, len(practice_ids))

        non_null = 0
        good = 0
        for pid in practice_ids:
            cell = best.get((pid, k))
            if cell and cell.has_value:
                non_null += 1
                if cell.confidence >= args.min_conf:
                    good += 1

        completeness = non_null / max(1, len(practice_ids))
        good_rate = good / max(1, len(practice_ids))
        per_key.append(
            {
                "metric_key": k,
                "coverage": cov,
                "completeness": completeness,
                "good_rate": good_rate,
            }
        )

    # Overall score: average of good_rate across keys (weighted by 1.0)
    overall = sum(x["good_rate"] for x in per_key) / max(1, len(per_key))
    overall_80cov = None
    overall_90cov = None
    keys_80 = [x for x in per_key if x["coverage"] >= 0.8]
    keys_90 = [x for x in per_key if x["coverage"] >= 0.9]
    if keys_80:
        overall_80cov = sum(x["good_rate"] for x in keys_80) / len(keys_80)
    if keys_90:
        overall_90cov = sum(x["good_rate"] for x in keys_90) / len(keys_90)

    # Per-practice score: fraction of keys that are "good" for that practice
    per_practice = []
    for pid in practice_ids:
        good = 0
        for k in metric_keys:
            cell = best.get((pid, k))
            if cell and cell.has_value and cell.confidence >= args.min_conf:
                good += 1
        per_practice.append(
            {
                "practice_id": pid,
                "good_rate": good / max(1, len(metric_keys)),
            }
        )

    per_key.sort(key=lambda d: (d["good_rate"], d["completeness"], d["coverage"]))
    per_practice.sort(key=lambda d: d["good_rate"])

    report = {
        "export_dir": str(export_dir),
        "scope": args.scope,
        "min_conf": args.min_conf,
        "practice_count": len(practice_ids),
        "metric_key_count": len(metric_keys),
        "overall_good_rate": overall,
        "overall_good_rate_coverage_ge_0_8": overall_80cov,
        "overall_good_rate_coverage_ge_0_9": overall_90cov,
        "metric_key_count_coverage_ge_0_8": len(keys_80),
        "metric_key_count_coverage_ge_0_9": len(keys_90),
        "worst_metrics": per_key[:25],
        "best_metrics": sorted(per_key, key=lambda d: d["good_rate"], reverse=True)[:25],
        "worst_practices": per_practice[:10],
        "best_practices": sorted(per_practice, key=lambda d: d["good_rate"], reverse=True)[:10],
    }

    out_path = export_dir / f"accuracy_report_{args.scope}.json"
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(str(out_path))


if __name__ == "__main__":
    main()

