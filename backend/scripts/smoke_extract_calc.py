from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from app.services.calc_metrics_extractor import CalcMetricsExtractor
from app.services.calc_sheet_selector import CalcSheetSelector


def main() -> None:
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("xlsx", help="Path to .xlsx")
    ap.add_argument("--out", default="smoke_calc_extract.json")
    args = ap.parse_args()

    path = Path(args.xlsx)
    sel = CalcSheetSelector().select(path)
    if sel is None:
        raise SystemExit("No calc sheet selected")

    xl = pd.ExcelFile(path)
    df = xl.parse(sheet_name=sel.sheet_name, header=None, dtype=object)

    ex = CalcMetricsExtractor()
    practice_name, practice_address, conf = ex.extract_practice_header(df)
    metrics = ex.extract_metrics(df)

    payload = {
        "file": str(path),
        "selected_sheet": {
            "sheet_name": sel.sheet_name,
            "as_of_date": sel.as_of_date.isoformat() if sel.as_of_date else None,
            "as_of_date_source": sel.as_of_date_source,
            "practice_block_lines": sel.practice_block_lines,
            "score": sel.score,
        },
        "practice_header": {
            "practice_name": practice_name,
            "practice_address": practice_address,
            "confidence": conf,
        },
        "canonical_metrics": [
            {
                "metric_key": m.metric_key,
                "label": m.metric_label,
                "value_number": str(m.value_number) if m.value_number is not None else None,
                "value_text": m.value_text,
                "unit": m.unit,
                "confidence": m.confidence,
                "row": m.row,
                "col": m.col,
            }
            for m in metrics
            if not m.metric_key.startswith("raw:")
        ],
        "raw_metric_count": sum(1 for m in metrics if m.metric_key.startswith("raw:")),
    }
    Path(args.out).write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(args.out)


if __name__ == "__main__":
    main()

