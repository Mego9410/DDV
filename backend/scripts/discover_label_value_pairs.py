from __future__ import annotations

import json
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import pandas as pd


def _cell_to_text(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, float) and pd.isna(v):
        return ""
    if isinstance(v, str):
        return v.strip()
    return str(v).strip()


def _is_numberish(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, (int, float)) and not (isinstance(v, float) and pd.isna(v)):
        return True
    if isinstance(v, str):
        s = v.strip().replace(",", "")
        if not s:
            return False
        if re.fullmatch(r"[-+]?\d+(\.\d+)?%?", s):
            return True
        if re.fullmatch(r"[-+]?\£?\$?\€?\d+(\.\d+)?", s):
            return True
    return False


def _is_labelish(s: str) -> bool:
    if not s:
        return False
    s2 = s.lower()
    if len(s2) < 3:
        return False
    # avoid pure numbers
    if re.fullmatch(r"[-+]?\d+(\.\d+)?", s2):
        return False
    # avoid obvious headers noise
    if s2 in {"value", "values", "total"}:
        return False
    return True


@dataclass(frozen=True)
class PairHit:
    label: str
    value: Any
    row: int
    col: int


def extract_pairs(df: pd.DataFrame) -> list[PairHit]:
    hits: list[PairHit] = []
    rows, cols = df.shape
    for r in range(rows):
        for c in range(cols):
            label = _cell_to_text(df.iat[r, c])
            if not _is_labelish(label):
                continue

            # Prefer right-cell value
            if c + 1 < cols:
                v = df.iat[r, c + 1]
                if _is_numberish(v) or (_cell_to_text(v) and len(_cell_to_text(v)) <= 120):
                    hits.append(PairHit(label=label, value=v, row=r, col=c))
                    continue

            # Fallback: label and value in same cell "Label: Value"
            if isinstance(df.iat[r, c], str) and ":" in df.iat[r, c]:
                left, right = df.iat[r, c].split(":", 1)
                left = left.strip()
                right = right.strip()
                if _is_labelish(left) and right:
                    hits.append(PairHit(label=left, value=right, row=r, col=c))
                    continue

            # Fallback: below-cell
            if r + 1 < rows:
                v = df.iat[r + 1, c]
                if _is_numberish(v):
                    hits.append(PairHit(label=label, value=v, row=r, col=c))
                    continue

    return hits


def main() -> None:
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--inspection", required=True, help="Path to workbook_inspection JSON")
    ap.add_argument("--out", default="field_map.json", help="Output JSON path")
    ap.add_argument("--max_rows", type=int, default=200, help="Max rows per sheet to scan")
    ap.add_argument("--max_cols", type=int, default=40, help="Max cols per sheet to scan")
    args = ap.parse_args()

    inspection = json.loads(Path(args.inspection).read_text(encoding="utf-8"))

    global_counts: dict[str, int] = defaultdict(int)
    global_examples: dict[str, list[Any]] = defaultdict(list)
    per_file: list[dict[str, Any]] = []

    for item in inspection:
        xlsx_path = Path(item["file"])
        best = item.get("best_sheet") or {}
        sheet_name = best.get("sheet_name")
        if not sheet_name:
            continue

        xl = pd.ExcelFile(xlsx_path)
        df = xl.parse(sheet_name=sheet_name, header=None, dtype=object).iloc[: args.max_rows, : args.max_cols]
        hits = extract_pairs(df)

        file_map: dict[str, Any] = {}
        for h in hits:
            key = re.sub(r"\s+", " ", h.label.strip()).strip()
            if not key:
                continue
            # keep first seen per file for MVP (later: keep all occurrences + confidence)
            if key not in file_map:
                file_map[key] = _cell_to_text(h.value) if isinstance(h.value, str) else h.value

        for k, v in file_map.items():
            global_counts[k] += 1
            if len(global_examples[k]) < 3:
                global_examples[k].append(v)

        per_file.append(
            {
                "file": str(xlsx_path),
                "sheet": sheet_name,
                "pairs_found": len(file_map),
                "fields": file_map,
            }
        )

    # Sort fields by frequency across files
    fields_sorted = sorted(global_counts.items(), key=lambda kv: kv[1], reverse=True)
    out = {
        "field_frequency": [
            {"label": k, "files_present": n, "examples": global_examples.get(k, [])} for k, n in fields_sorted
        ],
        "per_file": per_file,
    }

    out_path = Path(args.out)
    out_path.write_text(json.dumps(out, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    print(str(out_path))


if __name__ == "__main__":
    main()

