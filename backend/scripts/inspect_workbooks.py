from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from dateutil import parser as date_parser


@dataclass(frozen=True)
class SheetCandidate:
    sheet_name: str
    score: float
    practice_block: list[str]
    detected_date: Optional[date]
    date_source: Optional[str]


def _cell_to_text(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, float) and pd.isna(v):
        return ""
    if isinstance(v, str):
        return v.strip()
    return str(v).strip()


def _read_sheet_grid(xl: pd.ExcelFile, sheet: str, *, max_rows: int = 60, max_cols: int = 20) -> pd.DataFrame:
    df = xl.parse(sheet_name=sheet, header=None, dtype=object)
    df = df.iloc[:max_rows, :max_cols]
    return df


def _top_left_lines(df: pd.DataFrame, *, rows: int = 12, cols: int = 6) -> list[str]:
    region = df.iloc[:rows, :cols]
    lines: list[str] = []
    for r in range(region.shape[0]):
        # join row cells so merged-cell artifacts still appear as a “line”
        parts = [_cell_to_text(region.iat[r, c]) for c in range(region.shape[1])]
        line = " ".join([p for p in parts if p])
        line = re.sub(r"\s+", " ", line).strip()
        if line:
            lines.append(line)
    # de-dupe while preserving order
    out: list[str] = []
    seen: set[str] = set()
    for l in lines:
        k = l.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(l)
    return out


def _looks_like_address_block(lines: list[str]) -> float:
    """
    Heuristic scoring: practice name + address in top-left.
    We avoid hardcoding UK formatting too much; just look for common address signals.
    """
    if not lines:
        return 0.0

    score = 0.0
    text = "\n".join(lines).lower()

    # Strong explicit signals used in many of your calc sheets
    if "practice address" in text:
        score += 3.0
    if "practice of" in text:
        score += 2.0

    # Name-ish: often first line is a practice name (not a label like "calc")
    first = lines[0].lower()
    if len(first) >= 6 and not any(tok in first for tok in ["calc", "calculation", "worksheet", "report", "template"]):
        score += 1.0

    # Address signals
    if re.search(r"\b(road|rd|street|st|avenue|ave|lane|ln|drive|dr|close|cl|court|ct|way)\b", text):
        score += 1.5
    if re.search(r"\b(postcode|zip)\b", text):
        score += 1.0
    if re.search(r"\b[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}\b", "\n".join(lines)):
        # UK postcode (rough)
        score += 2.0
    if re.search(r"\b(tel|phone|fax)\b", text):
        score += 0.5

    # More non-empty lines = more likely a block
    score += min(2.0, 0.25 * max(0, len(lines) - 1))

    return score


def _parse_date_maybe(s: str) -> Optional[date]:
    s = s.strip()
    if not s:
        return None
    # Only treat as date if there are strong date signals (avoid parsing random numbers).
    if not (
        re.search(r"\d{1,4}\s*[-./]\s*\d{1,2}\s*[-./]\s*\d{1,4}", s)
        or re.search(r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)\b", s.lower())
    ):
        return None
    try:
        return date_parser.parse(s, dayfirst=True, fuzzy=True).date()
    except Exception:
        return None


def _detect_sheet_date(sheet_name: str, top_left: list[str]) -> tuple[Optional[date], Optional[str]]:
    # Prefer explicit date in sheet name
    d = _parse_date_maybe(sheet_name)
    if d:
        return d, "sheet_name"

    # Search top-left lines for date-like strings
    for line in top_left[:10]:
        d2 = _parse_date_maybe(line)
        if d2:
            return d2, "top_left"

        # sometimes "As at: 09.11.22"
        m = re.search(r"(\d{1,2}[./-]\d{1,2}[./-]\d{2,4})", line)
        if m:
            d3 = _parse_date_maybe(m.group(1))
            if d3:
                return d3, "top_left_regex"

    return None, None


def pick_practice_sheet(xlsx_path: Path) -> dict[str, Any]:
    xl = pd.ExcelFile(xlsx_path)
    candidates: list[SheetCandidate] = []

    for sheet in xl.sheet_names:
        df = _read_sheet_grid(xl, sheet)
        tl = _top_left_lines(df)
        addr_score = _looks_like_address_block(tl)
        detected_date, date_source = _detect_sheet_date(sheet, tl)

        # “recency” bonus: later date wins if scores are close.
        recency_bonus = 0.0
        if detected_date:
            # normalize year weight; just a small nudge
            recency_bonus = min(1.5, max(0.0, (detected_date.year - 2018) * 0.05))

        score = addr_score + recency_bonus
        candidates.append(
            SheetCandidate(
                sheet_name=sheet,
                score=score,
                practice_block=tl[:8],
                detected_date=detected_date,
                date_source=date_source,
            )
        )

    candidates_sorted = sorted(candidates, key=lambda c: (c.score, c.detected_date or date.min), reverse=True)
    best = candidates_sorted[0] if candidates_sorted else None

    return {
        "file": str(xlsx_path),
        "sheets": xl.sheet_names,
        "candidates": [
            {
                "sheet_name": c.sheet_name,
                "score": round(c.score, 3),
                "detected_date": c.detected_date.isoformat() if c.detected_date else None,
                "date_source": c.date_source,
                "practice_block": c.practice_block,
            }
            for c in candidates_sorted[:8]
        ],
        "best_sheet": {
            "sheet_name": best.sheet_name,
            "score": round(best.score, 3),
            "detected_date": best.detected_date.isoformat() if best.detected_date else None,
            "date_source": best.date_source,
            "practice_block": best.practice_block,
        }
        if best
        else None,
    }


def main() -> None:
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("files", nargs="+", help="Paths to .xlsx files")
    ap.add_argument("--out", default="workbook_inspection.json", help="Output JSON path")
    args = ap.parse_args()

    results: list[dict[str, Any]] = []
    for f in args.files:
        p = Path(f)
        results.append(pick_practice_sheet(p))

    out_path = Path(args.out)
    out_path.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")
    print(str(out_path))


if __name__ == "__main__":
    main()

