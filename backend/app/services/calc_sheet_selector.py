from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from dateutil import parser as date_parser


@dataclass(frozen=True)
class SelectedCalcSheet:
    sheet_name: str
    practice_block_lines: list[str]
    as_of_date: Optional[date]
    as_of_date_source: Optional[str]
    score: float


class CalcSheetSelector:
    """
    Pick the most relevant Calc/Calculation/Update sheet from a workbook:
    - must look like it contains practice name/address block in top-left
    - prefer dated Update/Calc tabs (latest date wins)
    """

    def select(self, xlsx_path: Path) -> Optional[SelectedCalcSheet]:
        xl = pd.ExcelFile(xlsx_path)
        candidates: list[SelectedCalcSheet] = []
        for sheet in xl.sheet_names:
            if not self._is_calc_like_name(sheet):
                continue
            df = xl.parse(sheet_name=sheet, header=None, dtype=object).iloc[:60, :20]
            lines = self._top_left_lines(df)
            addr_score = self._looks_like_practice_header(lines)
            if addr_score < 4.0:
                continue
            as_of, src = self._detect_date(sheet, lines)
            recency_bonus = 0.0
            if as_of:
                recency_bonus = min(2.0, max(0.0, (as_of.year - 2018) * 0.07))
            score = addr_score + recency_bonus
            candidates.append(
                SelectedCalcSheet(
                    sheet_name=sheet,
                    practice_block_lines=lines[:8],
                    as_of_date=as_of,
                    as_of_date_source=src,
                    score=score,
                )
            )

        if not candidates:
            return None
        candidates = sorted(candidates, key=lambda c: (c.as_of_date or date.min, c.score), reverse=True)
        return candidates[0]

    def _is_calc_like_name(self, sheet_name: str) -> bool:
        s = sheet_name.strip().lower()
        return (
            "calc" in s
            or "calculation" in s
            or s.startswith("update")
            or "update" in s
        )

    def _top_left_lines(self, df: pd.DataFrame, *, rows: int = 12, cols: int = 6) -> list[str]:
        region = df.iloc[:rows, :cols]
        lines: list[str] = []
        for r in range(region.shape[0]):
            parts = [self._cell_to_text(region.iat[r, c]) for c in range(region.shape[1])]
            line = " ".join([p for p in parts if p])
            line = re.sub(r"\s+", " ", line).strip()
            if line:
                lines.append(line)
        out: list[str] = []
        seen: set[str] = set()
        for l in lines:
            k = l.lower()
            if k in seen:
                continue
            seen.add(k)
            out.append(l)
        return out

    def _cell_to_text(self, v: Any) -> str:
        if v is None:
            return ""
        if isinstance(v, float) and pd.isna(v):
            return ""
        return str(v).strip()

    def _looks_like_practice_header(self, lines: list[str]) -> float:
        if not lines:
            return 0.0
        score = 0.0
        text = "\n".join(lines).lower()
        if "practice address" in text:
            score += 3.0
        if "practice of" in text:
            score += 2.0
        if re.search(r"\b[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}\b", "\n".join(lines)):
            score += 2.0
        if re.search(r"\b(road|rd|street|st|avenue|ave|lane|ln|drive|dr|close|cl|court|ct|way)\b", text):
            score += 1.5
        score += min(2.0, 0.25 * max(0, len(lines) - 1))
        return score

    def _detect_date(self, sheet_name: str, top_left: list[str]) -> tuple[Optional[date], Optional[str]]:
        d = self._parse_date_maybe(sheet_name)
        if d:
            return d, "sheet_name"
        for line in top_left[:10]:
            m = re.search(r"(\d{1,2}[./-]\d{1,2}[./-]\d{2,4})", line)
            if m:
                d2 = self._parse_date_maybe(m.group(1))
                if d2:
                    return d2, "top_left"
        return None, None

    def _parse_date_maybe(self, s: str) -> Optional[date]:
        s = s.strip()
        if not s:
            return None
        if not re.search(r"\d{1,4}\s*[-./]\s*\d{1,2}\s*[-./]\s*\d{1,4}", s):
            return None
        try:
            return date_parser.parse(s, dayfirst=True, fuzzy=True).date()
        except Exception:
            return None

