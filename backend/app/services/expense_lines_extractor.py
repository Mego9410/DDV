from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date as date_type, datetime, timedelta
from decimal import Decimal
from typing import Any, Optional

import pandas as pd

from app.validators.normalizers import to_decimal


# Output fields. Materialized keys == DB column names (suffix _gbp).
EXPENSE_FIELDS: list[str] = [
    "accountancy_bookkeeping",
    "light_heat",
    "phone_telecoms",
    "it_software",
    "professional_subs",
    "bank_charges",
    "therapist_gross_fees",
]

# Normalized label aliases per field (see _norm_label).
# These target the "Reconstituted P & L" actuals list and the "Forecast" projection list,
# where label sits in the left-most column and values sit under year/total columns.
_ALIASES: dict[str, list[str]] = {
    "accountancy_bookkeeping": [
        "accountancy bookeeper",
        "accountancy bookkeeper",
        "accountancy and bookkeeping",
        "acct bookkeeper",
        "acct bookeeper",
        "accountancy fees",
        "accountancy",
        "bookkeeping",
        "bookeeping",
        "bookkeeper",
        "bookeeper",
    ],
    "light_heat": [
        "light and heat",
        "light heat",
        "heat and light",
        "heat light",
        "lighting and heating",
        "light and heating",
    ],
    "phone_telecoms": [
        "phone and telecoms",
        "telephone and fax",
        "telephone",
        "telecommunications",
        "telecoms",
        "mobile",
        "phone",
    ],
    "it_software": [
        "software and it",
        "it and software",
        "computer software",
        "computer costs",
        "computer cost",
        "computer",
        "software",
        "it costs",
        "information technology",
    ],
    "professional_subs": [
        "subscriptions and donations",
        "subs and donations",
        "professional subs",
        "professional subscriptions",
        "prof subs",
        "subscriptions",
        "subscription",
        "subs",
    ],
    "bank_charges": [
        "bank charges and interest",
        "bank charges and int",
        "bank charges",
        "bank charge",
        "bank chgs",
        "bank fees",
    ],
}

# Labels that look like a target but are NOT a cost line (e.g. the accountant firm header).
_LABEL_EXCLUDE_EXACT = {"accountants", "accountant"}

_DATE_SEP_RE = re.compile(r"\d{1,2}\s*[./-]\s*\d{1,2}\s*[./-]\s*\d{2,4}")
_YEAR_RE = re.compile(r"(19|20)\d{2}")


@dataclass(frozen=True)
class ExpenseHit:
    field: str
    gbp: Decimal
    source: str  # "actuals" | "forecast" | "management_info"
    label: str
    period_end: Optional[date_type]
    sheet: str
    confidence: float
    row: int
    col: int


class ExpenseLinesExtractor:
    """
    Extract additional P&L expense lines that are NOT part of the Certified Accounts table:
      - accountancy_bookkeeping, light_heat, phone_telecoms, it_software,
        professional_subs, bank_charges  (from Reconstituted P&L actuals, Forecast fallback)
      - therapist_gross_fees            (best-effort, from Management Information section)

    Returns a bundle shaped like the other extractors:
      {"materialized": {<col>_gbp: str|None}, "field_confidence": {...}, "evidence": {...}}
    """

    def extract(self, sheet_map: dict[str, pd.DataFrame]) -> dict[str, Any]:
        best: dict[str, ExpenseHit] = {}

        # 1) Actuals from Reconstituted P&L (preferred). Across all sheets/sections,
        #    keep the candidate with the latest period_end per field.
        for sheet_name, df in sheet_map.items():
            for hit in self._scan_reconstituted(df, sheet_name):
                self._consider_actual(best, hit)

        # 2) Forecast fallback: only fill fields with no actual value.
        for sheet_name, df in sheet_map.items():
            if "forecast" not in sheet_name.lower():
                continue
            for hit in self._scan_forecast(df, sheet_name):
                cur = best.get(hit.field)
                if cur is not None and cur.source == "actuals":
                    continue
                # Among forecast candidates, prefer the larger non-zero figure.
                if cur is None or (cur.source == "forecast" and abs(hit.gbp) > abs(cur.gbp)):
                    best[hit.field] = hit

        # 3) Therapist gross fees (best-effort, separate logic).
        therapist = self._extract_therapist_gross(sheet_map)
        if therapist is not None:
            best["therapist_gross_fees"] = therapist

        # Materialize
        mat: dict[str, Any] = {}
        conf: dict[str, float] = {}
        ev: dict[str, Any] = {}
        for field in EXPENSE_FIELDS:
            col = f"{field}_gbp"
            h = best.get(field)
            mat[col] = str(h.gbp) if h is not None else None
            conf[col] = float(h.confidence) if h is not None else 0.0
            ev[field] = (
                {
                    "value": str(h.gbp),
                    "source": h.source,
                    "label": h.label,
                    "period_end": h.period_end.isoformat() if h.period_end else None,
                    "sheet": h.sheet,
                    "row": h.row,
                    "col": h.col,
                    "confidence": h.confidence,
                }
                if h is not None
                else None
            )

        return {"materialized": mat, "field_confidence": conf, "evidence": ev}

    # ---------- Reconstituted P&L (actuals) ----------

    def _scan_reconstituted(self, df: pd.DataFrame, sheet_name: str) -> list[ExpenseHit]:
        hits: list[ExpenseHit] = []
        nrows, ncols = df.shape
        if nrows == 0 or ncols == 0:
            return hits

        for hdr_r, year_cols in self._find_reconstituted_sections(df):
            # year columns sorted latest-first
            cols_by_date = sorted(year_cols.items(), key=lambda kv: kv[1], reverse=True)
            section_end = min(hdr_r + 45, nrows)
            for r in range(hdr_r + 1, section_end):
                label = self._cell_text(df.iat[r, 0])
                if not label:
                    continue
                norm = self._norm_label(label)
                if not norm or norm in _LABEL_EXCLUDE_EXACT:
                    continue
                # boundary: stop at next section header
                if norm.startswith("reconstituted") or norm.startswith("projected") or norm.startswith("management information"):
                    break
                field = self._match_field(norm)
                if field is None:
                    continue
                # Take the latest NON-ZERO year value for this row. A 0 in these
                # expense lines almost always means "not broken out that year", so we
                # skip it and let an earlier year (or the Forecast fallback) supply a value.
                picked = None
                for c, d in cols_by_date:
                    if c >= ncols:
                        continue
                    num = to_decimal(df.iat[r, c])
                    if num is not None and abs(num) > 0:
                        picked = (num, c, d)
                        break
                if picked is None:
                    continue
                num, c, d = picked
                hits.append(
                    ExpenseHit(
                        field=field,
                        gbp=num,
                        source="actuals",
                        label=label,
                        period_end=d,
                        sheet=sheet_name,
                        confidence=0.85,
                        row=r,
                        col=c,
                    )
                )
        return hits

    def _find_reconstituted_sections(self, df: pd.DataFrame) -> list[tuple[int, dict[int, date_type]]]:
        """Return list of (header_row, {col: year_end_date}) for each Reconstituted P&L table."""
        out: list[tuple[int, dict[int, date_type]]] = []
        nrows, ncols = df.shape
        max_r = min(nrows, 400)
        for r in range(max_r):
            anchor = False
            for c in range(min(ncols, 6)):
                t = self._cell_text(df.iat[r, c]).lower()
                if not t:
                    continue
                if "reconstituted" in t and ("p & l" in t or "p&l" in t or "p and l" in t or "p l" in t):
                    anchor = True
                    break
            if not anchor:
                continue
            # header date row sits within the next few rows
            for hr in range(r, min(r + 5, nrows)):
                year_cols = self._date_cols_in_row(df, hr)
                if len(year_cols) >= 2:
                    out.append((hr, year_cols))
                    break
        return out

    def _date_cols_in_row(self, df: pd.DataFrame, r: int) -> dict[int, date_type]:
        cols: dict[int, date_type] = {}
        ncols = df.shape[1]
        for c in range(min(ncols, 12)):
            d = self._parse_date_cell(df.iat[r, c])
            if d is not None:
                cols[c] = d
        return cols

    # ---------- Forecast (fallback projection) ----------

    def _scan_forecast(self, df: pd.DataFrame, sheet_name: str) -> list[ExpenseHit]:
        hits: list[ExpenseHit] = []
        nrows, ncols = df.shape
        if nrows == 0 or ncols == 0:
            return hits
        # Forecast expense list lives in the upper region; label in col 0, totals in C4/C6/C2.
        value_cols = [c for c in (3, 5, 1) if c < ncols]
        for r in range(min(nrows, 80)):
            label = self._cell_text(df.iat[r, 0])
            if not label:
                continue
            norm = self._norm_label(label)
            if not norm or norm in _LABEL_EXCLUDE_EXACT:
                continue
            field = self._match_field(norm)
            if field is None:
                continue
            picked = None
            for c in value_cols:
                num = to_decimal(df.iat[r, c])
                if num is not None and abs(num) > 0:
                    picked = (num, c)
                    break
            if picked is None:
                continue
            num, c = picked
            hits.append(
                ExpenseHit(
                    field=field,
                    gbp=num,
                    source="forecast",
                    label=label,
                    period_end=None,
                    sheet=sheet_name,
                    confidence=0.6,
                    row=r,
                    col=c,
                )
            )
        return hits

    # ---------- Therapist gross fees (best-effort) ----------

    def _extract_therapist_gross(self, sheet_map: dict[str, pd.DataFrame]) -> Optional[ExpenseHit]:
        """
        Therapist gross fees appear as column headers labelled 'Therapist' in the
        Management Information section, with the gross figure in a totals row beneath.
        We sum distinct therapist columns within a sheet, then take the max across sheets
        (avoids double-counting the same therapist across projection tabs).
        """
        best_sum: Optional[Decimal] = None
        best_ev: Optional[tuple[str, int, int]] = None  # (sheet, row, col)
        for sheet_name, df in sheet_map.items():
            nrows, ncols = df.shape
            if nrows == 0 or ncols == 0:
                continue
            sheet_total = Decimal(0)
            found = False
            ev_cell: Optional[tuple[int, int]] = None
            for r in range(min(nrows, 250)):
                for c in range(min(ncols, 30)):
                    if self._norm_label(self._cell_text(df.iat[r, c])) != "therapist":
                        continue
                    col_val = self._max_numeric_below(df, r, c, depth=8)
                    if col_val is not None and col_val > 0:
                        sheet_total += col_val
                        found = True
                        if ev_cell is None:
                            ev_cell = (r, c)
            if found and (best_sum is None or sheet_total > best_sum):
                best_sum = sheet_total
                best_ev = (sheet_name, ev_cell[0], ev_cell[1]) if ev_cell else (sheet_name, 0, 0)

        if best_sum is None or best_sum <= 0 or best_ev is None:
            return None
        return ExpenseHit(
            field="therapist_gross_fees",
            gbp=best_sum,
            source="management_info",
            label="Therapist",
            period_end=None,
            sheet=best_ev[0],
            confidence=0.4,
            row=best_ev[1],
            col=best_ev[2],
        )

    def _max_numeric_below(self, df: pd.DataFrame, r: int, c: int, *, depth: int) -> Optional[Decimal]:
        nrows = df.shape[0]
        best: Optional[Decimal] = None
        for rr in range(r + 1, min(r + 1 + depth, nrows)):
            num = to_decimal(df.iat[rr, c])
            if num is None:
                continue
            if best is None or abs(num) > abs(best):
                best = num
        return best

    # ---------- helpers ----------

    def _consider_actual(self, best: dict[str, ExpenseHit], hit: ExpenseHit) -> None:
        cur = best.get(hit.field)
        if cur is None or cur.source != "actuals":
            best[hit.field] = hit
            return
        # Both actuals: prefer the later period_end; tie-break on confidence.
        cur_d = cur.period_end or date_type.min
        new_d = hit.period_end or date_type.min
        if new_d > cur_d or (new_d == cur_d and hit.confidence > cur.confidence):
            best[hit.field] = hit

    def _match_field(self, norm_label: str) -> Optional[str]:
        best_field: Optional[str] = None
        best_len = 0
        for field, aliases in _ALIASES.items():
            for alias in aliases:
                if not self._label_matches_alias(norm_label, alias):
                    continue
                if len(alias) > best_len:
                    best_len = len(alias)
                    best_field = field
        return best_field

    @staticmethod
    def _label_matches_alias(norm_label: str, alias: str) -> bool:
        if norm_label == alias:
            return True
        if norm_label.startswith(alias + " "):
            return True
        # multiword aliases are safe as substrings
        if " " in alias and alias in norm_label:
            return True
        return False

    @staticmethod
    def _norm_label(s: Any) -> str:
        if s is None:
            return ""
        if isinstance(s, float) and pd.isna(s):
            return ""
        t = str(s).strip().lower()
        t = t.replace("&", " and ")
        t = t.replace("/", " ")
        t = re.sub(r"[^a-z0-9 ]+", " ", t)
        t = re.sub(r"\s+", " ", t).strip()
        return t

    @staticmethod
    def _cell_text(v: Any) -> str:
        if v is None:
            return ""
        if isinstance(v, float) and pd.isna(v):
            return ""
        return str(v).strip()

    @staticmethod
    def _parse_date_cell(v: Any) -> Optional[date_type]:
        if isinstance(v, datetime):
            return v.date()
        if isinstance(v, date_type):
            return v
        try:
            if hasattr(v, "to_pydatetime"):
                return v.to_pydatetime().date()
        except Exception:
            pass
        if isinstance(v, (int, float)) and not (isinstance(v, float) and pd.isna(v)):
            fv = float(v)
            if 30000 <= fv <= 60000:
                try:
                    return (datetime(1899, 12, 30) + timedelta(days=int(fv))).date()
                except Exception:
                    return None
            return None
        if isinstance(v, str):
            s = v.strip()
            if not s:
                return None
            if not (_DATE_SEP_RE.search(s) or _YEAR_RE.search(s)):
                return None
            try:
                from dateutil import parser as date_parser

                return date_parser.parse(s, dayfirst=True, fuzzy=True).date()
            except Exception:
                return None
        return None
