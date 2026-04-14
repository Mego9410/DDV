from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from datetime import date as date_type, datetime
from typing import Any, Optional

import pandas as pd
from dateutil import parser as date_parser

from app.validators.normalizers import to_decimal, to_text


@dataclass(frozen=True)
class MetricHit:
    metric_key: str
    metric_label: str
    value_number: Optional[Decimal]
    value_text: Optional[str]
    unit: Optional[str]
    confidence: float
    row: Optional[int]
    col: Optional[int]


class CalcMetricsExtractor:
    """
    Extract metrics from a Calc/Update/Calculation sheet.

    Strategy:
    - Parse practice name/address from the top-left "Practice of" / "Practice address" block (line-based).
    - Extract canonical metrics from common label/value pairs (fuzzy-ish via regex + alias lists).
    - Also extract a catch-all set of label/value pairs for later expansion and mapping.
    """

    # Canonical keys we know appear frequently in your files
    CANONICAL_SPECS: dict[str, list[str]] = {
        "goodwill": ["goodwill"],
        "gdwill_perc": ["gdwill perc", "goodwill perc", "goodwill %", "gdwill %", "% goodwill"],
        "efandf": ["ef&f", "e f & f", "efandf", "equipment fittings & furniture", "fixtures fittings"],
        "total": ["total"],
        "grand_total": ["grand total"],
        "freehold": ["freehold"],
        "number_of_surgeries": ["number of surgeries", "no of surgeries", "surgery count", "surgeries"],
        "fpi": ["fpi"],
    }

    def __init__(self, *, canonical_mapping_path: str | None = None) -> None:
        """
        If canonical_mapping_path is provided, it should be a JSON file shaped like:
        {
          "canonical_specs": {
             "goodwill_value": ["goodwill", "good will", ...],
             ...
          }
        }
        This allows us to learn/update mappings from your corpus without code changes.
        """
        if canonical_mapping_path:
            p = Path(canonical_mapping_path)
            if p.exists():
                import json

                payload = json.loads(p.read_text(encoding="utf-8"))
                specs = payload.get("canonical_specs")
                if isinstance(specs, dict):
                    # basic validation: lists of strings
                    cleaned: dict[str, list[str]] = {}
                    for k, v in specs.items():
                        if isinstance(k, str) and isinstance(v, list) and all(isinstance(x, str) for x in v):
                            cleaned[k] = v
                    if cleaned:
                        self.CANONICAL_SPECS = cleaned

    def extract_practice_header(self, df: pd.DataFrame) -> tuple[Optional[str], Optional[str], float]:
        lines = self._top_left_lines(df)
        name = None
        addr = None
        conf = 0.0
        # Addresses often wrap across multiple rows/cells. We capture the "Practice address"
        # line and then append the next few non-empty lines that look like address continuations.
        def looks_like_continuation(s: str) -> bool:
            sl = s.strip().lower()
            if not sl:
                return False
            if sl.startswith("practice of") or "practice address" in sl:
                return False
            # stop at obvious section headers / non-address text
            if "valuation" in sl or "method" in sl or "methods" in sl:
                return False
            if sl in {"values", "value"} or sl.startswith("values "):
                return False
            if "calculation methods" in sl or "calculation method" in sl:
                return False
            if "calculation" in sl and "address" not in sl:
                return False
            return True

        scan = lines[:12]
        for i, l in enumerate(scan):
            ll = l.lower()
            if ll.startswith("practice of"):
                name = l.split(":", 1)[1].strip() if ":" in l else l.replace("Practice of", "").strip()
                conf += 0.5
            if "practice address" in ll:
                base = l.split(":", 1)[1].strip() if ":" in l else l.replace("Practice address", "").strip()
                cont: list[str] = []
                # pull up to 3 continuation lines (usually street/town/county/postcode)
                for nxt in scan[i + 1 : i + 4]:
                    if looks_like_continuation(nxt):
                        cont.append(nxt.strip())
                joined = ", ".join([x for x in [base] + cont if x])
                addr = joined
                conf += 0.5
        if name or addr:
            conf = min(1.0, conf + 0.2)
        return to_text(name), to_text(addr), conf

    def extract_metrics(self, df: pd.DataFrame, *, max_rows: int = 350, max_cols: int = 90) -> list[MetricHit]:
        df = df.iloc[:max_rows, :max_cols]
        pairs = self._extract_pairs(df)

        # Build canonical best-hits
        best: dict[str, MetricHit] = {}
        for label, value, r, c in pairs:
            norm_label = self._norm(label)
            # determine unit/typed value
            unit, num, txt = self._coerce_value(value)
            for key, aliases in self.CANONICAL_SPECS.items():
                if self._matches_any(norm_label, aliases):
                    # Avoid mapping "Goodwill %" into goodwill_value
                    if key == "goodwill_value" and ("perc" in norm_label or "%" in norm_label):
                        continue
                    conf = self._label_confidence(norm_label, aliases)
                    hit = MetricHit(
                        metric_key=key,
                        metric_label=label,
                        value_number=num,
                        value_text=txt,
                        unit=unit,
                        confidence=conf,
                        row=r,
                        col=c,
                    )
                    if key not in best or hit.confidence > best[key].confidence:
                        best[key] = hit

        # Add structured region extractions (tables/blocks)
        structured = []
        structured.extend(self._extract_split_of_income(df))
        structured.extend(self._extract_uda_block(df))
        structured.extend(self._extract_certified_accounts(df))
        # Boost extraction for merged-cell layouts
        structured.extend(self._extract_local_numeric_near_label(df, label_pattern=r"number\s+of\s+surger", key="number_of_surgeries", unit=None))
        structured.extend(self._extract_local_numeric_near_label(df, label_pattern=r"\bfreehold\b", key="freehold", unit="gbp"))

        # Catch-all metrics: store as key=slug(label) if not canonical
        out: list[MetricHit] = list(best.values()) + structured
        for label, value, r, c in pairs:
            slug = self._slug(label)
            if not slug:
                continue
            if f"raw:{slug}" in {h.metric_key for h in out}:
                continue
            unit, num, txt = self._coerce_value(value)
            out.append(
                MetricHit(
                    metric_key=f"raw:{slug}",
                    metric_label=label,
                    value_number=num,
                    value_text=txt,
                    unit=unit,
                    confidence=0.4,  # catch-all baseline
                    row=r,
                    col=c,
                )
            )

        return out

    def _extract_certified_accounts(self, df: pd.DataFrame) -> list[MetricHit]:
        """
        Extract the 'CERTIFIED ACCOUNTS' multi-year table.

        We emit one metric per (year_end_date, field), encoded into the metric_key:
          certified_<field>_gbp__YYYY-MM-DD
          certified_<field>_percent__YYYY-MM-DD

        Fields tracked (from your screenshot):
        - income, other_inc, associates, wages, hygiene, materials, labs, net_profit
        - plus their adjacent Percent columns
        """
        hits: list[MetricHit] = []
        anchor = self._find_text_cell(df, r"certified\s+accounts")
        if anchor is not None:
            ar, _ac = anchor
            # Work in a window below the anchor; Certified Accounts is usually wide.
            win = df.iloc[ar : min(ar + 110, df.shape[0]), : min(90, df.shape[1])].copy()
            row_offset = ar
        else:
            # Fallback: scan the whole sheet for a Certified Accounts-like table by shape.
            # We look for a column with multiple year-end dates AND nearby header keywords.
            ar = 0
            win = df.iloc[: min(500, df.shape[0]), : min(120, df.shape[1])].copy()
            row_offset = 0

        def parse_date_cell(v: Any):
            # Accept real date/datetime/Timestamp values
            if isinstance(v, (date_type, datetime)):
                return v.date() if isinstance(v, datetime) else v
            try:
                # pandas Timestamp
                if hasattr(v, "to_pydatetime"):
                    dt = v.to_pydatetime()
                    if isinstance(dt, datetime):
                        return dt.date()
            except Exception:
                pass

            # Excel serial dates sometimes appear as floats/ints
            if isinstance(v, (int, float)) and not (isinstance(v, float) and pd.isna(v)):
                fv = float(v)
                # Rough Excel serial date range for modern sheets
                if 30000 <= fv <= 60000:
                    try:
                        # Excel epoch: 1899-12-30
                        from datetime import timedelta

                        return (datetime(1899, 12, 30) + timedelta(days=int(fv))).date()
                    except Exception:
                        return None

            if isinstance(v, str):
                s = v.strip()
                if not s:
                    return None
                return self._parse_date_safe(s)
            return None

        # Find candidate "year end" columns by detecting columns with multiple date-like cells.
        # Consider first 30 columns (dates are usually near the left but not always).
        candidates: list[tuple[int, int, int]] = []  # (score, col, first_data_r)
        for c in range(min(30, win.shape[1])):
            date_rows: list[int] = []
            for r in range(0, min(60, win.shape[0])):
                if parse_date_cell(win.iat[r, c]):
                    date_rows.append(r)
            if len(date_rows) < 2:
                continue

            first_data_r = date_rows[0]
            header_r = max(0, first_data_r - 1)

            # Score: presence of key headers near header row(s)
            header_band = " ".join(
                self._cell_to_text(win.iat[rr, cc]).lower()
                for rr in range(max(0, header_r - 2), min(header_r + 2, win.shape[0]))
                for cc in range(0, min(60, win.shape[1]))
            )
            score = 0
            for kw in ["income", "wages", "labs", "net profit", "associates", "materials", "hyg"]:
                if kw in header_band:
                    score += 2
            # Additional score: do rows contain large numbers in nearby columns
            for rr in date_rows[:3]:
                row_nums = 0
                for cc in range(c + 1, min(c + 20, win.shape[1])):
                    _u, n, _t = self._coerce_value(win.iat[rr, cc])
                    if n is not None and abs(float(n)) >= 500:
                        row_nums += 1
                if row_nums >= 3:
                    score += 2
            candidates.append((score, c, first_data_r))

        if not candidates:
            return hits
        candidates.sort(reverse=True)
        _best_score, col_year_end, first_data_r = candidates[0]
        header_r = max(0, first_data_r - 1)

        def cell_num(rr: int, cc: int):
            if cc >= win.shape[1] or rr >= win.shape[0]:
                return None
            _u, n, _t = self._coerce_value(win.iat[rr, cc])
            return n

        def looks_amount(n):
            return n is not None and abs(float(n)) >= 500

        def looks_percent(n):
            return n is not None and 0 <= float(n) <= 100

        # Scan a small header block (header_r-2..header_r+1) to locate each field,
        # then choose the column that looks like an amount in the first data row.
        header_rows = list(range(max(0, header_r - 4), min(header_r + 4, win.shape[0])))
        fields = {
            "income_gbp": ["income"],
            "other_inc_gbp": ["other inc", "other income"],
            "associates_gbp": ["associates", "associate"],
            "wages_gbp": ["wages", "wage"],
            "hygiene_gbp": ["hygiene", "hyg"],
            "materials_gbp": ["materials", "material"],
            "labs_gbp": ["labs", "lab"],
            "net_profit_gbp": ["net profit", "netprofit"],
        }

        col_map: dict[str, int] = {}
        for out_field, needles in fields.items():
            candidates: list[int] = []
            for rr in header_rows:
                for cc in range(0, min(win.shape[1], 90)):
                    ht = self._cell_to_text(win.iat[rr, cc]).strip().lower()
                    if not ht:
                        continue
                    if any(n in ht for n in needles):
                        candidates.append(cc)
            # choose best candidate: amount-looking in first data row, else leftmost
            chosen = None
            for cc in sorted(set(candidates)):
                n = cell_num(first_data_r, cc)
                if looks_amount(n):
                    chosen = cc
                    break
            if chosen is None and candidates:
                chosen = sorted(set(candidates))[0]
            if chosen is not None:
                col_map[out_field] = chosen

        # Helper: given a money column, find the percent column immediately to its right if header says Percent
        def percent_col_for(money_col: int) -> int | None:
            # Prefer a header cell labeled Percent; fallback to a column whose first-row value looks like percent.
            for dc in range(1, 4):
                cc = money_col + dc
                if cc >= win.shape[1]:
                    break
                ht = self._cell_to_text(win.iat[header_r, cc]).strip().lower()
                if ht in {"percent", "%"} or "percent" in ht:
                    return cc
            for dc in range(1, 4):
                cc = money_col + dc
                if cc >= win.shape[1]:
                    break
                n = cell_num(first_data_r, cc)
                if looks_percent(n):
                    return cc
            return None

        # Parse contiguous rows; stop after first gap once started.
        started = False
        # Parse contiguous rows; stop after first gap once started.
        started = False
        for r in range(first_data_r, min(first_data_r + 15, win.shape[0])):
            year_cell = self._cell_to_text(win.iat[r, col_year_end]).strip()
            ydate = self._parse_date_safe(year_cell)
            if ydate is None:
                if started:
                    break
                continue
            started = True
            ykey = ydate.isoformat()

            for field, money_col in list(col_map.items()):
                if not field.endswith("_gbp"):
                    continue
                if money_col >= win.shape[1]:
                    continue
                unit, num, _txt = self._coerce_value(win.iat[r, money_col])
                if num is not None:
                    # If this column looks like a percent, skip as a GBP field
                    if looks_percent(num) and not looks_amount(num):
                        continue
                    hits.append(
                        MetricHit(
                            metric_key=f"certified_{field}__{ykey}",
                            metric_label=f"{field} ({ykey})",
                            value_number=num,
                            value_text=None,
                            unit="gbp",
                            confidence=0.9,
                            row=row_offset + r,
                            col=money_col,
                        )
                    )

                pc = percent_col_for(money_col)
                if pc is not None and pc < win.shape[1]:
                    unitp, nump, _txtp = self._coerce_value(win.iat[r, pc])
                    if nump is not None:
                        if not looks_percent(nump):
                            continue
                        hits.append(
                            MetricHit(
                                metric_key=f"certified_{field.replace('_gbp','_percent')}__{ykey}",
                                metric_label=f"{field} percent ({ykey})",
                                value_number=nump,
                                value_text=None,
                                unit="percent",
                                confidence=0.9,
                                row=row_offset + r,
                                col=pc,
                            )
                        )
        return hits

    def _parse_date_safe(self, s: str):
        try:
            return date_parser.parse(s, dayfirst=True, fuzzy=True).date()
        except Exception:
            return None

    def _extract_split_of_income(self, df: pd.DataFrame) -> list[MetricHit]:
        """
        Extract the 'SPLIT OF INCOME' table into stable metric keys:
        - income_split_<type>_percent
        - income_split_<type>_value
        - income_split_<type>_applied_percent
        - income_split_<type>_applied_value
        """
        hits: list[MetricHit] = []
        # Find header row by either:
        # - explicit "split of income" title nearby, or
        # - direct detection of a row containing "income type" + "percent"
        anchor = self._find_text_cell(df, r"(split\s+of\s+income|income\s+split|split\s+income)")
        search_start = anchor[0] if anchor else 0
        header_r = None
        for r in range(search_start, min(search_start + 120, df.shape[0])):
            row_text = " ".join(self._cell_to_text(df.iat[r, c]).lower() for c in range(min(df.shape[1], 20)))
            if "income type" in row_text and "percent" in row_text:
                header_r = r
                break
        if header_r is None:
            return hits

        # Determine column offsets by scanning header cells
        col_map: dict[str, int] = {}
        for c in range(0, min(df.shape[1], 20)):
            t = self._cell_to_text(df.iat[header_r, c]).lower()
            if "income type" in t:
                col_map["type"] = c
            elif t.strip() == "percent":
                col_map.setdefault("percent", c)
            elif t.strip() == "£" or t.strip() == "gbp":
                # could be two £ columns; first encountered is value, second is applied_value
                if "value" not in col_map:
                    col_map["value"] = c
                else:
                    col_map["applied_value"] = c
            elif "% applied" in t:
                col_map["applied_percent"] = c

        # fallback assumptions if headers are blank but structure matches screenshot
        c_type = col_map.get("type", 0)
        c_percent = col_map.get("percent", c_type + 1)
        c_value = col_map.get("value", c_type + 2)
        c_applied_percent = col_map.get("applied_percent", c_type + 3)
        c_applied_value = col_map.get("applied_value", c_type + 4)

        for r in range(header_r + 1, min(header_r + 60, df.shape[0])):
            t = self._cell_to_text(df.iat[r, c_type]).strip()
            if not t:
                continue
            tl = t.lower()
            if tl in {"100.0", "100", "total"}:
                # totals row; ignore (we already have total elsewhere)
                continue
            if re.fullmatch(r"\d+(\.\d+)?", tl):
                continue
            if "income type" in tl:
                continue
            if "split of income" in tl:
                continue

            # normalize common variants into stable types
            if tl in {"den plan", "denplan"}:
                t = "Denplan"
            if tl in {"fpi and dpas", "fpi/dpas"}:
                t = "FPI"

            key_type = self._slug(t).replace("raw:", "").replace("income_", "").replace(" ", "_")
            key_type = re.sub(r"^raw_", "", key_type)
            key_type = re.sub(r"[^a-z0-9_]+", "_", key_type.lower())
            key_type = key_type.strip("_")[:40]
            if not key_type:
                continue

            # percent
            unit_p, num_p, txt_p = self._coerce_value(df.iat[r, c_percent] if c_percent < df.shape[1] else None)
            if num_p is not None:
                hits.append(
                    MetricHit(
                        metric_key=f"income_split_{key_type}_percent",
                        metric_label=f"{t} Percent",
                        value_number=num_p,
                        value_text=txt_p,
                        unit="percent",
                        confidence=0.9,
                        row=r,
                        col=c_percent,
                    )
                )
            # value
            unit_v, num_v, txt_v = self._coerce_value(df.iat[r, c_value] if c_value < df.shape[1] else None)
            if num_v is not None:
                hits.append(
                    MetricHit(
                        metric_key=f"income_split_{key_type}_value",
                        metric_label=f"{t} £",
                        value_number=num_v,
                        value_text=txt_v,
                        unit="gbp",
                        confidence=0.9,
                        row=r,
                        col=c_value,
                    )
                )
            # applied percent
            unit_ap, num_ap, txt_ap = self._coerce_value(df.iat[r, c_applied_percent] if c_applied_percent < df.shape[1] else None)
            if num_ap is not None:
                hits.append(
                    MetricHit(
                        metric_key=f"income_split_{key_type}_applied_percent",
                        metric_label=f"{t} % applied",
                        value_number=num_ap,
                        value_text=txt_ap,
                        unit="percent",
                        confidence=0.9,
                        row=r,
                        col=c_applied_percent,
                    )
                )
            # applied value
            unit_av, num_av, txt_av = self._coerce_value(df.iat[r, c_applied_value] if c_applied_value < df.shape[1] else None)
            if num_av is not None:
                hits.append(
                    MetricHit(
                        metric_key=f"income_split_{key_type}_applied_value",
                        metric_label=f"{t} applied £",
                        value_number=num_av,
                        value_text=txt_av,
                        unit="gbp",
                        confidence=0.9,
                        row=r,
                        col=c_applied_value,
                    )
                )

            # Stop if we hit a totals percent row (100.0) in percent column
            perc_text = self._cell_to_text(df.iat[r, c_percent] if c_percent < df.shape[1] else "").strip()
            if perc_text.startswith("100"):
                break

        return hits

    def _extract_uda_block(self, df: pd.DataFrame) -> list[MetricHit]:
        """
        Extract UDA figures from the 'NHS CONTRACT NUMBER' block:
        - nhs_contract_number
        - uda_contract_value_gbp
        - uda_count
        - uda_rate_gbp
        Optionally uplift row:
        - uda_uplift_value_gbp
        """
        hits: list[MetricHit] = []
        anchor = self._find_text_cell(df, r"nhs\s+contract\s+number")
        if anchor is None:
            return hits
        ar, ac = anchor

        # Attempt to capture the contract number (usually the value in the cell to the right).
        try:
            v = df.iat[ar, min(ac + 1, df.shape[1] - 1)] if df.shape[1] > 0 else None
            t = self._cell_to_text(v).strip()
            if t and len(t) <= 80 and re.search(r"\d", t):
                hits.append(
                    MetricHit(
                        metric_key="nhs_contract_number",
                        metric_label="NHS contract number",
                        value_number=None,
                        value_text=t,
                        unit=None,
                        confidence=0.85,
                        row=ar,
                        col=min(ac + 1, df.shape[1] - 1),
                    )
                )
        except Exception:
            pass

        # Search a small region beneath for a row containing "UDA" and "£UDA"
        for r in range(ar, min(ar + 12, df.shape[0])):
            row_join = " ".join(self._cell_to_text(df.iat[r, c]).lower() for c in range(min(df.shape[1], 20)))
            if "uda" not in row_join:
                continue

            # Find positions of "UDA" and "£UDA" labels in the row
            uda_c = None
            puda_c = None
            for c in range(0, min(df.shape[1], 30)):
                t = self._cell_to_text(df.iat[r, c]).strip().lower()
                if t == "uda":
                    uda_c = c
                if "£uda" in t or t == "£uda":
                    puda_c = c

            # Heuristic: contract value is usually a numeric cell a couple columns before "UDA"
            if uda_c is not None:
                for vc in range(max(0, uda_c - 3), uda_c):
                    unit, num, _ = self._coerce_value(df.iat[r, vc])
                    if num is not None:
                        hits.append(
                            MetricHit(
                                metric_key="uda_contract_value_gbp",
                                metric_label="UDA contract value",
                                value_number=num,
                                value_text=None,
                                unit="gbp",
                                confidence=0.85,
                                row=r,
                                col=vc,
                            )
                        )
                        break
                # UDA count numeric cell after "UDA"
                if uda_c + 1 < df.shape[1]:
                    unit, num, _ = self._coerce_value(df.iat[r, uda_c + 1])
                    if num is not None:
                        hits.append(
                            MetricHit(
                                metric_key="uda_count",
                                metric_label="UDA count",
                                value_number=num,
                                value_text=None,
                                unit=None,
                                confidence=0.85,
                                row=r,
                                col=uda_c + 1,
                            )
                        )
            # UDA rate numeric cell after "£UDA"
            if puda_c is not None and puda_c + 1 < df.shape[1]:
                unit, num, _ = self._coerce_value(df.iat[r, puda_c + 1])
                if num is not None:
                    hits.append(
                        MetricHit(
                            metric_key="uda_rate_gbp",
                            metric_label="UDA rate",
                            value_number=num,
                            value_text=None,
                            unit="gbp",
                            confidence=0.85,
                            row=r,
                            col=puda_c + 1,
                        )
                    )

            # Uplift row (contains "uplift")
            for rr in range(r + 1, min(r + 4, df.shape[0])):
                uplift_join = " ".join(self._cell_to_text(df.iat[rr, c]).lower() for c in range(min(df.shape[1], 20)))
                if "uplift" in uplift_join:
                    # numeric in same column as contract value
                    if uda_c is not None:
                        for vc in range(max(0, uda_c - 3), uda_c):
                            unit, num, _ = self._coerce_value(df.iat[rr, vc])
                            if num is not None:
                                hits.append(
                                    MetricHit(
                                        metric_key="uda_uplift_value_gbp",
                                        metric_label="UDA uplift value",
                                        value_number=num,
                                        value_text=None,
                                        unit="gbp",
                                        confidence=0.8,
                                        row=rr,
                                        col=vc,
                                    )
                                )
                                break
                    break

            break

        return hits

    def _extract_local_numeric_near_label(
        self, df: pd.DataFrame, *, label_pattern: str, key: str, unit: str | None, search_right: int = 10
    ) -> list[MetricHit]:
        hits: list[MetricHit] = []
        pos = self._find_text_cell(df, label_pattern)
        if pos is None:
            return hits
        r, c = pos
        # Search rightwards for the first numeric-looking value
        for cc in range(c + 1, min(c + 1 + search_right, df.shape[1])):
            _u, num, txt = self._coerce_value(df.iat[r, cc])
            if num is not None:
                hits.append(
                    MetricHit(
                        metric_key=key,
                        metric_label=self._cell_to_text(df.iat[r, c]),
                        value_number=num,
                        value_text=txt,
                        unit=unit,
                        confidence=0.9,
                        row=r,
                        col=cc,
                    )
                )
                return hits
        # Also try a couple rows below (merged cell layouts)
        for rr in range(r + 1, min(r + 3, df.shape[0])):
            for cc in range(c, min(c + 1 + search_right, df.shape[1])):
                _u, num, txt = self._coerce_value(df.iat[rr, cc])
                if num is not None:
                    hits.append(
                        MetricHit(
                            metric_key=key,
                            metric_label=self._cell_to_text(df.iat[r, c]),
                            value_number=num,
                            value_text=txt,
                            unit=unit,
                            confidence=0.85,
                            row=rr,
                            col=cc,
                        )
                    )
                    return hits
        return hits

    def _find_text_cell(self, df: pd.DataFrame, pattern: str) -> tuple[int, int] | None:
        rx = re.compile(pattern, re.IGNORECASE)
        max_r = min(df.shape[0], 250)
        max_c = min(df.shape[1], 40)
        for r in range(max_r):
            for c in range(max_c):
                v = df.iat[r, c]
                if not isinstance(v, str):
                    continue
                if rx.search(v.strip()):
                    return (r, c)
        return None

    def _top_left_lines(self, df: pd.DataFrame, *, rows: int = 12, cols: int = 6) -> list[str]:
        region = df.iloc[:rows, :cols]
        lines: list[str] = []
        for r in range(region.shape[0]):
            parts = [self._cell_to_text(region.iat[r, c]) for c in range(region.shape[1])]
            line = " ".join([p for p in parts if p])
            line = re.sub(r"\s+", " ", line).strip()
            if line:
                lines.append(line)
        # de-dupe
        out: list[str] = []
        seen: set[str] = set()
        for l in lines:
            k = l.lower()
            if k in seen:
                continue
            seen.add(k)
            out.append(l)
        return out

    def _extract_pairs(self, df: pd.DataFrame) -> list[tuple[str, Any, int, int]]:
        hits: list[tuple[str, Any, int, int]] = []
        rows, cols = df.shape
        for r in range(rows):
            for c in range(cols):
                cell = df.iat[r, c]
                label = self._cell_to_text(cell)
                if not self._is_labelish(label):
                    continue

                # Same-cell "Label: Value"
                if isinstance(cell, str) and ":" in cell and len(cell) <= 140:
                    left, right = cell.split(":", 1)
                    left = left.strip()
                    right = right.strip()
                    if self._is_labelish(left) and right:
                        hits.append((left, right, r, c))
                        continue

                # Right cell value
                if c + 1 < cols:
                    v = df.iat[r, c + 1]
                    if self._is_valueish(v):
                        hits.append((label, v, r, c))
                        continue

                # Two-right (common when there is a "£" column)
                if c + 2 < cols:
                    v = df.iat[r, c + 2]
                    if self._is_valueish(v):
                        hits.append((label, v, r, c))
                        continue

        return hits

    def _is_labelish(self, s: str) -> bool:
        if not s:
            return False
        if len(s) < 2:
            return False
        sl = s.strip().lower()
        if re.fullmatch(r"[-+]?\d+(\.\d+)?", sl):
            return False
        # ignore obvious header noise
        if sl in {"£", "%", "value", "values"}:
            return False
        return True

    def _is_valueish(self, v: Any) -> bool:
        if v is None:
            return False
        if isinstance(v, float) and pd.isna(v):
            return False
        if isinstance(v, (int, float, Decimal)):
            return True
        if isinstance(v, str):
            s = v.strip()
            if not s:
                return False
            if len(s) > 180:
                return False
            # number-ish or short text
            return bool(re.search(r"\d", s)) or len(s) <= 60
        return False

    def _coerce_value(self, v: Any) -> tuple[Optional[str], Optional[Decimal], Optional[str]]:
        if isinstance(v, str):
            s = v.strip()
            if s.endswith("%"):
                num = to_decimal(s[:-1])
                return ("percent", num, None) if num is not None else (None, None, to_text(s))
        num = to_decimal(v)
        if num is not None:
            return ("gbp" if self._looks_currency(v) else None, num, None)
        return (None, None, to_text(v))

    def _looks_currency(self, v: Any) -> bool:
        if isinstance(v, str):
            return "£" in v or "$" in v or "€" in v
        return False

    def _cell_to_text(self, v: Any) -> str:
        if v is None:
            return ""
        if isinstance(v, float) and pd.isna(v):
            return ""
        return str(v).strip()

    def _norm(self, s: str) -> str:
        s = s.lower()
        s = s.replace("&", "and")
        s = re.sub(r"[^a-z0-9% ]+", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s

    def _matches_any(self, norm_label: str, aliases: list[str]) -> bool:
        for a in aliases:
            na = self._norm(a)
            if na and na in norm_label:
                return True
        return False

    def _label_confidence(self, norm_label: str, aliases: list[str]) -> float:
        # simple heuristic: exact alias containment is higher
        for a in aliases:
            na = self._norm(a)
            if norm_label == na:
                return 0.95
            if na and na in norm_label:
                return 0.85
        return 0.7

    def _slug(self, label: str) -> str:
        s = self._norm(label)
        if not s:
            return ""
        s = s.replace("%", "percent")
        s = re.sub(r"\s+", "_", s)
        return s[:80]

