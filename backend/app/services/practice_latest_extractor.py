from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

from app.services.calc_metrics_extractor import CalcMetricsExtractor, MetricHit
from app.services.calc_sheet_selector import CalcSheetSelector
from app.services.workbook_reader import WorkbookReader
from app.utils.address_normalizer import normalize_uk_address
from app.validators.normalizers import make_practice_key, to_text, to_date


@dataclass(frozen=True)
class PracticeLatestResult:
    practice_key: str
    practice_name: Optional[str]
    display_name: str
    address_text: Optional[str]
    postcode: Optional[str]
    city: Optional[str]
    county: Optional[str]
    address_line1: Optional[str]
    address_line2: Optional[str]
    visited_on: Optional[date]
    surgery_count: Optional[int]
    associate_cost_amount: Optional[Decimal]
    associate_cost_pct: Optional[Decimal]  # 0..100
    accounts_period_end: Optional[date]
    source_file: str
    raw_json: dict[str, Any]
    field_confidence: dict[str, float]
    evidence: dict[str, Any]
    missing_fields: list[str]
    low_conf_fields: list[str]


class PracticeLatestExtractor:
    """
    Produce a single 'latest-only' practice row from an .xlsx file.

    We reuse existing Calc sheet selection and metric extraction, then materialize:
    - surgery_count (from number_of_surgeries)
    - associate_cost_amount / associate_cost_pct (from Certified Accounts associates row)
    - accounts_period_end (latest certified accounts period detected)
    """

    def __init__(self, *, canonical_mapping_path: str | None = None, low_conf_threshold: float = 0.7) -> None:
        self._selector = CalcSheetSelector()
        self._reader = WorkbookReader()
        self._metrics = CalcMetricsExtractor(canonical_mapping_path=canonical_mapping_path)
        self._low_conf = low_conf_threshold

    def extract(self, xlsx_path: Path) -> PracticeLatestResult:
        selected = self._selector.select(xlsx_path)
        if selected is None:
            raise ValueError(f"No calc-like sheet found for {xlsx_path}")

        # Load all sheets once (some workbooks have Certified Accounts on non-calc tabs;
        # but the CalcMetricsExtractor can still find the table heuristically).
        grids = self._reader.read_xlsx(xlsx_path, max_rows=600, max_cols=140)
        sheet_map = {g.sheet_name: g.df for g in grids}
        df = sheet_map.get(selected.sheet_name)
        if df is None:
            raise ValueError(f"Selected sheet not found: {selected.sheet_name}")

        # Header identity from the selected calc-like sheet
        p_name, p_addr, header_conf = self._metrics.extract_practice_header(df)
        p_name = to_text(p_name)
        p_addr = to_text(p_addr)

        # Address normalization (postcode/county)
        addr_norm = normalize_uk_address(p_addr) if p_addr else None
        # Use normalized full_text for storage so we don't persist header junk
        # (we already truncate to postcode inside normalize_uk_address).
        address_text_clean = addr_norm.full_text if addr_norm else p_addr
        postcode = addr_norm.postcode if addr_norm else None
        county = addr_norm.county if addr_norm else None
        city = addr_norm.city if addr_norm else None
        address_line1 = addr_norm.address_line1 if addr_norm else None
        address_line2 = addr_norm.address_line2 if addr_norm else None
        visited_on = self._extract_visited_on(df)

        display_name = p_name or (xlsx_path.stem[:120])
        practice_name = p_name

        practice_key = make_practice_key(practice_name=practice_name, postcode=postcode)
        if not practice_key:
            # fallback: still produce a deterministic-ish key for logging/debug,
            # but mark it as low confidence (ingest can decide to skip/route to review)
            safe_name = re.sub(r"[^a-z0-9]+", " ", (practice_name or xlsx_path.stem).lower()).strip()
            safe_name = re.sub(r"\s+", " ", safe_name)[:80] or "unknown"
            practice_key = f"{safe_name}|UNKNOWN"

        # Extract metrics from ALL sheets (for Certified Accounts especially)
        all_hits: list[MetricHit] = []
        per_sheet_hits: dict[str, list[MetricHit]] = {}
        for sn, sdf in sheet_map.items():
            hits = self._metrics.extract_metrics(sdf)
            if hits:
                per_sheet_hits[sn] = hits
                all_hits.extend(hits)

        # Materialize surgery_count (prefer canonical key)
        surgery_hit = self._best_hit(all_hits, key="number_of_surgeries")
        surgery_count = int(surgery_hit.value_number) if (surgery_hit and surgery_hit.value_number is not None) else None
        if surgery_count is None:
            # Fallback: some sheets show "<n> surgeries" without a nearby "Number of surgeries" label.
            surgery_count = self._fallback_surgery_count(df)
            if surgery_count is not None and surgery_hit is None:
                # create synthetic hit for evidence/confidence
                surgery_hit = MetricHit(
                    metric_key="number_of_surgeries",
                    metric_label="surgeries (fallback)",
                    value_number=Decimal(surgery_count),
                    value_text=None,
                    unit=None,
                    confidence=0.75,
                    row=None,
                    col=None,
                )

        # Certified associates: pick latest period_end from keys emitted by extractor
        associates_amount, associates_pct, period_end, assoc_evidence, assoc_conf = self._latest_certified_associates(all_hits)

        # Certified accounts: materialize latest + previous year end fields (income/other/inc/etc).
        certified_bundle = self._latest_certified_accounts_latest_prev(all_hits)
        cert_mat = certified_bundle["materialized"]
        cert_conf = certified_bundle["field_confidence"]
        cert_evidence = certified_bundle["evidence"]

        # Core canonical metrics + UDA + Income split (selected types)
        core_bundle = self._materialize_core_and_income_split(all_hits, sheet_map)
        core_mat = core_bundle["materialized"]
        core_conf = core_bundle["field_confidence"]
        core_evidence = core_bundle["evidence"]

        # Field confidence (deterministic-ish)
        field_confidence: dict[str, float] = {}
        evidence: dict[str, Any] = {
            "selected_sheet": selected.sheet_name,
            "selected_sheet_as_of_date": selected.as_of_date.isoformat() if selected.as_of_date else None,
            "selected_sheet_date_source": selected.as_of_date_source,
            "header_confidence": header_conf,
            "header_lines": selected.practice_block_lines,
        }

        # Practice identity confidence
        field_confidence["practice_name"] = min(1.0, max(0.0, header_conf))
        field_confidence["postcode"] = 1.0 if postcode else 0.0
        field_confidence["county"] = 0.8 if county else 0.0
        field_confidence["city"] = 0.8 if city else 0.0
        field_confidence["address_line1"] = 0.6 if address_line1 else 0.0
        field_confidence["address_line2"] = 0.6 if address_line2 else 0.0
        field_confidence["visited_on"] = 0.9 if visited_on else 0.0

        # Surgery count confidence
        field_confidence["surgery_count"] = float(surgery_hit.confidence) if surgery_hit else 0.0
        evidence["surgery_count"] = self._hit_evidence(surgery_hit, sheet_map) if surgery_hit else None

        # Associate costs confidence
        field_confidence["associate_cost_amount"] = assoc_conf.get("associate_cost_amount", 0.0)
        field_confidence["associate_cost_pct"] = assoc_conf.get("associate_cost_pct", 0.0)
        evidence["associates"] = assoc_evidence

        # Certified accounts confidence (latest + prev)
        field_confidence.update(cert_conf)
        evidence["certified_accounts"] = cert_evidence

        # Core + UDA + income split confidence
        field_confidence.update(core_conf)
        evidence["core_metrics"] = core_evidence

        # accounts_period_end confidence: prefer certified date if present, else selected sheet date
        accounts_period_end = period_end or selected.as_of_date
        field_confidence["accounts_period_end"] = 1.0 if accounts_period_end else 0.0

        # Build raw payload for traceability/debug
        raw_json: dict[str, Any] = {
            "source_file": str(xlsx_path),
            "selected_sheet": {
                "sheet_name": selected.sheet_name,
                "score": selected.score,
                "as_of_date": selected.as_of_date.isoformat() if selected.as_of_date else None,
                "as_of_date_source": selected.as_of_date_source,
                "practice_block_lines": selected.practice_block_lines,
            },
            "practice": {
                "practice_name": practice_name,
                "address_text": address_text_clean,
                "postcode": postcode,
                "city": city,
                "county": county,
                "address_line1": address_line1,
                "address_line2": address_line2,
            },
            "materialized": {
                "practice_key": practice_key,
                "surgery_count": surgery_count,
                "associate_cost_amount": str(associates_amount) if associates_amount is not None else None,
                "associate_cost_pct": str(associates_pct) if associates_pct is not None else None,
                "accounts_period_end": accounts_period_end.isoformat() if accounts_period_end else None,
                "visited_on": visited_on.isoformat() if visited_on else None,
                **cert_mat,
                **core_mat,
            },
            "extraction": {
                "field_confidence": field_confidence,
                "evidence": evidence,
            },
        }

        missing_fields: list[str] = [k for k, v in field_confidence.items() if v <= 0.0]
        low_conf_fields: list[str] = [k for k, v in field_confidence.items() if 0.0 < v < self._low_conf]

        return PracticeLatestResult(
            practice_key=practice_key,
            practice_name=practice_name,
            display_name=display_name,
            address_text=address_text_clean,
            postcode=postcode,
            city=city,
            county=county,
            address_line1=address_line1,
            address_line2=address_line2,
            visited_on=visited_on,
            surgery_count=surgery_count,
            associate_cost_amount=associates_amount,
            associate_cost_pct=associates_pct,
            accounts_period_end=accounts_period_end,
            source_file=str(xlsx_path),
            raw_json=raw_json,
            field_confidence=field_confidence,
            evidence=evidence,
            missing_fields=missing_fields,
            low_conf_fields=low_conf_fields,
        )

    def _best_hit(self, hits: list[MetricHit], *, key: str) -> Optional[MetricHit]:
        best: Optional[MetricHit] = None
        for h in hits:
            if h.metric_key != key:
                continue
            if best is None or h.confidence > best.confidence:
                best = h
        return best

    def _latest_certified_associates(
        self, hits: list[MetricHit]
    ) -> tuple[Optional[Decimal], Optional[Decimal], Optional[date], dict[str, Any], dict[str, float]]:
        """
        CalcMetricsExtractor emits keys like:
          certified_associates_gbp__YYYY-MM-DD
          certified_associates_percent__YYYY-MM-DD
        We pick the latest YYYY-MM-DD and return amount + percent (0..100).
        """
        amount_by_date: dict[date, MetricHit] = {}
        pct_by_date: dict[date, MetricHit] = {}

        for h in hits:
            if not h.metric_key.startswith("certified_"):
                continue
            if "associates" not in h.metric_key:
                continue
            if "__" not in h.metric_key:
                continue
            base, d = h.metric_key.rsplit("__", 1)
            dte = to_date(d)
            if not dte:
                continue
            if base.endswith("associates_gbp"):
                amount_by_date[dte] = h
            elif base.endswith("associates_percent"):
                pct_by_date[dte] = h

        if not amount_by_date and not pct_by_date:
            return None, None, None, {"found": False}, {"associate_cost_amount": 0.0, "associate_cost_pct": 0.0}

        latest = max(set(amount_by_date.keys()) | set(pct_by_date.keys()))
        h_amt = amount_by_date.get(latest)
        h_pct = pct_by_date.get(latest)

        amt = h_amt.value_number if (h_amt and h_amt.value_number is not None) else None
        pct = h_pct.value_number if (h_pct and h_pct.value_number is not None) else None

        evidence = {
            "found": True,
            "period_end": latest.isoformat(),
            "amount_hit": self._hit_evidence(h_amt, None) if h_amt else None,
            "percent_hit": self._hit_evidence(h_pct, None) if h_pct else None,
        }

        conf_amount = float(h_amt.confidence) if h_amt else 0.0
        conf_pct = float(h_pct.confidence) if h_pct else 0.0

        # Small bonus if both amount+percent present for same period
        if h_amt and h_pct:
            conf_amount = min(1.0, conf_amount + 0.1)
            conf_pct = min(1.0, conf_pct + 0.1)

        conf = {"associate_cost_amount": conf_amount, "associate_cost_pct": conf_pct}
        return amt, pct, latest, evidence, conf

    def _latest_certified_accounts_latest_prev(self, hits: list[MetricHit]) -> dict[str, Any]:
        """
        Materialize Certified Accounts for two most recent year ends:
        - latest year_end (as in accounts_period_end)
        - previous year_end (year_end_prev)

        Produces stable keys under raw_json.materialized:
          certified_accounts_period_end_prev
          cert_<field>_gbp / cert_<field>_percent
          cert_<field>_gbp_prev / cert_<field>_percent_prev

        Fields:
          income, other_inc, associates, wages, hygiene, materials, labs, net_profit
        """
        fields = [
            "income",
            "other_inc",
            "associates",
            "wages",
            "hygiene",
            "materials",
            "labs",
            "net_profit",
        ]

        # date -> { field_variant -> MetricHit }
        by_date: dict[date, dict[str, MetricHit]] = {}
        for h in hits:
            if not isinstance(h.metric_key, str) or not h.metric_key.startswith("certified_") or "__" not in h.metric_key:
                continue
            base, d = h.metric_key.split("__", 1)
            dte = to_date(d)
            if not dte:
                continue
            field_variant = base.replace("certified_", "")
            by_date.setdefault(dte, {})[field_variant] = h

        if not by_date:
            # Return empty materialization but stable confidence keys
            empty_conf: dict[str, float] = {"certified_accounts_period_end_prev": 0.0}
            for f in fields:
                empty_conf[f"cert_{f}_gbp"] = 0.0
                empty_conf[f"cert_{f}_percent"] = 0.0
                empty_conf[f"cert_{f}_gbp_prev"] = 0.0
                empty_conf[f"cert_{f}_percent_prev"] = 0.0
            return {"materialized": {}, "field_confidence": empty_conf, "evidence": {"found": False}}

        dates = sorted(by_date.keys())
        latest = dates[-1]
        prev = dates[-2] if len(dates) >= 2 else None

        def pick(dte: date, key: str) -> MetricHit | None:
            return by_date.get(dte, {}).get(key)

        mat: dict[str, Any] = {}
        conf: dict[str, float] = {}
        ev: dict[str, Any] = {"found": True, "latest_year_end": latest.isoformat(), "prev_year_end": prev.isoformat() if prev else None}

        if prev:
            mat["certified_accounts_period_end_prev"] = prev.isoformat()
            conf["certified_accounts_period_end_prev"] = 1.0
        else:
            conf["certified_accounts_period_end_prev"] = 0.0

        for f in fields:
            # latest
            hk_gbp = f"{f}_gbp"
            hk_pct = f"{f}_percent"
            h_gbp = pick(latest, hk_gbp)
            h_pct = pick(latest, hk_pct)

            mat[f"cert_{f}_gbp"] = str(h_gbp.value_number) if (h_gbp and h_gbp.value_number is not None) else None
            mat[f"cert_{f}_percent"] = str(h_pct.value_number) if (h_pct and h_pct.value_number is not None) else None
            conf[f"cert_{f}_gbp"] = float(h_gbp.confidence) if h_gbp else 0.0
            conf[f"cert_{f}_percent"] = float(h_pct.confidence) if h_pct else 0.0

            # previous
            if prev:
                hp_gbp = pick(prev, hk_gbp)
                hp_pct = pick(prev, hk_pct)
                mat[f"cert_{f}_gbp_prev"] = str(hp_gbp.value_number) if (hp_gbp and hp_gbp.value_number is not None) else None
                mat[f"cert_{f}_percent_prev"] = str(hp_pct.value_number) if (hp_pct and hp_pct.value_number is not None) else None
                conf[f"cert_{f}_gbp_prev"] = float(hp_gbp.confidence) if hp_gbp else 0.0
                conf[f"cert_{f}_percent_prev"] = float(hp_pct.confidence) if hp_pct else 0.0
            else:
                conf[f"cert_{f}_gbp_prev"] = 0.0
                conf[f"cert_{f}_percent_prev"] = 0.0

            ev[f] = {
                "latest": {"gbp": self._hit_evidence(h_gbp, None) if h_gbp else None, "percent": self._hit_evidence(h_pct, None) if h_pct else None},
                "prev": {"gbp": self._hit_evidence(pick(prev, hk_gbp), None) if prev and pick(prev, hk_gbp) else None,
                         "percent": self._hit_evidence(pick(prev, hk_pct), None) if prev and pick(prev, hk_pct) else None},
            }

        return {"materialized": mat, "field_confidence": conf, "evidence": ev}

    def _hit_evidence(self, hit: Optional[MetricHit], sheet_map: dict[str, Any] | None) -> Optional[dict[str, Any]]:
        if hit is None:
            return None
        return {
            "metric_key": hit.metric_key,
            "metric_label": hit.metric_label,
            "value_number": str(hit.value_number) if hit.value_number is not None else None,
            "value_text": hit.value_text,
            "unit": hit.unit,
            "confidence": hit.confidence,
            "row": hit.row,
            "col": hit.col,
        }

    def _fallback_surgery_count(self, df: Any) -> Optional[int]:
        """
        Scan the selected calc-like sheet for patterns like "3 surgeries" / "3 surg".
        Returns the first plausible count found near the top-left region.
        """
        try:
            import re

            rx = re.compile(r"\b(\d{1,2})\s*(?:surger(?:y|ies)|surg)\b", re.IGNORECASE)
            max_r = min(getattr(df, "shape", [0, 0])[0], 60)
            max_c = min(getattr(df, "shape", [0, 0])[1], 40)
            for r in range(max_r):
                for c in range(max_c):
                    v = df.iat[r, c]
                    if not isinstance(v, str):
                        continue
                    m = rx.search(v)
                    if not m:
                        continue
                    n = int(m.group(1))
                    if 0 < n <= 20:
                        return n
        except Exception:
            return None
        return None

    def _extract_visited_on(self, df: Any) -> Optional[date]:
        """
        Extract "Visited on" date from the header area.
        Typically appears as a label with the date in the cell to the right.
        """
        try:
            import re

            rx = re.compile(r"\bvisited\s+on\b", re.IGNORECASE)
            max_r = min(getattr(df, "shape", [0, 0])[0], 80)
            max_c = min(getattr(df, "shape", [0, 0])[1], 40)
            for r in range(max_r):
                for c in range(max_c):
                    v = df.iat[r, c]
                    if not isinstance(v, str):
                        continue
                    if not rx.search(v):
                        continue
                    # right neighbor usually holds the date
                    if c + 1 < getattr(df, "shape", [0, 0])[1]:
                        d = to_date(df.iat[r, c + 1])
                        if d:
                            return d
                    # fallback: same cell like "Visited on 12.08.22"
                    d = to_date(v)
                    if d:
                        return d
        except Exception:
            return None
        return None

    def _materialize_core_and_income_split(self, hits: list[MetricHit], sheet_map: dict[str, Any]) -> dict[str, Any]:
        """
        Materialize additional fields requested for the practices table:
        - core canonical metrics: goodwill, efandf, total, freehold, grand_total
        - NHS contract details: nhs_contract_number, uda_* fields
        - split of income: fpi/nhs/denplan/rent (percent/value/applied_*)
        """
        core_keys = ["goodwill", "efandf", "total", "freehold", "grand_total"]
        uda_keys = ["nhs_contract_number", "uda_contract_value_gbp", "uda_count", "uda_rate_gbp", "uda_uplift_value_gbp"]
        income_types = ["fpi", "nhs", "denplan", "rent"]
        income_suffixes = ["percent", "value", "applied_percent", "applied_value"]

        mat: dict[str, Any] = {}
        conf: dict[str, float] = {}
        ev: dict[str, Any] = {"core": {}, "uda": {}, "income_split": {}}

        # Core metrics: choose best hit by confidence
        for k in core_keys:
            h = self._best_hit(hits, key=k)
            mat[k] = str(h.value_number) if (h and h.value_number is not None) else None
            conf[k] = float(h.confidence) if h else 0.0
            ev["core"][k] = self._hit_evidence(h, sheet_map) if h else None

        # UDA / NHS contract details
        for k in uda_keys:
            h = self._best_hit(hits, key=k)
            if k == "nhs_contract_number":
                mat[k] = h.value_text if h else None
            else:
                mat[k] = str(h.value_number) if (h and h.value_number is not None) else None
            conf[k] = float(h.confidence) if h else 0.0
            ev["uda"][k] = self._hit_evidence(h, sheet_map) if h else None

        # Split of income: deterministic keys already emitted by CalcMetricsExtractor
        hit_by_key: dict[str, MetricHit] = {}
        for h in hits:
            if h.metric_key.startswith("income_split_"):
                prev = hit_by_key.get(h.metric_key)
                if prev is None or h.confidence > prev.confidence:
                    hit_by_key[h.metric_key] = h

        for t in income_types:
            ev["income_split"][t] = {}
            for suf in income_suffixes:
                key = f"income_split_{t}_{suf}"
                h = hit_by_key.get(key)
                out_key = key  # keep same name in materialized/json
                mat[out_key] = str(h.value_number) if (h and h.value_number is not None) else None
                conf[out_key] = float(h.confidence) if h else 0.0
                ev["income_split"][t][suf] = self._hit_evidence(h, sheet_map) if h else None

        return {"materialized": mat, "field_confidence": conf, "evidence": ev}

