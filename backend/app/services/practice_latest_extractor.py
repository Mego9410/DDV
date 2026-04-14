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
    county: Optional[str]
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
        postcode = addr_norm.postcode if addr_norm else None
        county = addr_norm.county if addr_norm else None

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

        # Certified associates: pick latest period_end from keys emitted by extractor
        associates_amount, associates_pct, period_end, assoc_evidence, assoc_conf = self._latest_certified_associates(all_hits)

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

        # Surgery count confidence
        field_confidence["surgery_count"] = float(surgery_hit.confidence) if surgery_hit else 0.0
        evidence["surgery_count"] = self._hit_evidence(surgery_hit, sheet_map) if surgery_hit else None

        # Associate costs confidence
        field_confidence["associate_cost_amount"] = assoc_conf.get("associate_cost_amount", 0.0)
        field_confidence["associate_cost_pct"] = assoc_conf.get("associate_cost_pct", 0.0)
        evidence["associates"] = assoc_evidence

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
                "address_text": p_addr,
                "postcode": postcode,
                "county": county,
            },
            "materialized": {
                "practice_key": practice_key,
                "surgery_count": surgery_count,
                "associate_cost_amount": str(associates_amount) if associates_amount is not None else None,
                "associate_cost_pct": str(associates_pct) if associates_pct is not None else None,
                "accounts_period_end": accounts_period_end.isoformat() if accounts_period_end else None,
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
            address_text=p_addr,
            postcode=postcode,
            county=county,
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

