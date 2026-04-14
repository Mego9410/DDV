from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import pandas as pd

try:
    from rapidfuzz import fuzz
except Exception:  # pragma: no cover
    fuzz = None  # type: ignore[assignment]

from app.core.config import get_settings
from app.extractors.field_spec import FieldSpec


@dataclass(frozen=True)
class ExtractedField:
    field_name: str
    raw_value: Any
    confidence: float
    label_found: Optional[str]
    position: Optional[tuple[int, int]]  # (row, col) for label


class LabelSearchExtractor:
    """
    MVP heuristic extractor:
    - scan for known label text (exact or fuzzy)
    - once found, pick a nearby cell as value (right cell preferred, else below)
    - return confidence combining label match quality + value presence
    """

    def __init__(self) -> None:
        self._settings = get_settings()

    def extract_fields(self, df: pd.DataFrame, specs: list[FieldSpec]) -> list[ExtractedField]:
        text_grid = df.applymap(self._cell_to_text)
        results: list[ExtractedField] = []
        for spec in specs:
            results.append(self._extract_one(text_grid=text_grid, raw_df=df, spec=spec))
        return results

    def _extract_one(self, *, text_grid: pd.DataFrame, raw_df: pd.DataFrame, spec: FieldSpec) -> ExtractedField:
        best = None  # (confidence, label_variant, (r,c), match_score)
        for r in range(text_grid.shape[0]):
            for c in range(text_grid.shape[1]):
                cell = text_grid.iat[r, c]
                if not cell:
                    continue
                for label in spec.labels:
                    score = self._label_match_score(cell, label)
                    if score <= 0:
                        continue
                    confidence = score / 100.0
                    if best is None or confidence > best[0]:
                        best = (confidence, label, (r, c), score)

        if best is None:
            return ExtractedField(
                field_name=spec.field_name,
                raw_value=None,
                confidence=0.0,
                label_found=None,
                position=None,
            )

        base_conf, label, (r, c), _score = best
        raw_value = self._pick_nearby_value(raw_df=raw_df, r=r, c=c)
        value_present = 1.0 if self._is_present(raw_value) else 0.0
        # weight label match more than value presence
        confidence = (0.75 * base_conf) + (0.25 * value_present)

        return ExtractedField(
            field_name=spec.field_name,
            raw_value=raw_value,
            confidence=float(max(0.0, min(1.0, confidence))),
            label_found=label,
            position=(r, c),
        )

    def _pick_nearby_value(self, *, raw_df: pd.DataFrame, r: int, c: int) -> Any:
        # Prefer right cell; handle "Label: Value" in same cell as fallback.
        same = raw_df.iat[r, c]
        if isinstance(same, str) and ":" in same:
            tail = same.split(":", 1)[1].strip()
            if tail:
                return tail
        if c + 1 < raw_df.shape[1]:
            v = raw_df.iat[r, c + 1]
            if self._is_present(v):
                return v
        if r + 1 < raw_df.shape[0]:
            v = raw_df.iat[r + 1, c]
            if self._is_present(v):
                return v
        # try down-right
        if r + 1 < raw_df.shape[0] and c + 1 < raw_df.shape[1]:
            v = raw_df.iat[r + 1, c + 1]
            if self._is_present(v):
                return v
        return None

    def _label_match_score(self, cell_text: str, label: str) -> int:
        ct = cell_text.strip().lower()
        lb = label.strip().lower()
        if not ct or not lb:
            return 0

        # exact / containment boosts
        if ct == lb:
            return 100
        if lb in ct:
            return 95

        if not self._settings.fuzzy_match_enabled:
            return 0
        if fuzz is None:
            return 0
        score = int(fuzz.partial_ratio(ct, lb))
        return score if score >= self._settings.fuzzy_match_threshold else 0

    def _cell_to_text(self, v: Any) -> str:
        if v is None:
            return ""
        if isinstance(v, float) and pd.isna(v):
            return ""
        if isinstance(v, str):
            return v
        return str(v)

    def _is_present(self, v: Any) -> bool:
        if v is None:
            return False
        if isinstance(v, float) and pd.isna(v):
            return False
        if isinstance(v, str) and v.strip() == "":
            return False
        return True

