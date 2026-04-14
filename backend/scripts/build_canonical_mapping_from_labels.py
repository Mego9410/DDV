from __future__ import annotations

import json
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from rapidfuzz import fuzz

from app.services.calc_sheet_selector import CalcSheetSelector


def _norm_label(s: str) -> str:
    s = s.lower()
    s = s.replace("&", "and")
    s = re.sub(r"[^a-z0-9% ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _is_plausible_metric_label(s: str) -> bool:
    if not s or len(s) < 2:
        return False
    ns = _norm_label(s)
    if not ns:
        return False
    if ns in {"value", "values", "percent", "income type"}:
        return False
    if re.fullmatch(r"[-+]?\d+(\.\d+)?", ns):
        return False
    return True


@dataclass
class LabelExample:
    raw: str
    norm: str


def _extract_label_candidates_from_pairs(
    df: pd.DataFrame, *, max_rows: int = 250, max_cols: int = 40
) -> list[LabelExample]:
    df = df.iloc[:max_rows, :max_cols]
    out: list[LabelExample] = []
    rows, cols = df.shape
    for r in range(rows):
        for c in range(cols):
            cell = df.iat[r, c]
            if not isinstance(cell, str):
                continue
            label = cell.strip()
            if not _is_plausible_metric_label(label):
                continue

            # Only accept if there is a plausible nearby value cell (right / two-right).
            v1 = df.iat[r, c + 1] if c + 1 < cols else None
            v2 = df.iat[r, c + 2] if c + 2 < cols else None
            if not (_is_valueish(v1) or _is_valueish(v2)):
                continue

            out.append(LabelExample(raw=label, norm=_norm_label(label)))
    return out


def _is_valueish(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, float) and pd.isna(v):
        return False
    if isinstance(v, (int, float)):
        return True
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return False
        # must contain a digit or be short text value
        return bool(re.search(r"\d", s)) or len(s) <= 40
    return False


def _cluster_labels(labels: list[str], *, threshold: int = 90) -> list[list[str]]:
    """
    Greedy clustering by fuzzy partial ratio on normalized labels.
    Good enough for MVP mapping; can be improved later with embeddings.
    """
    clusters: list[list[str]] = []
    reps: list[str] = []
    for l in labels:
        placed = False
        for i, rep in enumerate(reps):
            if fuzz.partial_ratio(l, rep) >= threshold:
                clusters[i].append(l)
                placed = True
                break
        if not placed:
            clusters.append([l])
            reps.append(l)
    return clusters


def main() -> None:
    import argparse

    ap = argparse.ArgumentParser(description="Build a canonical mapping proposal from Calc sheet labels.")
    ap.add_argument("files", nargs="+", help="Paths to xlsx files")
    ap.add_argument("--out", default="canonical_mapping.json")
    ap.add_argument("--threshold", type=int, default=90, help="Fuzzy cluster threshold (0-100)")
    ap.add_argument("--min_files", type=int, default=5, help="Only keep clusters present in at least N files")
    args = ap.parse_args()

    selector = CalcSheetSelector()
    # Collect normalized labels per file
    labels_by_file: dict[str, set[str]] = {}

    for f in args.files:
        p = Path(f)
        selected = selector.select(p)
        if selected is None:
            continue
        xl = pd.ExcelFile(p)
        df = xl.parse(sheet_name=selected.sheet_name, header=None, dtype=object)
        cands = _extract_label_candidates_from_pairs(df)
        labels_by_file[str(p)] = set(c.norm for c in cands)

    all_labels = sorted({l for s in labels_by_file.values() for l in s})
    clusters = _cluster_labels(all_labels, threshold=args.threshold)

    # Compute file coverage per cluster
    cluster_summaries: list[dict[str, Any]] = []
    for cl in clusters:
        present_files = 0
        for labels in labels_by_file.values():
            if any(x in labels for x in cl):
                present_files += 1
        if present_files < args.min_files:
            continue
        cluster_summaries.append(
            {
                "representative": sorted(cl, key=len)[0],
                "variants": sorted(cl),
                "files_present": present_files,
            }
        )

    # Sort by coverage, then size
    cluster_summaries.sort(key=lambda d: (d["files_present"], len(d["variants"])), reverse=True)

    # Output a proposal file. Human can edit to assign canonical keys.
    proposal = {
        "canonical_specs": {
            # Start with empty; user (or next step) can fill canonical keys from clusters.
        },
        "clusters": cluster_summaries,
        "stats": {
            "files_scanned": len(labels_by_file),
            "unique_labels": len(all_labels),
            "threshold": args.threshold,
            "min_files": args.min_files,
        },
    }

    Path(args.out).write_text(json.dumps(proposal, indent=2, ensure_ascii=False), encoding="utf-8")
    print(args.out)


if __name__ == "__main__":
    main()

