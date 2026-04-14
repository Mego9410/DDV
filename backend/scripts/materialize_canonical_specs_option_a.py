from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


def _keyify(s: str) -> str:
    s = s.lower().strip()
    s = s.replace("&", "and")
    s = s.replace("%", "percent")
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = s.replace(" ", "_")
    return s[:60]


def main() -> None:
    import argparse

    ap = argparse.ArgumentParser(description="Convert label clusters into canonical_specs (Option A keys).")
    ap.add_argument("--proposal", required=True, help="canonical_mapping_proposal.v2.json")
    ap.add_argument("--out", required=True, help="Output mapping JSON with canonical_specs filled")
    ap.add_argument("--min_files", type=int, default=8, help="Only include clusters present in at least N files")
    ap.add_argument(
        "--deny",
        default="address,practice,year,average,combined,associate,associates,af,ss,pm,va,nhs",
        help="Comma-separated representative keys to skip (too ambiguous/generic)",
    )
    args = ap.parse_args()

    deny = {d.strip().lower() for d in args.deny.split(",") if d.strip()}
    payload: dict[str, Any] = json.loads(Path(args.proposal).read_text(encoding="utf-8"))
    clusters = payload.get("clusters") or []

    canonical_specs: dict[str, list[str]] = {}
    used_keys: set[str] = set()

    for cl in clusters:
        if not isinstance(cl, dict):
            continue
        rep = str(cl.get("representative") or "").strip()
        files_present = int(cl.get("files_present") or 0)
        variants = cl.get("variants") or []
        if files_present < args.min_files:
            continue
        if not rep:
            continue
        if rep.lower() in deny:
            continue
        if not isinstance(variants, list) or not variants:
            continue

        key = _keyify(rep)
        if not key or key in deny:
            continue

        # Avoid collisions by suffixing
        base = key
        i = 2
        while key in used_keys:
            key = f"{base}_{i}"
            i += 1
        used_keys.add(key)

        # Keep up to 50 variants
        vclean = []
        for v in variants:
            if isinstance(v, str) and v.strip():
                vclean.append(v.strip())
        canonical_specs[key] = vclean[:50]

    out = {
        "canonical_specs": canonical_specs,
        "meta": {
            "source_proposal": str(args.proposal),
            "min_files": args.min_files,
            "deny": sorted(deny),
            "note": "Option A keys generated automatically; you can edit keys/aliases manually.",
        },
    }
    Path(args.out).write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")
    print(args.out)


if __name__ == "__main__":
    main()

