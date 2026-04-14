from __future__ import annotations

import argparse
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
from hashlib import sha1
from pathlib import Path
from typing import Any

from app.core.config import get_settings
from app.services.practice_latest_extractor import PracticeLatestExtractor


def _safe_slug(s: str, *, max_len: int = 80) -> str:
    s = s.strip().lower()
    s = s.replace("|", "_")
    s = re.sub(r"[^a-z0-9_]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s[:max_len] if s else "practice"


def _file_id(practice_key: str, source_file: str) -> str:
    return sha1(f"{practice_key}|{source_file}".encode("utf-8")).hexdigest()[:12]


def _process_one(path_str: str, *, canonical_mapping_path: str | None, low_conf_threshold: float) -> dict[str, Any]:
    t0 = time.time()
    p = Path(path_str)
    extractor = PracticeLatestExtractor(
        canonical_mapping_path=canonical_mapping_path,
        low_conf_threshold=low_conf_threshold,
    )
    try:
        result = extractor.extract(p)
        payload = result.raw_json
        return {
            "source_file": str(p),
            "practice_key": result.practice_key,
            "accounts_period_end": result.accounts_period_end.isoformat() if result.accounts_period_end else None,
            "status": "ok",
            "elapsed_ms": int((time.time() - t0) * 1000),
            "payload": payload,
        }
    except Exception as e:
        return {
            "source_file": str(p),
            "practice_key": None,
            "accounts_period_end": None,
            "status": "error",
            "error": str(e),
            "elapsed_ms": int((time.time() - t0) * 1000),
        }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--paths-json", required=True, help="Path to a JSON array of xlsx file paths")
    ap.add_argument("--out-dir", default="out/practices", help="Directory for per-practice JSON output")
    ap.add_argument("--manifest", default="out/manifest.json", help="Path for manifest JSON output")
    ap.add_argument("--max-workers", type=int, default=6, help="Max parallel workers")
    ap.add_argument("--canonical-mapping", default=None, help="Optional canonical mapping JSON path")
    args = ap.parse_args()

    settings = get_settings()

    paths = json.loads(Path(args.paths_json).read_text(encoding="utf-8"))
    if not isinstance(paths, list) or not all(isinstance(x, str) for x in paths):
        raise SystemExit("--paths-json must be a JSON array of file path strings")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = Path(args.manifest)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    items: list[dict[str, Any]] = []
    t_all = time.time()

    with ThreadPoolExecutor(max_workers=args.max_workers) as ex:
        futs = [
            ex.submit(
                _process_one,
                p,
                canonical_mapping_path=args.canonical_mapping,
                low_conf_threshold=float(settings.low_confidence_threshold),
            )
            for p in paths
        ]
        for fut in as_completed(futs):
            r = fut.result()
            items.append({k: v for k, v in r.items() if k != "payload"})

            if r.get("status") == "ok":
                payload = r["payload"]
                practice_key = str(r["practice_key"])
                fid = _file_id(practice_key, str(r["source_file"]))
                slug = _safe_slug(practice_key)
                out_path = out_dir / f"{slug}__{fid}.json"
                out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    manifest = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "count": len(items),
        "elapsed_ms": int((time.time() - t_all) * 1000),
        "items": sorted(items, key=lambda x: (x.get("status") != "ok", x.get("source_file", ""))),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    print(str(manifest_path))


if __name__ == "__main__":
    main()

