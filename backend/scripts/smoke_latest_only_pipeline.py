from __future__ import annotations

import argparse
import json
from pathlib import Path

from sqlmodel import Session, select
from sqlalchemy import func

from app.db.session import get_engine
from app.models.practice import Practice


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--paths-json", required=True, help="JSON array of xlsx paths")
    ap.add_argument("--n", type=int, default=5, help="How many files to process in smoke run")
    ap.add_argument("--out-dir", default="out/practices_smoke", help="Where extract output should be written")
    ap.add_argument("--dry-run-ingest", action="store_true", help="Skip DB writes (extract only)")
    args = ap.parse_args()

    paths = json.loads(Path(args.paths_json).read_text(encoding="utf-8"))
    if not isinstance(paths, list) or not all(isinstance(x, str) for x in paths):
        raise SystemExit("--paths-json must be a JSON array of file path strings")

    subset_path = Path("out") / "smoke_paths.json"
    subset_path.parent.mkdir(parents=True, exist_ok=True)
    subset_path.write_text(json.dumps(paths[: args.n], indent=2), encoding="utf-8")

    # 1) extract to per-practice JSON
    print("Run extraction via:")
    print(f"  python scripts/extract_practice_latest_batch.py --paths-json {subset_path} --out-dir {args.out_dir}")

    # 2) ingest
    print("Then ingest via:")
    print(f"  python scripts/ingest_practice_latest_from_out.py --in-dir {args.out_dir}")

    if args.dry_run_ingest:
        return

    # 3) run a canonical query directly (after ingest)
    engine = get_engine()
    with Session(engine) as session:
        q = (
            select(func.avg(Practice.associate_cost_amount))
            .where(Practice.county == "Kent")
            .where(Practice.surgery_count == 3)
            .where(Practice.associate_cost_amount.is_not(None))
        )
        avg_val = session.exec(q).one()
        print({"avg_associate_cost_amount_kent_3_surgery": float(avg_val) if avg_val is not None else None})


if __name__ == "__main__":
    main()

