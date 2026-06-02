from __future__ import annotations

import argparse
import re
import sys
import time
from pathlib import Path
from typing import Any

import httpx
from sqlmodel import Session
from sqlalchemy import text

# Ensure `backend/` is on sys.path so `import app...` works when invoked as:
#   python scripts/<file>.py
BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from app.db.session import get_engine  # noqa: E402


UK_POSTCODE_RE = re.compile(r"^[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}$", re.IGNORECASE)


def norm_postcode(pc: str) -> str:
    return re.sub(r"\s+", "", pc.strip().upper())


def chunked(xs: list[str], n: int) -> list[list[str]]:
    return [xs[i : i + n] for i in range(0, len(xs), n)]


def main() -> None:
    ap = argparse.ArgumentParser(description="Backfill postcode_geocode + practices.lat/lng from postcodes.io.")
    ap.add_argument("--batch", type=int, default=100, help="postcodes.io batch size (default: 100)")
    ap.add_argument("--max", type=int, default=5000, help="Max postcodes to process this run (default: 5000)")
    ap.add_argument("--timeout", type=float, default=20.0, help="HTTP timeout seconds (default: 20)")
    args = ap.parse_args()

    engine = get_engine()

    with Session(engine) as session:
        rows = session.exec(
            text(
                """
                select distinct regexp_replace(upper(btrim(postcode)), '\\s+', '', 'g') as pc
                from public.practices
                where postcode is not null
                  and btrim(postcode) <> ''
                  and (lat is null or lng is null)
                limit :lim
                """
            ),
            params={"lim": int(args.max)},
        ).all()

    postcodes = [r[0] for r in rows if r and r[0] and UK_POSTCODE_RE.match(r[0])]
    if not postcodes:
        print("No postcodes to backfill.")
        return

    print(f"Backfilling {len(postcodes)} postcodes…")

    client = httpx.Client(timeout=float(args.timeout))
    ok = 0
    missing = 0

    for batch in chunked(postcodes, int(args.batch)):
        resp = client.post("https://api.postcodes.io/postcodes", json={"postcodes": batch})
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()
        results = data.get("result") or []

        # Write rows in one transaction per batch.
        with Session(engine) as session:
            for item in results:
                q = item.get("query")
                r = item.get("result")
                if not q or not isinstance(q, str):
                    continue
                pc = norm_postcode(q)
                if not r:
                    missing += 1
                    continue
                lat = r.get("latitude")
                lng = r.get("longitude")
                if lat is None or lng is None:
                    missing += 1
                    continue

                session.exec(
                    text(
                        """
                        insert into public.postcode_geocode(postcode, lat, lng)
                        values (:pc, :lat, :lng)
                        on conflict (postcode)
                        do update set lat = excluded.lat, lng = excluded.lng, updated_at = now()
                        """
                    ),
                    params={"pc": pc, "lat": float(lat), "lng": float(lng)},
                )

                session.exec(
                    text(
                        """
                        update public.practices
                        set lat = :lat, lng = :lng, updated_at = now()
                        where regexp_replace(upper(btrim(postcode)), '\\s+', '', 'g') = :pc
                          and (lat is null or lng is null)
                        """
                    ),
                    params={"pc": pc, "lat": float(lat), "lng": float(lng)},
                )
                ok += 1

            session.commit()

        # Gentle rate limiting
        time.sleep(0.25)

    print(f"Done. Updated {ok} postcodes. Missing/unmatched: {missing}.")


if __name__ == "__main__":
    main()

