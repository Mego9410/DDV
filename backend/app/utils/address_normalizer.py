from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional


# Accept full UK postcodes and (common in spreadsheets) partially clipped inward codes
# like "DL3 7H" (missing the final letter due to narrow cell display/formatting).
UK_POSTCODE_RE = re.compile(r"\b([A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{1,2})\b", re.IGNORECASE)


@dataclass(frozen=True)
class NormalizedAddress:
    full_text: str
    postcode: Optional[str]
    city: Optional[str]
    county: Optional[str]
    address_line1: Optional[str]
    address_line2: Optional[str]


def normalize_uk_address(addr: str | None) -> NormalizedAddress | None:
    """
    Best-effort UK-ish address normalizer for analytics:
    - extracts postcode reliably when present
    - splits comma-separated segments into lines/city/county
    This is intentionally heuristic; keep `full_text` as the source of truth.
    """
    if not addr:
        return None
    raw = " ".join(addr.strip().split())
    if not raw:
        return None

    # Drop common non-address tails that sometimes get concatenated into the same cell region.
    # Examples: "VALUATION METHODS ...", "A - GW only based on ..."
    stop_rx = re.compile(r"\b(valuation\s+methods?|calculation\s+methods?|a\s*-\s*gw\b)\b", re.IGNORECASE)
    sm = stop_rx.search(raw)
    if sm is not None:
        raw = raw[: sm.start()].rstrip(" ,.;:-")

    # Postcode
    m = UK_POSTCODE_RE.search(raw.upper())
    postcode = m.group(1) if m else None
    if m is not None:
        # Truncate anything after the postcode. Some sheets have adjacent headers/notes
        # that get concatenated into the same "address" line.
        raw = raw[: m.end()].rstrip(" ,.;:-")

    # Tokenize by commas
    parts = [p.strip() for p in raw.split(",") if p.strip()]

    # Some sheets include "Practice address:" prefix already removed; but may have practice name as first part.
    # We try to pull the last 2-3 segments as location-ish.
    city = None
    county = None
    if len(parts) >= 2:
        # Heuristic: last part often includes postcode; preceding parts may be (town, county)
        last = parts[-1]
        prev = parts[-2]

        if postcode and postcode.replace(" ", "") in last.upper().replace(" ", ""):
            # Prefer: ... , <town>, <county>, <postcode>
            # Many of your examples look like: "<street>, <town>, <county>, <postcode>"
            if len(parts) >= 3:
                p3 = parts[-3]
                if not re.search(r"\d", p3):
                    city = p3
                    county = prev if not re.search(r"\d", prev) else None
                else:
                    city = prev
            else:
                city = prev
        else:
            # If no postcode match, still treat trailing segments as broader location
            city = last
            if len(parts) >= 3 and not re.search(r"\d", prev):
                county = prev

    # Address lines: take leading segments until we reach the city segment
    address_line1 = parts[0] if parts else None
    address_line2 = None
    if len(parts) >= 2:
        address_line2 = parts[1]

    # If city is one of the first segments, avoid duplicating it as an address line
    if city and address_line2 and address_line2.lower() == city.lower():
        address_line2 = None

    return NormalizedAddress(
        full_text=raw,
        postcode=postcode,
        city=city,
        county=county,
        address_line1=address_line1,
        address_line2=address_line2,
    )

