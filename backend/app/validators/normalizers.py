from __future__ import annotations

import re
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from dateutil import parser as date_parser

UK_POSTCODE_STRICT_RE = re.compile(r"^(GIR0AA|[A-Z]{1,2}\d[A-Z\d]?\d[A-Z]{2})$", re.IGNORECASE)


def to_text(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def to_decimal(v: Any) -> Optional[Decimal]:
    if v is None:
        return None
    if isinstance(v, Decimal):
        # Treat NaN as missing
        try:
            if v.is_nan():
                return None
        except Exception:
            pass
        return v
    if isinstance(v, (int, float)):
        if isinstance(v, float):
            try:
                # NaN -> missing
                if v != v:  # noqa: PLR0124
                    return None
            except Exception:
                pass
        d = Decimal(str(v))
        try:
            if d.is_nan():
                return None
        except Exception:
            pass
        return d
    s = str(v).strip()
    if not s:
        return None
    # common cleaning: commas, currency symbols, parentheses negatives
    s = s.replace(",", "")
    negative = False
    if s.startswith("(") and s.endswith(")"):
        negative = True
        s = s[1:-1]
    for ch in ["$", "£", "€"]:
        s = s.replace(ch, "")
    try:
        d = Decimal(s)
        return -d if negative else d
    except InvalidOperation:
        return None


def to_date(v: Any) -> Optional[date]:
    if v is None:
        return None
    if isinstance(v, date):
        return v
    s = str(v).strip()
    if not s:
        return None
    try:
        return date_parser.parse(s, dayfirst=False, fuzzy=True).date()
    except Exception:
        return None


def to_percent_0_1(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)) and 0 <= float(v) <= 1:
        return float(v)
    s = str(v).strip()
    if not s:
        return None
    s = s.replace(" ", "")
    if s.endswith("%"):
        try:
            return float(s[:-1]) / 100.0
        except Exception:
            return None
    try:
        f = float(s)
        # Heuristic: 0..100 probably means percent, >1 means percent points
        if 1 < f <= 100:
            return f / 100.0
        return f
    except Exception:
        return None


def to_percent_0_100(v: Any) -> Optional[float]:
    """
    Normalize a percent value into 0..100 (percent points).
    Accepts values like 0.23, 23, '23%', '0.23'.
    """
    f01 = to_percent_0_1(v)
    if f01 is None:
        return None
    return f01 * 100.0


def normalize_uk_postcode(v: Any) -> Optional[str]:
    """
    Best-effort UK postcode normalization:
    - uppercase
    - strip spaces and non-alphanumerics
    - validate a strict-ish pattern

    Returns a compact form without spaces (e.g. 'SW1A1AA').
    """
    s = to_text(v)
    if not s:
        return None
    s = s.upper()
    s = re.sub(r"[^A-Z0-9]", "", s)
    if not s:
        return None
    if not UK_POSTCODE_STRICT_RE.fullmatch(s):
        return None
    return s


def normalize_practice_name(v: Any) -> Optional[str]:
    """
    Normalize practice name for stable keys:
    - lowercase
    - '&' → 'and'
    - remove punctuation
    - collapse whitespace
    """
    s = to_text(v)
    if not s:
        return None
    s = s.lower().strip()
    s = s.replace("&", " and ")
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s if s else None


def make_practice_key(*, practice_name: Any, postcode: Any) -> Optional[str]:
    """
    Stable key for latest-only upserts.
    Format: <normalized_name>|<NORMALIZED_POSTCODE>
    """
    n = normalize_practice_name(practice_name)
    pc = normalize_uk_postcode(postcode)
    if not n or not pc:
        return None
    return f"{n}|{pc}"

