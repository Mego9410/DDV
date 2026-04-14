from __future__ import annotations

import json
from functools import lru_cache
from typing import Any

import httpx
from openai import OpenAI

from app.core.config import get_settings
from app.nlq.intent_schema import QueryIntent


@lru_cache(maxsize=1)
def _cached_supabase_openai_key() -> str | None:
    """
    Best-effort secret fetch from Supabase. Cached to avoid a network call
    on every request.
    """
    settings = get_settings()
    if not settings.supabase_url or not settings.supabase_service_role_key:
        return None

    url = settings.supabase_url.rstrip("/")
    table = settings.supabase_secret_table
    headers = {
        "apikey": settings.supabase_service_role_key,
        "Authorization": f"Bearer {settings.supabase_service_role_key}",
    }

    params = {"select": "value", "key": "eq.openai_api_key", "limit": "1"}
    try:
        resp = httpx.get(f"{url}/rest/v1/{table}", headers=headers, params=params, timeout=10.0)
    except Exception:
        return None
    if not resp.is_success:
        return None
    data = resp.json()
    if not data:
        return None
    value = data[0].get("value")
    if not isinstance(value, str) or not value.strip():
        return None
    return value.strip()


def generate_intent(*, question: str, allow_group_by: bool = False) -> QueryIntent:
    """
    Use an LLM to translate natural language -> constrained QueryIntent JSON.
    Guardrails:
    - we validate the produced JSON with Pydantic (allowlist fields/ops/metrics)
    - if invalid, we raise ValueError and the caller should log as blocked/error
    """
    settings = get_settings()
    api_key = settings.openai_api_key or _cached_supabase_openai_key()
    if not api_key:
        raise RuntimeError(
            "Missing OpenAI configuration. Set OPENAI_API_KEY on the backend environment, "
            "or store it in Supabase (app_secrets key='openai_api_key') and set SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY."
        )

    client = OpenAI(api_key=api_key)

    system = """You translate questions into a strict JSON object for querying a Postgres table named 'practices'.

Return ONLY valid JSON that matches this schema:
{
  "metric": "associate_cost_amount" | "associate_cost_pct",
  "agg": "avg" | "median" | "min" | "max" | "count",
  "filters": [
     {"field": "county" | "surgery_count" | "accounts_period_end", "op": "=" | "in" | ">=" | "<=" | "between", "value": <any>}
  ],
  "group_by": ["county" | "surgery_count" | "accounts_period_end"],
  "limit": <int>
}

Rules:
- Prefer "=" for single-value filters.
- For surgery count, use an integer.
- For county, use title case (e.g. "Kent").
- Use limit <= 200 unless asked for more.
- If asked for an average, use agg="avg" and metric accordingly.
- If the user asks a question you cannot represent with the schema, still return JSON but set agg="count", metric="associate_cost_amount", and add no filters.
"""

    user = f"Question: {question}"

    model = "gpt-4o-mini"
    resp = client.responses.create(
        model=model,
        input=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
    )

    text = resp.output_text
    try:
        data: Any = json.loads(text)
    except Exception as e:
        raise ValueError(f"LLM returned non-JSON: {e}. Text={text[:300]!r}") from e

    intent = QueryIntent.model_validate(data)
    if not allow_group_by:
        intent.group_by = []
    return intent

