from __future__ import annotations

import json
from typing import Any

from openai import OpenAI

from app.nlq.intent_schema import QueryIntent


def generate_intent(*, question: str, allow_group_by: bool = False) -> QueryIntent:
    """
    Use an LLM to translate natural language -> constrained QueryIntent JSON.
    Guardrails:
    - we validate the produced JSON with Pydantic (allowlist fields/ops/metrics)
    - if invalid, we raise ValueError and the caller should log as blocked/error
    """
    from app.core.config import get_settings

    settings = get_settings()
    if not settings.openai_api_key:
        raise RuntimeError(
            "Missing OpenAI configuration. Set OPENAI_API_KEY on the backend environment (server-side)."
        )

    client = OpenAI(api_key=settings.openai_api_key)

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

