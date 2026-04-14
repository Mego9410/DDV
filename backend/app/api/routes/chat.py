from __future__ import annotations

import time
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel
from sqlmodel import Session

from app.db.session import get_engine
from app.nlq.openai_intent import generate_intent
from app.utils.access_token import verify_access_token

from app.nlq.intent_schema import QueryIntent

router = APIRouter()


class ChatIn(BaseModel):
    message: str


class ChatOut(BaseModel):
    answer: str
    value: float | int | None = None
    intent: dict[str, Any] | None = None
    latency_ms: int


def _require_token(authorization: str | None = Header(default=None)) -> None:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    token = authorization.split(" ", 1)[1].strip()
    from app.core.config import get_settings

    settings = get_settings()
    try:
        verify_access_token(token=token, secret=settings.access_token_secret)
    except Exception as e:
        raise HTTPException(status_code=401, detail=str(e)) from e


def _execute_intent(session: Session, intent: QueryIntent) -> dict[str, Any]:
    # Keep logic colocated with current MVP (derived from scripts/nlq_query.py),
    # but do NOT write any RequestLog to avoid query history.
    from sqlalchemy import func
    from sqlmodel import select

    from app.models.practice import Practice
    from app.nlq.intent_schema import Agg, FilterOp

    def _col_for_metric(metric: str):
        if metric == "associate_cost_amount":
            return Practice.associate_cost_amount
        if metric == "associate_cost_pct":
            return Practice.associate_cost_pct
        raise ValueError(f"Unknown metric: {metric}")

    def _col_for_field(field: str):
        if field == "county":
            return Practice.county
        if field == "surgery_count":
            return Practice.surgery_count
        if field == "accounts_period_end":
            return Practice.accounts_period_end
        raise ValueError(f"Unknown field: {field}")

    def _apply_filters(q, intent: QueryIntent):
        for f in intent.filters:
            col = _col_for_field(f.field.value)
            op = f.op.value
            v = f.value
            if op == FilterOp.eq.value:
                q = q.where(col == v)
            elif op == FilterOp.gte.value:
                q = q.where(col >= v)
            elif op == FilterOp.lte.value:
                q = q.where(col <= v)
            elif op == FilterOp.in_.value:
                if not isinstance(v, list):
                    v = [v]
                q = q.where(col.in_(v))
            elif op == FilterOp.between.value:
                if not isinstance(v, list) or len(v) != 2:
                    raise ValueError("between requires value=[low, high]")
                q = q.where(col >= v[0]).where(col <= v[1])
            else:
                raise ValueError(f"Unsupported op: {op}")
        return q

    metric_col = _col_for_metric(intent.metric.value)

    if intent.agg == Agg.count:
        q = select(func.count()).select_from(Practice)
        q = _apply_filters(q, intent)
        val = session.exec(q).one()
        return {"value": int(val)}

    if intent.agg == Agg.avg:
        q = select(func.avg(metric_col)).where(metric_col.is_not(None))
        q = _apply_filters(q, intent)
        val = session.exec(q).one()
        return {"value": float(val) if val is not None else None}

    if intent.agg == Agg.min:
        q = select(func.min(metric_col)).where(metric_col.is_not(None))
        q = _apply_filters(q, intent)
        val = session.exec(q).one()
        return {"value": float(val) if val is not None else None}

    if intent.agg == Agg.max:
        q = select(func.max(metric_col)).where(metric_col.is_not(None))
        q = _apply_filters(q, intent)
        val = session.exec(q).one()
        return {"value": float(val) if val is not None else None}

    if intent.agg == Agg.median:
        median_expr = func.percentile_cont(0.5).within_group(metric_col)
        q = select(median_expr).where(metric_col.is_not(None))
        q = _apply_filters(q, intent)
        val = session.exec(q).one()
        return {"value": float(val) if val is not None else None}

    raise ValueError(f"Unsupported agg: {intent.agg}")


@router.post("/", response_model=ChatOut, dependencies=[Depends(_require_token)])
def chat(body: ChatIn) -> ChatOut:
    t0 = time.time()
    engine = get_engine()
    try:
        intent = generate_intent(question=body.message)
        intent_json = intent.model_dump()
        with Session(engine) as session:
            out = _execute_intent(session, intent)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    value = out.get("value")
    answer = f"Result: {value}" if value is not None else "No results found for that question."
    latency_ms = int((time.time() - t0) * 1000)
    return ChatOut(answer=answer, value=value, intent=intent_json, latency_ms=latency_ms)

