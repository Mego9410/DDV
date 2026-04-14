from __future__ import annotations

import argparse
import time
from typing import Any

from sqlalchemy import func
from sqlmodel import Session, select

from app.db.session import get_engine
from app.models.logs import RequestLog
from app.models.practice import Practice
from app.nlq.intent_schema import Agg, FilterOp, QueryIntent
from app.nlq.openai_intent import generate_intent


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


def execute_intent(session: Session, intent: QueryIntent) -> dict[str, Any]:
    metric_col = _col_for_metric(intent.metric.value)

    # Always ignore nulls for the metric being aggregated
    base = select(metric_col).where(metric_col.is_not(None))
    base = _apply_filters(base, intent)

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
        # percentile_cont is available in Postgres
        median_expr = func.percentile_cont(0.5).within_group(metric_col)
        q = select(median_expr).where(metric_col.is_not(None))
        q = _apply_filters(q, intent)
        val = session.exec(q).one()
        return {"value": float(val) if val is not None else None}

    raise ValueError(f"Unsupported agg: {intent.agg}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("question", help="Natural language question")
    ap.add_argument("--dry-run", action="store_true", help="Do not write request_log")
    args = ap.parse_args()

    t0 = time.time()
    engine = get_engine()

    status = "ok"
    warnings: list[str] = []
    error_message: str | None = None
    intent_json: dict[str, Any] | None = None
    out: dict[str, Any] | None = None

    with Session(engine) as session:
        try:
            intent = generate_intent(question=args.question)
            intent_json = intent.model_dump()
            out = execute_intent(session, intent)
            if out.get("value") is None:
                status = "no_results"
        except Exception as e:
            status = "error"
            error_message = str(e)

        latency_ms = int((time.time() - t0) * 1000)

        if not args.dry_run:
            rl = RequestLog(
                request_type="nlq",
                query_text=args.question,
                intent=intent_json,
                sql_template=None,
                params=None,
                status=status,
                row_count=None,
                latency_ms=latency_ms,
                warnings=warnings,
                error_message=error_message,
            )
            session.add(rl)
            session.commit()

    if status == "ok" or status == "no_results":
        print(out)
    else:
        raise SystemExit(error_message or "Unknown error")


if __name__ == "__main__":
    main()

