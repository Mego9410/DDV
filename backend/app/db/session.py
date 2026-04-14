from __future__ import annotations

from collections.abc import Generator

from sqlalchemy import Engine
from sqlalchemy import create_engine
from sqlmodel import Session

from app.core.config import get_settings


def get_engine() -> Engine:
    settings = get_settings()
    url = settings.database_url
    # Prefer psycopg (v3) driver when user supplies a plain postgres URL.
    # Supabase dashboards often show `postgresql://...` which defaults to psycopg2 in SQLAlchemy.
    if isinstance(url, str) and url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+psycopg://", 1)
    return create_engine(url, pool_pre_ping=True)


def get_session() -> Generator[Session, None, None]:
    engine = get_engine()
    with Session(engine) as session:
        yield session

