from __future__ import annotations

from collections.abc import Generator

from sqlalchemy import Engine
from sqlalchemy import create_engine
from sqlmodel import Session

from app.core.config import get_settings


def get_engine() -> Engine:
    settings = get_settings()
    return create_engine(settings.database_url, pool_pre_ping=True)


def get_session() -> Generator[Session, None, None]:
    engine = get_engine()
    with Session(engine) as session:
        yield session

