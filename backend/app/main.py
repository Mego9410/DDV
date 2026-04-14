from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlmodel import SQLModel

from app.api.router import api_router
from app.core.config import get_settings
from app.db.session import get_engine


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(title="Spreadsheet Ingestion MVP", version="0.1.0")

    origins = [o.strip() for o in (settings.cors_origins or "*").split(",") if o.strip()]
    allow_all = "*" in origins
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"] if allow_all else origins,
        allow_credentials=not allow_all,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.on_event("startup")
    def _startup() -> None:
        engine = get_engine()
        # MVP: create tables automatically. Replace with Alembic migrations later.
        #
        # For local UI prototyping, it's convenient to run with SQLite. Some models
        # use Postgres-only column types (JSONB/ARRAY), so skip auto-create on SQLite.
        if engine.dialect.name != "sqlite":
            SQLModel.metadata.create_all(engine)

        # Ensure storage folders exist
        from app.services.storage_service import StorageService

        StorageService().ensure_dirs()

    app.include_router(api_router, prefix="/api")

    @app.get("/health")
    def health():
        return {"ok": True, "env": settings.app_env}

    return app


app = create_app()

