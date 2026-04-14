from __future__ import annotations

from fastapi import APIRouter

from app.api.routes import access, chat
from app.api.routes import calc
from app.api.routes import files, issues, records

api_router = APIRouter()
api_router.include_router(files.router, prefix="/files", tags=["files"])
api_router.include_router(records.router, prefix="/records", tags=["records"])
api_router.include_router(issues.router, prefix="/issues", tags=["issues"])
api_router.include_router(calc.router, prefix="/calc", tags=["calc"])
api_router.include_router(access.router, prefix="/access", tags=["access"])
api_router.include_router(chat.router, prefix="/chat", tags=["chat"])

