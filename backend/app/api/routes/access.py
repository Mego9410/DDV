from __future__ import annotations

from typing import Any

import httpx
from fastapi import APIRouter, HTTPException
from passlib.hash import bcrypt
from pydantic import BaseModel

from app.core.config import get_settings
from app.utils.access_token import mint_access_token

router = APIRouter()


class VerifyPasswordIn(BaseModel):
    password: str


class VerifyPasswordOut(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int


async def _fetch_shared_password_hash() -> str:
    settings = get_settings()
    if not settings.supabase_url or not settings.supabase_service_role_key:
        return ""

    url = settings.supabase_url.rstrip("/")
    table = settings.supabase_secret_table
    key = settings.supabase_shared_password_key

    endpoint = f"{url}/rest/v1/{table}"
    headers = {
        "apikey": settings.supabase_service_role_key,
        "Authorization": f"Bearer {settings.supabase_service_role_key}",
    }
    params = {"select": "value", "key": f"eq.{key}", "limit": "1"}

    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(endpoint, headers=headers, params=params)
        if resp.status_code != 200:
            raise HTTPException(
                status_code=500,
                detail=f"Supabase secret fetch failed ({resp.status_code}): {resp.text}",
            )
        data: Any = resp.json()
        if not isinstance(data, list) or not data or "value" not in data[0]:
            raise HTTPException(status_code=500, detail="Shared password hash not found in Supabase.")
        return str(data[0]["value"])


@router.post("/verify", response_model=VerifyPasswordOut)
async def verify_password(body: VerifyPasswordIn) -> VerifyPasswordOut:
    settings = get_settings()
    stored_hash = await _fetch_shared_password_hash()

    # If Supabase isn't configured yet, use a plaintext shared password (dev fallback).
    if not stored_hash:
        ok = body.password == settings.shared_password_plain
    else:
        ok = False
        try:
            ok = bcrypt.verify(body.password, stored_hash)
        except Exception:
            ok = False

    if not ok:
        raise HTTPException(status_code=401, detail="Invalid password")

    token = mint_access_token(secret=settings.access_token_secret, ttl_seconds=settings.access_token_ttl_seconds)
    return VerifyPasswordOut(
        access_token=token,
        expires_in=settings.access_token_ttl_seconds,
    )

