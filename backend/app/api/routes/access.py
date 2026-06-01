from __future__ import annotations

import os
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException
from passlib.hash import bcrypt
from pydantic import BaseModel

from app.core.config import get_settings
from app.utils.access_token import mint_access_token

router = APIRouter()

# bcrypt hash for plaintext "password" (matches supabase migration)
_PASSWORD_BCRYPT = "$2a$10$Z8uxdQ2GCBD7fn80Mc3OCuWiiWkOPFADCSgho4UFN5xSb60p.r8b6"


def _normalize_plain(value: str | None) -> str | None:
    if value is None:
        return None
    s = value.strip()
    if not s:
        return None
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()
    return s or None


def _plain_candidates(settings) -> list[str]:
    out: list[str] = []
    env_plain = _normalize_plain(os.getenv("SHARED_PASSWORD_PLAIN"))
    if env_plain:
        out.append(env_plain)
    out.append(settings.shared_password_plain)
    # de-dupe while preserving order
    seen: set[str] = set()
    unique: list[str] = []
    for p in out:
        if p not in seen:
            seen.add(p)
            unique.append(p)
    return unique


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


async def _password_is_valid(password: str, settings) -> bool:
    trimmed = password.strip()
    if not trimmed:
        return False

    if any(trimmed == candidate for candidate in _plain_candidates(settings)):
        return True

    hashes: list[str] = [_PASSWORD_BCRYPT]
    if settings.supabase_url and settings.supabase_service_role_key:
        try:
            stored = await _fetch_shared_password_hash()
            if stored:
                hashes.append(stored)
        except HTTPException:
            pass

    for h in hashes:
        try:
            if bcrypt.verify(trimmed, h):
                return True
        except Exception:
            continue
    return False


@router.post("/verify", response_model=VerifyPasswordOut)
async def verify_password(body: VerifyPasswordIn) -> VerifyPasswordOut:
    settings = get_settings()
    ok = await _password_is_valid(body.password, settings)

    if not ok:
        raise HTTPException(status_code=401, detail="Invalid password")

    token = mint_access_token(secret=settings.access_token_secret, ttl_seconds=settings.access_token_ttl_seconds)
    return VerifyPasswordOut(
        access_token=token,
        expires_in=settings.access_token_ttl_seconds,
    )

