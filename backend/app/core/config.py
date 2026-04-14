from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_env: str = "dev"
    log_level: str = "INFO"

    data_dir: str = "/data"
    database_url: str

    # Shared-password gate (stored in Supabase, verified server-side)
    # Create a table like: app_secrets(key text primary key, value text not null)
    # Store bcrypt hash at key="shared_password_hash"
    supabase_url: str | None = None
    supabase_service_role_key: str | None = None
    supabase_secret_table: str = "app_secrets"
    supabase_shared_password_key: str = "shared_password_hash"

    # Dev fallback when Supabase isn't configured (plaintext).
    # Set this to something else before sharing broadly.
    shared_password_plain: str = "pass"

    # Token issued after password verification
    access_token_secret: str = "dev-change-me"
    access_token_ttl_seconds: int = 60 * 30

    # CORS (comma-separated origins). Default: allow all in dev.
    cors_origins: str = "*"

    # LLM (server-side only)
    openai_api_key: str | None = None

    fuzzy_match_enabled: bool = True
    fuzzy_match_threshold: int = 85
    low_confidence_threshold: float = 0.7

    @property
    def uploads_dir(self) -> Path:
        return Path(self.data_dir) / "uploads"


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]

