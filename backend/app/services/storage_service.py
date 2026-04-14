from __future__ import annotations

from pathlib import Path
from uuid import UUID

from app.core.config import get_settings


class StorageService:
    def __init__(self) -> None:
        self._settings = get_settings()

    def ensure_dirs(self) -> None:
        self._settings.uploads_dir.mkdir(parents=True, exist_ok=True)

    def build_upload_path(self, file_id: UUID, suffix: str) -> Path:
        self.ensure_dirs()
        return self._settings.uploads_dir / f"{file_id}{suffix}"

