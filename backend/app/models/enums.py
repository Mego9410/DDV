from __future__ import annotations

import enum


class ProcessingStatus(str, enum.Enum):
    uploaded = "uploaded"
    queued = "queued"
    processing = "processing"
    completed = "completed"
    completed_with_warnings = "completed_with_warnings"
    failed = "failed"


class IssueSeverity(str, enum.Enum):
    info = "info"
    warning = "warning"
    error = "error"

