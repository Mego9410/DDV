from __future__ import annotations

from pathlib import Path
from uuid import UUID

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from sqlmodel import Session, select

from app.models import ProcessingStatus, UploadedFile
from app.schemas.files import UploadResponse, UploadedFileRead
from app.services.processing_service import ProcessingService
from app.services.storage_service import StorageService
from app.utils.hashing import sha256_file
from app.db.session import get_session

router = APIRouter()


@router.post("/upload", response_model=UploadResponse)
async def upload_files(
    file: UploadFile = File(...),
    session: Session = Depends(get_session),
) -> UploadResponse:
    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing filename.")

    suffix = Path(file.filename).suffix.lower()
    if suffix not in [".xlsx"]:
        raise HTTPException(status_code=400, detail="Only .xlsx supported in MVP.")

    storage = StorageService()
    db_file = UploadedFile(
        original_filename=file.filename,
        content_type=file.content_type,
        storage_path="",
        status=ProcessingStatus.uploaded,
    )
    session.add(db_file)
    session.commit()
    session.refresh(db_file)

    target_path = storage.build_upload_path(db_file.id, suffix=suffix)
    content = await file.read()
    target_path.write_bytes(content)

    db_file.byte_size = len(content)
    db_file.storage_path = str(target_path)
    db_file.sha256 = sha256_file(target_path)
    session.add(db_file)
    session.commit()
    session.refresh(db_file)

    return UploadResponse(file=_to_read(db_file))


@router.post("/{file_id}/process")
def process_file(
    file_id: UUID,
    session: Session = Depends(get_session),
):
    f = session.exec(select(UploadedFile).where(UploadedFile.id == file_id)).one_or_none()
    if f is None:
        raise HTTPException(status_code=404, detail="File not found.")

    svc = ProcessingService()
    result = svc.process_file(session=session, file_id=file_id)
    return {
        "file_id": str(result.file_id),
        "status": result.status,
        "records_created": result.records_created,
        "issues_created": result.issues_created,
    }


@router.get("/{file_id}", response_model=UploadedFileRead)
def get_file(file_id: UUID, session: Session = Depends(get_session)) -> UploadedFileRead:
    f = session.exec(select(UploadedFile).where(UploadedFile.id == file_id)).one_or_none()
    if f is None:
        raise HTTPException(status_code=404, detail="File not found.")
    return _to_read(f)


def _to_read(f: UploadedFile) -> UploadedFileRead:
    return UploadedFileRead(
        id=f.id,
        original_filename=f.original_filename,
        content_type=f.content_type,
        byte_size=f.byte_size,
        sha256=f.sha256,
        status=f.status,
        status_message=f.status_message,
        uploaded_at=f.uploaded_at,
        processed_at=f.processed_at,
    )

