# DDV

## Semi-structured Spreadsheet Ingestion MVP

This repo is an MVP platform to ingest many semi-structured spreadsheets (starting with `.xlsx`), extract consistent logical fields even with slight layout differences, normalize into a master schema, store into Postgres, and flag low-confidence/failed extractions for review.

### Stack
- **Backend**: Python + FastAPI
- **Parsing**: Pandas + OpenPyXL
- **DB**: PostgreSQL
- **ORM**: SQLModel (SQLAlchemy-based)

### Project layout (MVP)
- `backend/`: FastAPI app, extraction + validation pipeline
- `docker-compose.yml`: Postgres + API

### Quickstart (Docker)
1. Copy env file:
   - `cp backend/.env.example backend/.env` (PowerShell: `Copy-Item backend\.env.example backend\.env`)
2. Start services:
   - `docker compose up --build`
3. Open API docs:
   - `http://localhost:8000/docs`

### MVP flow
1. Upload `.xlsx` to `POST /api/files/upload`
2. Trigger processing for a file:
   - `POST /api/files/{file_id}/process`
3. Query extracted records:
   - `GET /api/records?file_id=...`

### Notes / TODO
- Add Alembic migrations once schema stabilizes.
- Add template detection + template-specific extractors.
- Add review UI for issues and record edits.
- Add background queue (RQ/Celery) for scale beyond in-process tasks.

