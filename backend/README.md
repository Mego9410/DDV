## Backend (FastAPI) - Local dev

### Requirements
- Python 3.11+ (3.12 recommended)
- Postgres 14+ (or run via Docker on a machine that has it)

### Setup (PowerShell)
From `DDV\backend`:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
```

Set `DATABASE_URL` in `.env` to point at your Postgres.

### Run
```powershell
uvicorn app.main:app --reload --port 8000
```

Open docs at `http://localhost:8000/docs`.

### Import spreadsheets into Supabase (latest-only)
This project already extracts a single "latest-only" row per workbook into `public.practices`.
To import a folder of `.xlsx` files directly into your Supabase Postgres:

- Set `DATABASE_URL` in `backend/.env` to your Supabase connection string (use the **transaction pooler** URI if you have it).
- Ensure the schema in `backend/sql/supabase_latest_only_schema.sql` (or `supabase/migrations/...latest_only_schema.sql`) has been applied to your Supabase DB.
- Run:

```powershell
python scripts/import_spreadsheets_to_supabase.py --input-dir "C:\path\to\xlsx" --recursive
```

This writes a report to `out/import_report.json` and prints a summary `{updated, skipped, errors}`.

