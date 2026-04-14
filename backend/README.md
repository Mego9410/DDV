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

