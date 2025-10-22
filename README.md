# EDL Pipeline

Enterprise-ready ingestion, validation, augmentation, and hosting for external deny lists (EDLs).

---

## Prerequisites

- Python 3.11 or newer
- `pip` (ships with Python)
- Git (to clone the repository)

All commands below assume you are in the project root after cloning.

---

## 1. Clone the repository and install dependencies

```powershell
git clone https://github.com/<your-org>/<your-repo>.git
cd <your-repo>

python -m venv .venv
.\.venv\Scripts\activate      # PowerShell (Windows)
# source .venv/bin/activate     # bash/zsh (Linux/macOS)

pip install -r requirements.txt
```

> Tip: the project keeps `__pycache__/`, `logs/`, and database files out of Git; you do not need to commit them.

---

## 2. Initialize the database schema

Create (or update) the SQLite schema in `edl_pipeline.db`:

```powershell
python -c "from db.session import ensure_schema; ensure_schema()"
```

`ensure_schema()` is idempotent—run it any time new tables/columns are added.

---

## 3. Provide configuration files

The pipeline expects YAML configuration describing sources and augmentation behaviour. Copy your files into `config/` (examples:

- `config/sources.yaml`
- `config/augmentor_config.yaml`

Alternatively, you can upload YAML bundles later via the API (see step 6b) and the configs will be persisted as profiles.

---

## 4. Set environment variables

At minimum set an API key before starting the service:

```powershell
setx EDL_API_KEY "super-secret-key"
```

Open a new shell (or use `set` inside the current session) so the variable is available to Uvicorn.

Optional:

- `EDL_AUTO_REFRESH_MINUTES` – interval (in minutes) the scheduler wakes up to evaluate profile refreshes.
- `DATABASE_URL` – override the default SQLite path if you want to use another database engine.

---

## 5. Start the FastAPI service

```powershell
python -m uvicorn api.main:app --host 0.0.0.0 --port 8000
```

Open http://localhost:8000/docs for interactive Swagger / Redoc documentation.

---

## 6. Trigger pipelines

### a. JSON payload (validate mode example)

```bash
curl -X POST http://localhost:8000/runs \
  -H "Content-Type: application/json" \
  -H "X-API-Key: super-secret-key" \
  -d '{
        "sources_path": "config/sources.yaml",
        "mode": "validate",
        "persist_to_db": true,
        "output_path": "test_output_data/api_output.json",
        "timeout": 15,
        "log_level": "INFO"
      }'
```

The response contains a `job_id` (for polling) and an optional `profile_id` if a stored configuration is used.

### b. Upload YAML bundles (creates a profile)

This call stores the uploaded files as a configuration profile and queues a run. It also sets a 30-minute refresh interval for the profile.

```bash
curl -X POST http://localhost:8000/runs/upload \
  -H "X-API-Key: super-secret-key" \
  -F "mode=augment" \
  -F "persist_to_db=true" \
  -F "refresh_interval_minutes=30" \
  -F "sources_file=@config/sources.yaml" \
  -F "augmentor_file=@config/augmentor_config.yaml"
```

### c. CLI usage (mirrors FastAPI)

```bash
python pipeline/cli.py --sources config/sources.yaml --mode validate --persist-db
```

Add `--config config/augmentor_config.yaml` when `--mode augment` is used.

---

## 7. Monitor jobs and profiles

- All jobs (queued, running, or completed):

  ```bash
  curl -H "X-API-Key: super-secret-key" http://localhost:8000/jobs
  ```

- Stored configuration profiles (with next scheduled run):

  ```bash
  curl -H "X-API-Key: super-secret-key" http://localhost:8000/profiles
  ```

---

## 8. Hosted EDL endpoints

Each endpoint returns plain text (one indicator per line) based on the most recent run (or configured profile pointer):

- `GET /edl/url`
- `GET /edl/fqdn`
- `GET /edl/ipv4`
- `GET /edl/ipv6`
- `GET /edl/cidr`

Responses include the source `run_id` in the `X-EDL-Run-ID` header.

---

## 9. Automated refresh

- Profiles uploaded with `refresh_interval_minutes` (or updated in the database) will be re-run automatically when the scheduler wakes up.
- Set `EDL_AUTO_REFRESH_MINUTES` to control how frequently the scheduler checks profiles (e.g. `setx EDL_AUTO_REFRESH_MINUTES 5`).
- The scheduler skips profiles that already have an active job or no interval configured.

---

## 10. Running the test suite

Activate your virtual environment and run:

```bash
pytest
```

This exercises the ingestor, engine, augmentor, CLI, and integration paths.

---

## Notes

- The SQLite database (`edl_pipeline.db`) lives at the project root by default. Backup or relocate it if you deploy in production.
- Generated folders (`__pycache__/`, `logs/`, `config/uploads/`) are ignored by Git—no need to commit them.
- Feel free to script these steps if you frequently set up new environments.
