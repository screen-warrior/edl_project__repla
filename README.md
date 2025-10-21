# EDL Pipeline

Enterprise-ready EDL ingestion, validation, and augmentation pipeline.

## Running the FastAPI service

A FastAPI application mirroring the CLI capabilities is available at `api/main.py`.

1. Install the optional API dependencies:

   ```bash
   pip install fastapi uvicorn
   ```

2. Ensure the database schema is up-to-date (run once after pulling changes):

   ```powershell
   python -c "from db.session import init_db; init_db()"
   ```

3. Export an API key (required for any write endpoints). Choose a suitably long random value:

   ```powershell
   setx EDL_API_KEY "super-secret-key"
   ```

   ```bash
   # bash/zsh
   export EDL_API_KEY="super-secret-key"
   ```

4. Start the service:

   ```bash
   uvicorn api.main:app --host 0.0.0.0 --port 8000
   ```

5. Submit a pipeline run (mirrors the CLI flags). Include the shared API key header:

   ```bash
   curl -X POST http://localhost:8000/runs \
     -H "Content-Type: application/json" \
     -H "X-API-Key: super-secret-key" \
     -d '{
           "sources_path": "config/sources.yaml",
           "mode": "validate",
           "persist_to_db": true,
          "output_path": "test_output_data/api_output.json"
        }'
   ```

6. Poll job status:

   ```bash
   curl http://localhost:8000/jobs/<job_id>
   ```

### Uploading configuration bundles

The API also accepts YAML configuration bundles (sources, augmentor, and optional firewall metadata):

```bash
curl -X POST http://localhost:8000/runs/upload \
  -H "X-API-Key: super-secret-key" \
  -F "sources_file=@config/sources.yaml" \
  -F "augmentor_file=@config/augmentor_config.yaml" \
  -F "mode=validate" \
  -F "persist_to_db=true"
```

Uploaded files are stored under `config/uploads/<job_id>/` for later review.

### Hosted EDL endpoints

The API exposes segregated indicator feeds derived from the most recent pipeline run:

- `GET /edl/url`
- `GET /edl/fqdn`
- `GET /edl/ipv4`
- `GET /edl/ipv6`

Each endpoint responds with `text/plain` (one indicator per line) and includes the originating `run_id` in the `X-EDL-Run-ID` header.

### Automated refresh scaffolding

Set `EDL_AUTO_REFRESH_MINUTES=<float>` before launching the service to enable a placeholder background scheduler. The scheduler currently logs each tick; full re-fetch automation will be introduced once firewall profiles are defined.

Interactive documentation is available at `http://localhost:8000/docs` once the server is running.
