# EDL Pipeline

Enterprise-ready EDL ingestion, validation, and augmentation pipeline.

## Running the FastAPI service

A FastAPI application mirroring the CLI capabilities is available at `api/main.py`.

1. Install the optional API dependencies:

   ```bash
   pip install fastapi uvicorn
   ```

2. Start the service:

   ```bash
   uvicorn api.main:app --host 0.0.0.0 --port 8000
   ```

3. Submit a pipeline run:

   ```bash
   curl -X POST http://localhost:8000/runs \
     -H "Content-Type: application/json" \
     -d '{
           "sources_path": "config/sources.yaml",
           "mode": "validate",
           "persist_to_db": true,
           "output_path": "test_output_data/api_output.json"
         }'
   ```

4. Poll job status:

   ```bash
   curl http://localhost:8000/jobs/<job_id>
   ```

Interactive documentation is available at `http://localhost:8000/docs` once the server is running.
