import asyncio
from pathlib import Path

from models.ingestion_model import EDLIngestionService
from models.schemas import FetchedEntry
from pipeline.engine import Orchestrator


def test_ingestion_preserves_metadata():
    ingestor = EDLIngestionService(log_level="ERROR")
    fetched = [
        FetchedEntry(
            source="test",
            raw="example.com # inline comment",
            line_number=1,
            metadata={"source_type": "file"},
        )
    ]

    results = ingestor.ingest(fetched)
    assert len(results) == 1
    ingested = results[0]
    assert ingested.entry == "example.com"
    assert ingested.metadata.get("comment") == "inline comment"
    assert ingested.metadata.get("raw") == "example.com # inline comment"
    assert ingested.line_number == 1


def test_orchestrator_validate_with_local_file():
    sample_path = Path("test_data") / "sample_data"
    assert sample_path.exists(), "Expected bundled sample data for integration test"

    orchestrator = Orchestrator(
        sources=[
            {"name": "local_sample", "type": "file", "location": str(sample_path)},
        ],
        mode="validate",
        augmentor_cfg=None,
        timeout=5,
        log_level="ERROR",
    )

    results = asyncio.run(orchestrator.run())

    assert results, "Pipeline should produce validation results"
    assert any(entry.valid for entry in results), "Expected some valid entries"
    assert any(not entry.valid for entry in results), "Expected some invalid entries to surface"
