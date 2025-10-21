import asyncio
import json
from pathlib import Path

from pipeline.ingestor import EDLIngestor


def test_ingestor_run_with_file(tmp_path):
    # Arrange
    data_file = tmp_path / "sample.txt"
    data_file.write_text("1.2.3.4\nexample.com\n\n")

    sources = [
        {
            "name": "local/test",
            "type": "file",
            "location": str(data_file),
        }
    ]

    ingestor = EDLIngestor(sources=sources, timeout=5, log_level="DEBUG")

    # Act
    entries = asyncio.run(ingestor.run())

    # Assert
    assert len(entries) == 3
    assert entries[0].raw == "1.2.3.4"
    assert entries[0].metadata["source_type"] == "file"
    assert entries[0].metadata["path"] == str(data_file)
