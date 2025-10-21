import json
from pathlib import Path

from pipeline.engine import run_pipeline


def test_run_pipeline_validate_creates_output(tmp_path):
    data_file = tmp_path / "sample.txt"
    data_file.write_text("1.2.3.4\nexample.com\n")

    sources = [
        {
            "name": "local/test",
            "type": "file",
            "location": str(data_file),
        }
    ]

    output_path = tmp_path / "output.json"

    run_pipeline(
        sources=sources,
        output=str(output_path),
        mode="validate",
        timeout=5,
        log_level="DEBUG",
        persist_to_db=False,
    )

    assert output_path.exists()
    data = json.loads(output_path.read_text())
    assert len(data) >= 1
    assert all("normalized" in entry for entry in data)
