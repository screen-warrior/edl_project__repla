import sys
from pathlib import Path

import pipeline.cli as cli


def test_cli_invokes_run_pipeline(monkeypatch, tmp_path):
    sources_file = tmp_path / "sources.yaml"
    sources_file.write_text("sources:\n  - name: local/test\n    type: file\n    location: {0}\n".format(tmp_path / "input.txt"))

    input_file = tmp_path / "input.txt"
    input_file.write_text("1.2.3.4\n")

    output_path = tmp_path / "out.json"

    captured = {}

    def fake_run_pipeline(**kwargs):
        captured["kwargs"] = kwargs

    monkeypatch.setattr(cli, "run_pipeline", fake_run_pipeline)

    monkeypatch.setattr(sys, "argv", ["cli.py", "--sources", str(sources_file), "--output", str(output_path)])

    cli.main()

    assert "kwargs" in captured
    assert captured["kwargs"]["output"] == str(output_path)
    assert captured["kwargs"]["mode"] == "validate"
