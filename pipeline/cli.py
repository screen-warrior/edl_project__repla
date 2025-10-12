# cli.py
import argparse
from utils.logger import get_logger, log_stage
from pipeline.engine_1 import EngineConfig, run_pipeline

def main():
    parser = argparse.ArgumentParser(description="Fetch + Validate EDL entries (with optional augmentation).")
    parser.add_argument("--sources", required=True, help="Path to sources.yaml")
    parser.add_argument("--mode", choices=["validate", "augment"], default="validate")
    parser.add_argument("--config", help="Path to augmentor_config.yaml (if mode=augment)")
    parser.add_argument("--output", default="test_output_data/validated_output.json")
    parser.add_argument("--timeout", type=int, default=15)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logger = get_logger("cli", args.log_level, "cli.log")

    # Load configs
    sources = EngineConfig.load_sources(args.sources, logger)
    augmentor_cfg = EngineConfig.load_augmentor(args.config, logger) if args.mode == "augment" else {}

    with log_stage(logger, "pipeline_total"):
        run_pipeline(
            sources=sources,
            output=args.output,
            mode=args.mode,
            augmentor_cfg=augmentor_cfg,
            timeout=args.timeout,
            log_level=args.log_level,
        )

if __name__ == "__main__":
    main()
