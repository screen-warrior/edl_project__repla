# cli.py
import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

if __package__ in (None, ""):
    project_root = Path(__file__).resolve().parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

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
    parser.add_argument(
        "--persist-db",
        action="store_true",
        help="Persist pipeline results to the configured database.",
    )
    parser.add_argument(
        "--interval-minutes",
        type=float,
        default=0.0,
        help="If >0, run the pipeline continuously at the specified interval.",
    )
    args = parser.parse_args()

    logger = get_logger("cli", args.log_level, "cli.log")
    logger.info(
        "CLI invocation | mode=%s persist_db=%s output=%s interval_minutes=%.2f",
        args.mode,
        args.persist_db,
        args.output,
        args.interval_minutes,
    )

    # Validate required args for modes
    if args.mode == "augment" and not args.config:
        # argparse's error method exits; provide a helpful message
        parser.error("--config is required when --mode=augment")

    # Load configs
    sources = EngineConfig.load_sources(args.sources, logger)
    augmentor_cfg = {}
    if args.mode == "augment":
        try:
            augmentor_cfg = EngineConfig.load_augmentor(args.config, logger)
        except Exception as e:
            logger.error("Unable to load augmentor config '%s': %s", args.config, e)
            raise

    from inspect import signature

    run_pipeline_sig = signature(run_pipeline)

    def execute_once():
        kwargs = dict(
            sources=sources,
            output=args.output,
            mode=args.mode,
            augmentor_cfg=augmentor_cfg,
            timeout=args.timeout,
            log_level=args.log_level,
        )
        if "persist_to_db" in run_pipeline_sig.parameters:
            kwargs["persist_to_db"] = args.persist_db

        with log_stage(logger, "pipeline_total"):
            run_pipeline(**kwargs)

    interval_seconds = max(0.0, args.interval_minutes) * 60.0

    if interval_seconds <= 0:
        execute_once()
        return

    logger.info(
        "Starting continuous execution every %.1f minutes. Press Ctrl+C to stop.",
        args.interval_minutes,
    )
    iteration = 1
    try:
        while True:
            start_time = time.monotonic()
            logger.info("Pipeline iteration %d started at %s", iteration, datetime.utcnow().isoformat())
            try:
                execute_once()
            except Exception:
                logger.exception("Pipeline iteration %d failed", iteration)

            elapsed = time.monotonic() - start_time
            sleep_for = max(0.0, interval_seconds - elapsed)
            logger.info(
                "Pipeline iteration %d completed in %.2fs; sleeping for %.2fs",
                iteration,
                elapsed,
                sleep_for,
            )
            iteration += 1
            time.sleep(sleep_for)
    except KeyboardInterrupt:
        logger.info("Continuous execution interrupted by user; exiting.")

if __name__ == "__main__":
    main()
