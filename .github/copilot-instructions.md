# Copilot Instructions for AI Coding Agents

## Project Overview
This repository implements an enterprise-grade EDL (Entity Data Lifecycle) pipeline for data ingestion, validation, and augmentation. The architecture is modular, with clear separation between configuration, data models, pipeline logic, and utilities.

## Key Components
- `config/`: YAML-based configuration for augmentors and data sources.
- `db/`: SQLAlchemy models and session management for database interactions.
- `models/`: ML/validation models and Pydantic schemas for data validation and augmentation.
- `pipeline/`: Core pipeline logic, including:
  - `engine.py`/`engine_1.py`: Main orchestration engines.
  - `augmenters.py`, `validators.py`, `fetcher.py`: Pluggable pipeline stages.
  - `cli.py`: Command-line interface for running pipeline tasks.
  - `rules.py`: Custom business logic and validation rules.
- `utils/`: Shared helpers for config loading and logging.

## Data Flow
1. Data is ingested and fetched via `fetcher.py`.
2. Augmentation and validation are performed using models in `models/` and logic in `augmenters.py`/`validators.py`.
3. Results are logged and output to `test_output_data/`.

## Developer Workflows
- **Run pipeline:** Use the CLI in `pipeline/cli.py` (see docstrings for usage).
- **Configuration:** Edit YAML files in `config/` to change sources or augmentor settings.
- **Database:** Models and sessions are managed in `db/` using SQLAlchemy.
- **Logging:** Output is written to `logs/` and `pipeline/logs/`.
- **Testing:** No formal test suite detected; validate changes by running the pipeline and inspecting outputs in `test_output_data/`.

## Project Conventions
- All configuration is YAML-based and loaded via `utils/config_loader.py`.
- Logging is centralized via `utils/logger.py` and `pipeline/validators_logging.py`.
- Augmentation and validation logic is modular—add new augmenters/validators as separate classes or functions.
- Output data is written as JSON to `test_output_data/`.

## Integration Points
- External data sources are defined in `config/sources.yaml`.
- Augmentor settings are in `config/augmentor_config.yaml`.
- Database schema is defined in `db/models.py`.

## Examples
- To add a new validation rule, implement it in `pipeline/rules.py` and register it in `validators.py`.
- To change data sources, update `config/sources.yaml` and reload via the pipeline CLI.

---
For more details, see the `README.md` and docstrings in each module. If conventions or workflows are unclear, ask for clarification or check with maintainers.
