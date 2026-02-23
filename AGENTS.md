# Repository Guidelines

## Project Structure & Module Organization
Core scraper logic lives in `src/`. `google_maps_scraper.py` orchestrates Playwright flows, while subpackages handle configuration (`config/`), domain models (`models/`), navigation helpers (`navigation/`), page extraction (`scraper/`), persistence (`persistence/`), and shared utilities (`utils/`).

Key module responsibilities:
- `src/config/settings.py`: typed config loading/validation from `config.yaml`.
- `src/navigation/grid_navigator.py`: grid cell planning and traversal strategy.
- `src/navigation/page_navigator.py`: page-level map/search interaction helpers.
- `src/scraper/business_scraper.py`: business profile extraction.
- `src/scraper/review_scraper.py`: review scraping and pagination handling.
- `src/persistence/csv_writer.py`: CSV output writing for businesses/reviews.
- `src/persistence/progress_tracker.py`: resume/progress metadata for long runs.
- `src/utils/review_analyzer.py`: reply-rate and review-response timing metrics.
- `src/utils/owner_enrichment_service.py`: owner/contact enrichment pipeline.
- `src/services/owner_csv_enricher.py`: enrich an existing CSV without re-scraping.

Entrypoints and interfaces:
- `main_new.py`: primary modern CLI (recommended for new runs).
- `main.py`: legacy CLI flow kept for compatibility.
- `web/app.py`: Flask dashboard/API entrypoint.
- `web/scraper_service.py`: background job execution from web requests.
- `config.yaml`: shared defaults used by CLI and web service.

Tests are split across root-level scripts (`test_integration.py`, `test_scraping_modes.py`) plus package tests in `tests/` (notably owner enrichment coverage).

## Build, Test, and Development Commands
- `python -m venv venv && source venv/bin/activate` (or `.\venv\Scripts\activate`) to create an isolated environment.
- `pip install -r requirements.txt` for scraper dependencies.
- `playwright install` to install browser binaries required by the scraper.
- `pip install -r web/requirements_web.txt` before running the dashboard/API.
- `python main.py -s "coffee shops" -t 20` executes the classic CLI scraper.
- `python main_new.py --scraping-mode coverage` runs the modern CLI in grid-balanced mode.
- `python main_new.py --scraping-mode depth_first` runs depth-first traversal (faster completion per cell).
- `python web/app.py` starts the Flask interface at http://localhost:5000.
- `pytest tests/test_owner_enrichment.py` runs owner enrichment tests only.
- `pytest` runs automated checks; append `--cov=src` for coverage snapshots.
- `black src web tests *.py` and `flake8 src web` enforce formatting and linting.

## Coding Style & Naming Conventions
Target Python 3.9 compatibility with Black defaults (4-space indent, 88-char lines). Prefer snake_case for modules and functions, PascalCase for classes, and descriptive logger names. Keep type hints and docstrings in line with patterns in `src/google_maps_scraper.py`. YAML keys in `config.yaml` remain lowercase with underscores; avoid introducing mixed-case variants.

## Testing Guidelines
Add new tests to files named `test_*.py`.
- Unit tests: mock Playwright and external networked services (LLM/crawling providers).
- Integration tests: mirror `test_integration.py` patterns and state prerequisites (for example, `python web/app.py` running locally).
- Owner enrichment tests: follow `tests/test_owner_enrichment.py` mocking style for deterministic results.
- Coverage target: keep modified modules above 80% where practical and verify with `pytest --cov=src --cov-report=term-missing`.

## Commit & Pull Request Guidelines
Commits use short, imperative subjects (“Add web interface”, “Refactor review extraction”) matching existing history. Group related changes per commit and expand in the body when altering scraping behavior or configuration schemas. Pull requests should summarise scope, list validation steps (`pytest`, sample scrape command, dashboard smoke test), link issues, and attach screenshots or CSV snippets when UX or output changes.

## Configuration & Security Tips
Treat `config.yaml` as environment-specific: adjust Chrome paths, bounds, scraping limits, and output naming locally.
- Do not commit secrets. Keep API keys (for example `OPENROUTER_API_KEY`) in environment variables or `.env` files ignored by git.
- When deploying `web/app.py`, set a unique `SECRET_KEY` and serve through HTTPS.
- Generated artifacts may include sensitive business/review data; prune or sanitize CSV/log/progress files before sharing.

## Scraping Modes & Data Flow
Typical pipeline:
1. CLI/web request merges runtime args with `config.yaml`.
2. `GoogleMapsScraper` initializes map navigation and grid traversal.
3. Business/review scrapers extract profile + review data.
4. `CSVWriter` persists outputs while `ProgressTracker` writes resume state.
5. Optional owner enrichment augments rows during or after scraping.

Mode selection guidance:
- `coverage`: spreads effort across all grid cells for balanced geographic coverage.
- `depth_first`: finishes one cell/area more completely before moving on.

## Output & Resume Artifacts
The project writes run artifacts at repo root by default:
- `result*.csv`: primary business output.
- `reviews.csv`: extracted reviews dataset.
- `scraper_log_*.log`: runtime logs.
- `scraper_progress*.json`: resumable state/checkpoints.

Keep filenames meaningful for long-running jobs, and remove stale progress files when intentionally starting from scratch.
