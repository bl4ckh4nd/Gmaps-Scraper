# Google-Maps-Scrapper
This Python script utilizes the Playwright library to perform web scraping and data extraction from Google Maps. It is particularly designed for obtaining information about businesses, including their name, address, website, phone number, reviews, and more.
## Table of Contents

- [Prerequisite](#prerequisite)
- [Key Features](#key-features)
- [Installation](#installation)
- [How to Use](#how-to-use)
- [Architecture](#architecture)
- [Video Example](#video-example)

## Prerequisite

- Python 3.9 is recommended. Python versions >= 3.10 may work but are not officially supported for the legacy script.
- Google Chrome installed and reachable at the path configured in `config.yaml` (or adjust the `browser.executable_path` there).
- Playwright and its browsers installed (via `pip install -r requirements.txt` and `playwright install` if needed).

Scraping Google Maps may violate Google’s Terms of Service in some jurisdictions. Use this tool responsibly and at your own risk.

## Key Features

- **Business Data Scraping**  
  Scrapes Google Maps listings to extract:
  - Name, address, website, phone number.
  - Place ID and canonical Maps URL.
  - Business types and selected on‑site services (shopping, pickup, delivery).

- **Review Collection & Analysis**  
  For each business, the scraper can:
  - Collect reviews (up to a configurable maximum per business).
  - Parse star ratings, review dates, and owner responses.
  - Persist stable review hashes in a SQLite sidecar so repeated runs can stop at already known reviews instead of scraping the full history again. The hash includes the listing `place_id`, reviewer, rating, and review text.
  - Compute metrics such as:
    - Reply rate to good vs. bad reviews.
    - Average time between reviews.
    - Counts of good/bad/neutral reviews.

- **Scraping Modes (Grid‑Based)**  
  The search area is divided into a geographic grid:
  - **Fast mode** (`fast`): traverse cells sequentially until your global target number of results is reached.
  - **Coverage mode** (`coverage`): distribute the target across all cells for better geographic coverage.

- **Owner Enrichment (Optional)**  
  When enabled, a post‑processing step:
  - Uses Crawl4AI’s adaptive crawler to visit each business’s website.
  - Collects owner‑relevant sections (e.g. “Impressum”, “About”, “Contact”).
  - Uses an OpenRouter‑hosted LLM to extract the legal owner/managing director.  
  Results are stored in dedicated CSV columns (`Owner Name`, `Owner Status`, `Owner Source URL`, etc.).

- **Resumable Jobs & Progress Tracking**  
  The scraper persists:
  - Which grid cells are completed.
  - How many listings were processed per cell.
  - A set of seen place IDs (to avoid re‑scraping in resumed runs).  
  The same progress machinery is used in the CLI and the web dashboard.

- **Schema‑Aware CSV Persistence**  
  The `CSVWriter`:
  - Writes both business and review data to CSV.
  - Detects and upgrades legacy business CSVs to include new owner columns automatically.
  - Deduplicates businesses by name + address on finalization.

- **Web Dashboard & API**  
  A Flask‑based dashboard (`web/app.py`) lets you:
  - Configure scrape jobs via a UI (search term, grid, mode, bounds, headless, owner enrichment).
  - Start jobs and monitor progress (including per‑cell coverage).
  - Download result CSVs and logs once runs complete.
  - Launch “Enrich Existing CSV” jobs for owner enrichment only.

## Installation

1. **Clone the repository**:

   ```bash
   git clone https://github.com/zohaibbashir/google-maps-scraping.git
   cd google-maps-scraping
   ```

2. **Create and activate a virtual environment (recommended)**:

   ```bash
   python3 -m venv venv
   source venv/bin/activate  # Linux/macOS
   # .\venv\Scripts\activate  # Windows PowerShell
   ```

3. **Install core dependencies**:

   ```bash
   pip install -r requirements.txt
   ```

4. **(Optional) Install web dashboard dependencies**:

   ```bash
   pip install -r web/requirements_web.txt
   ```

5. **(Optional) Install Crawl4AI for owner enrichment**:

   ```bash
   pip install "crawl4ai @ git+https://github.com/unclecode/crawl4ai.git"
   crawl4ai install browser
   ```

6. **Configure credentials (for owner enrichment)**:

   Set environment variables:

   - `OPENROUTER_API_KEY`: your OpenRouter API key (prefer free‑tier models like `google/gemini-2.0-flash-exp:free`).
   - Optional: `OPENROUTER_DEFAULT_MODEL` to override the default model globally.

## How to Use

### 1. CLI – Modern Scraper (`main_new.py`)

The modern entrypoint uses the modular `src/` stack and supports grid modes, progress tracking, and owner enrichment.

Basic example:

```bash
python main_new.py -s "Turkish Restaurants in Toronto Canada" -t 20 --scraping-mode fast
```

Single-city grid scrape with automatic bounds resolution:

```bash
python main_new.py \
  --city "Düsseldorf" \
  --query "Cafes" \
  -t 500 \
  --review-mode rolling_365d \
  --scraping-mode fast \
  --headless
```

Parallel city scrape with dedicated cell workers:

```bash
python main_new.py \
  --city "Münster" \
  --query "Restaurants" \
  -t 40000 \
  -g 10 \
  --scraping-mode coverage \
  --review-mode rolling_365d \
  --review-window-days 365 \
  --cell-workers 4 \
  --headless
```

Key options:

- `-s, --search`: search term (required for scraping).
- `--city`: resolve bounds for one city and run the normal single-job scraper inside those bounds.
- `--query`: raw Maps query used with `--city` (for example `Cafes` or `Hotels`).
- `-t, --total`: total target number of results (required for scraping).
- `-g, --grid`: grid size (e.g., `2` means 2x2 cells; default `2`).
- `-b, --bounds`: bounds string `"min_lat,min_lng,max_lat,max_lng"` (optional; defaults are in `config.yaml`).
- `--config`: path to a YAML config file (default `config.yaml`).
- `--headless` / `--no-headless`: override the `browser.headless` setting from config.
- `--browser-state-file`: optional JSON file used to persist Playwright cookies/local storage so consent choices survive browser restarts and reruns.
- `--scraping-mode`: `fast` or `coverage`; overrides `scraping.default_mode` from config.
- `--review-mode`: `all_available` or `rolling_365d`; controls whether review extraction stops at the rolling review window.
- `--review-window-days`: size of the rolling review window when `--review-mode rolling_365d` is used.
- `--extract`: comma-separated extraction groups to enable for this run. Supported groups are `contact_fields`, `business_details`, `review_summary`, `review_rows`, `review_analytics`, `deleted_review_signals`, and `website_modernity`.
- `--skip-extract`: comma-separated extraction groups to disable for this run.
- `--city-bounds-cache`: override the JSON cache file used for city bounds in `--city` mode.
- `--refresh-city-bounds`: refresh missing/stale city bounds from Nominatim before the run.
- `--cell-workers`: number of parallel worker processes for city mode. `1` keeps the sequential behavior; values greater than `1` use dedicated per-cell workers. The first version supports this only with `--scraping-mode coverage`.

When you run the scraper:

- It loads `config.yaml` (or your custom path).
- Applies CLI overrides (headless, max reviews, owner enrichment options).
- Applies extraction-group overrides from `--extract` / `--skip-extract`. Current defaults preserve the existing scraper output; `website_modernity` is opt-in because it performs an extra homepage probe.
- Persists browser consent/session state next to the progress file by default (or to `--browser-state-file` when provided), which reduces repeated Google cookie prompts after retries/resume.
- Applies review-mode overrides for rolling 12-month analysis when configured.
- Resolves the effective scraping mode as either the CLI value or the config default.
- In `--city` mode, it resolves the city bounds first, keeps the Maps search box text as the raw `--query`, and uses the city name only for labeling/output (`<query> in <city>`).
- In parallel `--city` mode, each completed cell writes its own artifacts under a `cells/` directory and the coordinator keeps a `city_cell_manifest.json` plus merged city-level business/review CSVs.
- Writes a per-run category summary CSV grouped by search query and observed Google Maps place type.

When `website_modernity` is enabled, the business CSV also includes deterministic homepage-quality fields such as `Website Status`, `Website Modernity Score`, `Website Modernity Reason`, `Website Uses HTTPS`, `Website Mobile Friendly Hint`, `Website Structured Data Hint`, and `Website Stale/Broken Hint`.

#### CLI – City Campaign Queue

You can also start a queued multi-city campaign from the CLI using a markdown city source such as `/run/media/merres/Volume/Coding2/ShoppaCrawler/docs/cities.md`.

Example smoke test:

```bash
python main_new.py \
  --campaign-cities-file /run/media/merres/Volume/Coding2/ShoppaCrawler/docs/cities.md \
  -t 3 \
  --campaign-output-dir ./campaign_runs/cities_smoke \
  --campaign-smoke-test \
  --campaign-smoke-cities 1 \
  --campaign-smoke-categories 1 \
  --review-mode rolling_365d \
  --scraping-mode fast \
  --headless
```

Campaign mode:

- parses the ordered city list from the markdown file
- expands categories into queries like `{category} in {city}`
- resolves per-city bounding boxes from the bundled German city cache (`data/city_bounds_de.json`)
- queues each job through the existing job runner
- writes campaign-specific artifacts into the chosen output directory:
  - `campaign_businesses.csv`
  - `campaign_reviews.csv`
  - `campaign_manifest.json`
  - `campaign_businesses_category_summary.csv`

Useful flags:

- `--campaign-categories`: comma-separated categories (default: Hotels, Restaurants, Cafes, Bakeries, Pharmacies)
- `--campaign-bounds-cache`: override the JSON cache file used for per-city bounds
- `--campaign-refresh-bounds`: refresh missing/stale city bounds from Nominatim before building jobs
- `--campaign-search-template`: override the search query template
- `--campaign-smoke-test`: run only a reduced city/category slice first
- `--campaign-resume`: resume an existing campaign manifest in `--campaign-output-dir`

#### Durable Queue Mode (Postgres + Redis/RQ)

For multi-worker runs, start the infrastructure stack:

```bash
docker compose up -d postgres redis rq-dashboard
export DATABASE_URL="postgresql://gmaps:gmaps@localhost:5432/gmaps_scraper"
export REDIS_URL="redis://localhost:6379/0"
python main_new.py --migrate-db
```

Create a durable queued campaign:

```bash
python main_new.py \
  -s "Restaurants in Münster" \
  -t 1000 \
  -g 5 \
  --scraping-mode coverage \
  --review-mode rolling_365d \
  --review-window-days 365 \
  --queue-start \
  --headless
```

Run one or more workers in separate terminals:

```bash
export DATABASE_URL="postgresql://gmaps:gmaps@localhost:5432/gmaps_scraper"
export REDIS_URL="redis://localhost:6379/0"
python main_new.py --worker
```

Export a completed durable campaign:

```bash
python main_new.py --export-campaign --campaign-id <campaign-id>
```

Durable mode stores progress in Postgres instead of JSON:

- `grid_cells` tracks every tile and whether discovery finished.
- `listings` stores canonical place IDs, Maps URLs, category/type, and latest business fields.
- `listing_mode_state` tracks per-listing completion separately for `tile_full` and `rolling_365d`.
- `reviews` deduplicates reviews by `(place_id, review_hash)`.
- `scrape_attempts` stores structured errors for retries and post-run debugging.

To let the Flask dashboard enqueue durable jobs instead of in-process threads, set:

```bash
export SCRAPER_USE_RQ=1
python web/app.py
```

#### Dockerized Postgres-First Runner Stack

The repository now also includes a Docker stack for the new **Postgres-first** architecture:

- `web`: Flask dashboard/API
- `scheduler`: promotes queued jobs and recovers stale runs
- `runner`: headed Playwright worker with `Xvfb` + `fluxbox`
- `postgres`: source of truth for jobs, runs, sessions, checkpoints, and artifacts
- `redis`: optional companion service for future live-event/caching paths

Bring the stack up:

```bash
docker compose up --build postgres redis web scheduler runner
```

Then open:

- Dashboard: `http://localhost:5000`
- Runner noVNC session: `http://localhost:7900`

The Docker stack enables:

- one dedicated browser session per active job
- headed Playwright inside Docker
- central global limits via `MAX_ACTIVE_JOBS` and `MAX_ACTIVE_BROWSER_SESSIONS`
- persistent job/session state in Postgres
- visible migration state in the dashboard (`in_process`, `rq`, or `postgres_runner`)

Useful environment variables:

- `SCRAPER_USE_POSTGRES_RUNNER=1`
- `MAX_ACTIVE_JOBS`
- `MAX_ACTIVE_BROWSER_SESSIONS`
- `RUNNER_HEADED=true`
- `ENABLE_NOVNC=true`
- `SCRAPER_ARTIFACT_ROOT`
- `SCRAPER_SESSION_ROOT`

New CLI service modes for this architecture:

- `python main_new.py --scheduler`
- `python main_new.py --scheduler-once`
- `python main_new.py --runner-service`
- `python main_new.py --runner-once`

Scale runners explicitly when needed:

```bash
docker compose up --scale runner=2
```

For multi-runner deployments, prefer putting the noVNC access behind a reverse proxy instead of binding every runner directly to the same host port.

#### Private GHCR Deployment

The repository now includes a GitHub Actions workflow at `.github/workflows/docker-publish.yml` that publishes the shared app image to **private GHCR** on pushes to `main`, tags like `v1.2.3`, or manual runs.

The three app services all reuse the same image:

- `web`
- `scheduler`
- `runner`

For server-side deployments, use the GHCR compose override:

```bash
export APP_IMAGE=ghcr.io/bl4ckh4nd/google-maps-scrapper
export APP_TAG=main
```

Log the target host into GHCR with a token that has `read:packages` and access to the private package:

```bash
echo "$GHCR_READ_TOKEN" | docker login ghcr.io -u <github-user> --password-stdin
```

Then pull and start the private images:

```bash
docker compose -f docker-compose.yml -f docker-compose.ghcr.yml pull
docker compose -f docker-compose.yml -f docker-compose.ghcr.yml up -d
```

Notes:

- Keep the package private in GHCR.
- Do not commit tokens into `.env` files or Compose files.
- The deploy host only needs `read:packages`; publishing is handled by GitHub Actions with `GITHUB_TOKEN`.
- Release tags publish matching image tags, and default-branch pushes publish `:main`.

#### Print Effective Configuration

To see the effective configuration (after applying CLI overrides) without running a scrape:

```bash
python main_new.py --config config.yaml --scraping-mode coverage --headless --print-config
```

This prints a JSON dump of the `ScraperSettings` dataclass plus the `effective_mode_cli` value that would be used for a run, then exits.

### 2. CLI – Owner Enrichment Only

You can retrofit owner information into an existing business CSV (e.g. from past runs):

```bash
python main_new.py --owner-enrich-csv result.csv --owner-output result_owner_enriched.csv
```

Flags of note:

- `--owner-enrich-csv`: path to an existing business CSV.
- `--owner-output`: where to write the enriched CSV. If omitted, a `*_owner_enriched.csv` file is created.
- `--owner-in-place`: overwrite the source file in‑place (a `.bak` backup is created first).
- `--owner-resume`: resume a partially completed enrichment run (uses a sidecar `.state.json` file). Not supported with `--owner-in-place`.
- `--owner-no-skip-existing`: reprocess rows that already have an `Owner Name`.
- `--owner-model`: override the OpenRouter model for this pass.

Model note:
- Explicit model selection is always honored. The `owner_enrichment.allow_free_models_only` setting is retained for compatibility but does not block non-free models.

### 3. Web Dashboard (`web/app.py`)

To run the web dashboard:

```bash
python web/app.py
```

Then open `http://localhost:5000` in your browser.

From the dashboard you can:

- Configure and launch scrape jobs:
  - Set search term, total results, grid size, bounds (via map), scraping mode, headless flag.
  - Toggle extraction groups for contact fields, business details, review summary/rows/analytics, deleted-review signals, and website modernity checks.
  - Optionally enable owner enrichment and choose an LLM model/max pages.
- Monitor progress:
  - See current result count, percentage, cells completed, and per‑cell distribution.
  - Watch streaming updates via SSE.
  - In Postgres-first runner mode, inspect orchestration states such as `queued`, `waiting_for_slot`, `starting_session`, `backoff`, and `retry_pending`.
  - Use operator actions such as retry, restart from checkpoint, restart from scratch, and cancel.
- Download results:
  - Business CSV, reviews CSV, and the scraper log for a completed job.
- Launch owner enrichment jobs:
  - Use the “Enrich Existing CSV” form to run the same owner enrichment pipeline on a CSV created earlier (either via CLI or the web).

Defaults exposed in the dashboard (bounds, grid size, max reviews, default scraping mode) are derived from `config.yaml`.

### 4. Legacy CLI (`main.py`)

The original script is still available for backwards compatibility:

```bash
python main.py -s "Turkish Restaurants in Toronto Canada" -t 20
```

This path:

- Launches the browser, performs the search, and writes results to `result.csv`.
- Does **not** support:
  - Coverage mode.
  - Owner enrichment.
  - The newer review analysis metrics and progress tracking.

Prefer `main_new.py` for all new workflows; consider `main.py` legacy‑only.

## Architecture

For a deeper description of how the scraper is structured (orchestrator, navigation, scrapers, persistence, web API, and owner enrichment), see `ARCHITECTURE.md`.
## Video Example:
I've included an example of running the code below.

https://www.linkedin.com/posts/zohaibbashir_python-data-webscraping-activity-7093920891411062784-flEQ
