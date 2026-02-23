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

Key options:

- `-s, --search`: search term (required for scraping).
- `-t, --total`: total target number of results (required for scraping).
- `-g, --grid`: grid size (e.g., `2` means 2x2 cells; default `2`).
- `-b, --bounds`: bounds string `"min_lat,min_lng,max_lat,max_lng"` (optional; defaults are in `config.yaml`).
- `--config`: path to a YAML config file (default `config.yaml`).
- `--headless` / `--no-headless`: override the `browser.headless` setting from config.
- `--scraping-mode`: `fast` or `coverage`; overrides `scraping.default_mode` from config.

When you run the scraper:

- It loads `config.yaml` (or your custom path).
- Applies CLI overrides (headless, max reviews, owner enrichment options).
- Resolves the effective scraping mode as either the CLI value or the config default.

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
  - Optionally enable owner enrichment and choose an LLM model/max pages.
- Monitor progress:
  - See current result count, percentage, cells completed, and per‑cell distribution.
  - Watch streaming updates via SSE.
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


