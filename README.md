# Google-Maps-Scrapper
This Python script utilizes the Playwright library to perform web scraping and data extraction from Google Maps. It is particularly designed for obtaining information about businesses, including their name, address, website, phone number, reviews, and more.
## Table of Contents

- [Prerequisite](#prerequisite)
- [Key Features](#key-features)
- [Installation](#installation)
- [How to Use](#how-to-use)
- [Video Example](#video-example)

## Prerequisite
- This code requires a python version below 3.10
- Any version of python beyond 3.9 may cause issues and may not work properly

## Key Features
- Data Scraping: The script scrapes data from Google Maps listings, extracting valuable information about businesses, such as their name, address, website, and contact details.

- Review Analysis: It extracts review counts and average ratings, providing insights into businesses' online reputation.

- Owner Enrichment (optional): When enabled, the scraper visits each business website with Crawl4AI adaptive crawling, captures imprint/contact details, and asks a lightweight OpenRouter-hosted LLM to extract the legal owner or managing director.

- Business Type Detection: The script identifies whether a business offers in-store shopping, in-store pickup, or delivery services.

- Operating Hours: It extracts information about the business's operating hours.

- Introduction Extraction: The script also scrapes introductory information about the businesses when available.

- Data Cleansing: It cleanses and organizes the scraped data, removing redundant or unnecessary columns.

- CSV Export: The cleaned data is exported to a CSV file for further analysis or integration with other tools.

## Installation

1. Clone this repository:

   ```bash
   git clone https://github.com/zohaibbashir/google-maps-scraping.git
2. Navigate to the project directory:
   ```bash
   cd google-maps-scraping
3. Install the required Python packages:
    ```bash
    pip install -r requirements.txt

## How to Use:

To use this script, follow these steps:

1. Run the modern entry point for structured scraping:
   ```bash
   python main_new.py -s "Turkish Restaurants in Toronto Canada" -t 20 --scraping-mode fast
   ```
   Use `--config` if you want to point at a custom YAML configuration file.

2. (Legacy) The original script is still available for backwards compatibility:
   ```bash
   python main.py -s "Turkish Restaurants in Toronto Canada" -t 20
   ```
   The browser will launch, perform the search, and export results to `result.csv`.

3. Optional owner enrichment (after installing Crawl4AI locally):
   ```bash
   pip install "crawl4ai @ git+https://github.com/unclecode/crawl4ai.git"
   crawl4ai install browser
   export OPENROUTER_API_KEY=your-openrouter-key
   python main_new.py -s "coffee shops berlin" -t 10 --owner-enrichment --owner-model google/gemini-2.0-flash-exp:free
   ```
   The resulting CSV will include additional owner columns such as `Owner Name`, `Owner Status`, and `Owner Source URL`.

4. When using the web dashboard (`python web/app.py`), enable “Enrich owner details” in the form to pass overrides that activate the same workflow.

### Enrich Existing CSVs

Retrofit owner information into a legacy scrape without collecting fresh business data:

```bash
python main_new.py --owner-enrich-csv result.csv --owner-output result_owner_enriched.csv
```

Flags of note:

- `--owner-in-place` overwrites the source file after creating a `.bak` backup.
- `--owner-resume` continues a previous run that stopped partway (uses a sidecar state file).
- `--owner-no-skip-existing` reprocesses rows that already contain an owner name.
- `--owner-model` selects a specific OpenRouter model for the extraction pass.

From the dashboard, use the “Enrich Existing CSV” card to launch the same process: supply the CSV path, optional output path, and toggle resume/in-place as needed. Progress (rows processed, owners found) surfaces alongside scrape jobs, and the enriched CSV appears in the results panel when the job finishes.

- Install the new dependencies listed in `requirements.txt` (`crawl4ai`, `httpx`).
- Prepare Crawl4AI locally:
  - `pip install crawl4ai` (or install from GitHub for the latest release).
  - `crawl4ai install browser` to download the playwright engine Crawl4AI relies on.
- Provide credentials via environment variables:
  - `OPENROUTER_API_KEY` – API key for OpenRouter (free-tier models recommended, e.g. `google/gemini-2.0-flash-exp:free`).
  - Optionally `OPENROUTER_DEFAULT_MODEL` to change the default LLM globally.
- Adjust `config.yaml` → `owner_enrichment` block to fine tune crawl depth, allowed models, retry limits, and logging.
- From the CLI you can override per run with `--owner-enrichment`, `--owner-model`, and `--owner-max-pages`.
## Video Example:
I've included an example of running the code below.

https://www.linkedin.com/posts/zohaibbashir_python-data-webscraping-activity-7093920891411062784-flEQ


