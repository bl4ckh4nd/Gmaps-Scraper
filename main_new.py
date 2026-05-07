"""
Google Maps Scraper - Entry Point

A refactored, maintainable Google Maps scraper with modular architecture.
Extracts business information and reviews from Google Maps using geographic grid search.
"""

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Dict, Tuple

from src.config import (
    EXTRACTION_GROUPS,
    Config,
    apply_argument_overrides,
    parse_extraction_group_csv,
    validate_extraction_groups,
)
from src.google_maps_scraper import GoogleMapsScraper, create_scraper_from_args
from src.navigation import GridNavigator
from src.persistence import OrchestratorStore, PostgresStore
from src.services import OwnerCSVEnricher, OwnerCSVEnrichmentOptions
from src.services.city_campaign_service import (
    DEFAULT_BOUNDS_CACHE_PATH,
    DEFAULT_CAMPAIGN_CATEGORIES,
    CityCampaignOptions,
    resolve_city_bounds,
    run_city_campaign,
)
from src.services.city_cell_worker_service import (
    CityCellWorkerOptions,
    run_city_cell_workers,
)
from src.services.queue_service import enqueue_discover_cell, run_worker
from src.services.runner_service import RunnerService
from src.services.scheduler_service import SchedulerService
from src.utils import ScraperException, load_dotenv


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments.
    
    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        description="Extract business data and reviews from Google Maps",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main_new.py -s "restaurants in Toronto" -t 50
  python main_new.py -s "pharmacies in Germany" -t 100 -g 3
  python main_new.py -s "Turkish Restaurants" -t 20 -b "43.6,-79.5,43.9,-79.2" -g 2
  
Configuration:
  Create a config.yaml file to customize browser, scraping, and output settings.
  Use --config to specify a different configuration file.
        """
    )
    
    # Scraping arguments (required when running scrape mode)
    parser.add_argument(
        "-s", "--search", 
        type=str,
        help="Search term (required for scraping)"
    )
    parser.add_argument(
        "-t", "--total", 
        type=int,
        help="Total number of results to collect (required for scraping)"
    )
    
    # Optional arguments
    parser.add_argument(
        "-b", "--bounds", 
        type=str,
        help="Search bounds as 'min_lat,min_lng,max_lat,max_lng' (optional)"
    )
    parser.add_argument(
        "-g", "--grid", 
        type=int, 
        default=2,
        help="Grid size for geographic division (default: 2x2)"
    )
    parser.add_argument(
        "--config", 
        type=str, 
        default="config.yaml",
        help="Configuration file path (default: config.yaml)"
    )
    parser.add_argument(
        "--headless", 
        dest="headless",
        action="store_true",
        help="Run browser in headless mode (overrides config to True)"
    )
    parser.add_argument(
        "--no-headless",
        dest="headless",
        action="store_false",
        help="Run browser with UI (overrides config to False)"
    )
    parser.add_argument(
        "--browser-state-file",
        type=str,
        help="Persist Playwright cookies/local storage to this JSON file",
    )
    parser.add_argument(
        "--max-reviews", 
        type=int, 
        help="Maximum reviews per business (overrides config)"
    )
    parser.add_argument(
        "--review-mode",
        type=str,
        choices=['all_available', 'rolling_365d'],
        help="Review collection mode: 'all_available' or 'rolling_365d'"
    )
    parser.add_argument(
        "--review-window-days",
        type=int,
        help="Rolling review window size in days when review mode is rolling_365d"
    )
    parser.add_argument(
        "--city",
        type=str,
        help="Resolve bounds for a specific city and run the normal tile-based scraper within that city"
    )
    parser.add_argument(
        "--query",
        type=str,
        help="Query typed into Google Maps when using --city (for example: Cafes)"
    )
    parser.add_argument(
        "--cell-workers",
        type=int,
        default=1,
        help="Number of parallel cell workers for city mode (default: 1)"
    )
    parser.add_argument(
        "--city-bounds-cache",
        type=str,
        default=str(DEFAULT_BOUNDS_CACHE_PATH),
        help="JSON cache file with per-city bounds for city-scoped scraping"
    )
    parser.add_argument(
        "--refresh-city-bounds",
        action="store_true",
        help="Refresh city bounds from Nominatim before running a city-scoped scrape"
    )
    parser.add_argument(
        "--campaign-cities-file",
        type=str,
        help="Run queued city campaign mode using the cities defined in this markdown file"
    )
    parser.add_argument(
        "--campaign-categories",
        type=str,
        default=",".join(DEFAULT_CAMPAIGN_CATEGORIES),
        help="Comma-separated categories for campaign mode"
    )
    parser.add_argument(
        "--campaign-output-dir",
        type=str,
        help="Dedicated output directory for campaign artifacts"
    )
    parser.add_argument(
        "--campaign-bounds-cache",
        type=str,
        default=str(DEFAULT_BOUNDS_CACHE_PATH),
        help="JSON cache file with per-city bounds for campaign mode"
    )
    parser.add_argument(
        "--campaign-refresh-bounds",
        action="store_true",
        help="Refresh city bounds from Nominatim before building campaign jobs"
    )
    parser.add_argument(
        "--campaign-search-template",
        type=str,
        default="{category} in {city}",
        help="Search template for campaign mode"
    )
    parser.add_argument(
        "--campaign-smoke-test",
        action="store_true",
        help="Run only a small city/category slice before a full campaign"
    )
    parser.add_argument(
        "--campaign-smoke-cities",
        type=int,
        default=2,
        help="Number of cities to include in smoke-test mode"
    )
    parser.add_argument(
        "--campaign-smoke-categories",
        type=int,
        default=2,
        help="Number of categories to include in smoke-test mode"
    )
    parser.add_argument(
        "--campaign-resume",
        action="store_true",
        help="Resume an existing campaign manifest in --campaign-output-dir"
    )
    parser.add_argument(
        "--scraping-mode", 
        type=str,
        choices=['fast', 'coverage'],
        help="Scraping mode: 'fast' (sequential) or 'coverage' (distributed). Overrides config default_mode."
    )
    parser.add_argument(
        "--log-level", 
        type=str, 
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help="Logging level (default: INFO)"
    )
    parser.add_argument(
        "--print-config",
        action="store_true",
        help="Print effective configuration (after applying CLI overrides) and exit",
    )
    parser.add_argument(
        "--migrate-db",
        action="store_true",
        help="Initialize or upgrade the Postgres schema used by queued scraper workers",
    )
    parser.add_argument(
        "--queue-start",
        action="store_true",
        help="Create a durable Postgres campaign and enqueue its grid cells in Redis/RQ",
    )
    parser.add_argument(
        "--worker",
        action="store_true",
        help="Run an RQ worker for scraper queues",
    )
    parser.add_argument(
        "--scheduler",
        action="store_true",
        help="Run the Postgres-first scheduler loop",
    )
    parser.add_argument(
        "--scheduler-once",
        action="store_true",
        help="Run one Postgres-first scheduler reconciliation cycle and exit",
    )
    parser.add_argument(
        "--runner-service",
        action="store_true",
        help="Run the Postgres-first headed Playwright runner loop",
    )
    parser.add_argument(
        "--runner-once",
        action="store_true",
        help="Run one Postgres-first runner cycle and exit",
    )
    parser.add_argument(
        "--worker-queues",
        type=str,
        default="",
        help="Comma-separated RQ queues to process (default: discovery and listing queues)",
    )
    parser.add_argument(
        "--export-campaign",
        action="store_true",
        help="Export a Postgres-backed campaign to business/review CSV files",
    )
    parser.add_argument(
        "--campaign-id",
        type=str,
        help="Durable campaign ID used with --export-campaign",
    )
    parser.add_argument(
        "--owner-enrichment",
        action="store_true",
        help="Enable adaptive owner enrichment workflow"
    )
    parser.add_argument(
        "--owner-model",
        type=str,
        help="Override default OpenRouter model for owner extraction"
    )
    parser.add_argument(
        "--owner-max-pages",
        type=int,
        help="Maximum pages to crawl per business website for owner enrichment"
    )
    parser.add_argument(
        "--extract",
        type=str,
        help=(
            "Comma-separated extraction groups to enable for this run. "
            f"Supported groups: {', '.join(EXTRACTION_GROUPS)}"
        ),
    )
    parser.add_argument(
        "--skip-extract",
        type=str,
        help=(
            "Comma-separated extraction groups to disable for this run. "
            f"Supported groups: {', '.join(EXTRACTION_GROUPS)}"
        ),
    )
    parser.add_argument(
        "--owner-enrich-csv",
        type=str,
        help="Path to an existing business CSV to enrich with owner data"
    )
    parser.add_argument(
        "--owner-output",
        type=str,
        help="Optional output path for owner-enriched CSV"
    )
    parser.add_argument(
        "--owner-in-place",
        action="store_true",
        help="Overwrite the input CSV with enriched data (creates .bak backup)"
    )
    parser.add_argument(
        "--owner-resume",
        action="store_true",
        help="Resume a previously interrupted owner enrichment run"
    )
    parser.set_defaults(owner_skip_existing=True, headless=None)
    parser.add_argument(
        "--owner-no-skip-existing",
        dest="owner_skip_existing",
        action="store_false",
        help="Re-enrich rows even if they already contain owner information"
    )
    parser.add_argument("--search-input", type=str, help=argparse.SUPPRESS)
    parser.add_argument("--cell-id", action="append", help=argparse.SUPPRESS)
    parser.add_argument("--result-file", type=str, help=argparse.SUPPRESS)
    parser.add_argument("--reviews-file", type=str, help=argparse.SUPPRESS)
    parser.add_argument("--progress-file", type=str, help=argparse.SUPPRESS)
    parser.add_argument("--log-file", type=str, help=argparse.SUPPRESS)

    return parser.parse_args()


def parse_bounds(bounds_str: str) -> Tuple[float, float, float, float]:
    """Parse bounds string into tuple of floats.
    
    Args:
        bounds_str: Comma-separated bounds string
        
    Returns:
        Tuple of (min_lat, min_lng, max_lat, max_lng)
        
    Raises:
        ValueError: If bounds format is invalid
    """
    try:
        bounds = tuple(map(float, bounds_str.split(',')))
        if len(bounds) != 4:
            raise ValueError("Bounds must have exactly 4 values")
        
        min_lat, min_lng, max_lat, max_lng = bounds
        
        # Validate bounds
        if min_lat >= max_lat:
            raise ValueError("min_lat must be less than max_lat")
        if min_lng >= max_lng:
            raise ValueError("min_lng must be less than max_lng")
        if not (-90 <= min_lat <= 90) or not (-90 <= max_lat <= 90):
            raise ValueError("Latitude values must be between -90 and 90")
        if not (-180 <= min_lng <= 180) or not (-180 <= max_lng <= 180):
            raise ValueError("Longitude values must be between -180 and 180")
        
        return bounds
        
    except Exception as e:
        raise ValueError(f"Invalid bounds format '{bounds_str}': {e}") from e


def validate_arguments(args: argparse.Namespace) -> None:
    """Validate command line arguments.
    
    Args:
        args: Parsed arguments
        
    Raises:
        ValueError: If arguments are invalid
    """
    if getattr(args, 'owner_max_pages', None) is not None and args.owner_max_pages <= 0:
        raise ValueError("Owner max pages must be positive")
    if getattr(args, 'extract', None) is not None:
        validate_extraction_groups(parse_extraction_group_csv(args.extract))
    if getattr(args, 'skip_extract', None):
        validate_extraction_groups(parse_extraction_group_csv(args.skip_extract))

    if (
        getattr(args, 'migrate_db', False)
        or getattr(args, 'worker', False)
        or getattr(args, 'scheduler', False)
        or getattr(args, 'scheduler_once', False)
        or getattr(args, 'runner_service', False)
        or getattr(args, 'runner_once', False)
    ):
        return

    if getattr(args, 'export_campaign', False):
        if not getattr(args, 'campaign_id', None):
            raise ValueError("--export-campaign requires --campaign-id")
        return

    if getattr(args, 'owner_enrich_csv', None):
        if args.owner_in_place and args.owner_output:
            raise ValueError("--owner-in-place cannot be combined with --owner-output")
        if args.owner_in_place and args.owner_resume:
            raise ValueError("--owner-in-place cannot be combined with --owner-resume")
        return

    if getattr(args, 'city', None) or getattr(args, 'query', None):
        if getattr(args, 'campaign_cities_file', None):
            raise ValueError("--city/--query cannot be combined with --campaign-cities-file")
        if args.search:
            raise ValueError("--city mode cannot be combined with --search")
        if args.bounds:
            raise ValueError("--city mode cannot be combined with --bounds")
        if not args.city or not args.city.strip():
            raise ValueError("--city is required in city mode")
        if not args.query or not args.query.strip():
            raise ValueError("--query is required in city mode")
        if args.total is None or args.total <= 0:
            raise ValueError("Total results must be positive in city mode")
        if args.total > 10000:
            print("Warning: Large result counts may take a very long time")
        if args.grid < 1 or args.grid > 10:
            raise ValueError("Grid size must be between 1 and 10")
        if args.max_reviews is not None and args.max_reviews < 0:
            raise ValueError("Max reviews cannot be negative")
        if getattr(args, 'review_window_days', None) is not None and args.review_window_days <= 0:
            raise ValueError("Review window days must be positive")
        if getattr(args, 'cell_workers', 1) <= 0:
            raise ValueError("cell_workers must be positive")
        return

    if getattr(args, 'campaign_cities_file', None):
        if args.total is None or args.total <= 0:
            raise ValueError("Total results must be positive in campaign mode")
        if getattr(args, 'campaign_smoke_cities', 0) <= 0:
            raise ValueError("campaign_smoke_cities must be positive")
        if getattr(args, 'campaign_smoke_categories', 0) <= 0:
            raise ValueError("campaign_smoke_categories must be positive")
        if getattr(args, 'campaign_resume', False) and not getattr(args, 'campaign_output_dir', None):
            raise ValueError("--campaign-resume requires --campaign-output-dir")
        return

    # Validate search term
    if not args.search or not args.search.strip():
        raise ValueError("Search term cannot be empty")

    # Validate total results
    if args.total is None or args.total <= 0:
        raise ValueError("Total results must be positive")
    if args.total > 10000:
        print("Warning: Large result counts may take a very long time")
    
    # Validate grid size
    if args.grid < 1 or args.grid > 10:
        raise ValueError("Grid size must be between 1 and 10")
    if getattr(args, 'cell_workers', 1) <= 0:
        raise ValueError("cell_workers must be positive")
    
    # Validate max reviews if provided
    if args.max_reviews is not None and args.max_reviews < 0:
        raise ValueError("Max reviews cannot be negative")
    if getattr(args, 'review_window_days', None) is not None and args.review_window_days <= 0:
        raise ValueError("Review window days must be positive")


def build_effective_config(args: argparse.Namespace) -> Config:
    """Load configuration and apply CLI overrides without starting the scraper."""

    config_path = getattr(args, "config", "config.yaml")
    try:
        config = Config.from_file(config_path)
    except Exception:
        config = Config()

    settings = config.settings

    # Apply the same overrides we support in the CLI
    apply_argument_overrides(settings, args)
    return config


def print_effective_config(args: argparse.Namespace) -> None:
    """Print the effective configuration after applying CLI overrides."""

    config = build_effective_config(args)
    settings_dict = asdict(config.settings)

    # Compute effective scraping mode for this run
    default_mode = settings_dict.get("scraping", {}).get("default_mode", "fast")
    effective_mode = args.scraping_mode or default_mode
    settings_dict.setdefault("scraping", {})["effective_mode_cli"] = effective_mode

    print("=" * 60)
    print("Google Maps Scraper - Effective Configuration")
    print("=" * 60)
    print(json.dumps(settings_dict, indent=2, default=str))
    print("=" * 60)
    print("Config file:", getattr(args, "config", "config.yaml"))
    print("Effective scraping mode for this run:", effective_mode)


def resolve_city_scrape_inputs(
    args: argparse.Namespace,
) -> tuple[str, str, Tuple[float, float, float, float]]:
    """Resolve city mode into display label, raw search input, and bounds."""

    city = (args.city or "").strip()
    query = (args.query or "").strip()
    resolved_bounds = resolve_city_bounds(
        [city],
        cache_path=args.city_bounds_cache,
        refresh=args.refresh_city_bounds,
    )[city]
    display_search_term = f"{query} in {city}"
    return display_search_term, query, resolved_bounds


def run_owner_csv_enrichment(args: argparse.Namespace) -> None:
    """Run owner enrichment on an existing CSV file."""

    csv_path = Path(args.owner_enrich_csv).expanduser()
    if not csv_path.exists():
        raise FileNotFoundError(f"Owner enrichment CSV not found: {csv_path}")

    # Pre-count total rows for stable progress reporting
    total_rows = 0
    try:
        with csv_path.open("r", encoding="utf-8", newline="") as fh:
            # subtract header row
            total_rows = max(sum(1 for _ in fh) - 1, 0)
    except Exception:
        total_rows = 0

    config_path = getattr(args, 'config', 'config.yaml')
    try:
        config = Config.from_file(config_path)
    except Exception:
        config = Config()

    output_path = Path(args.owner_output).expanduser() if args.owner_output else None
    if args.owner_in_place and output_path and output_path != csv_path:
        raise ValueError("--owner-in-place cannot be combined with --owner-output")
    if args.owner_in_place and args.owner_resume:
        raise ValueError("--owner-in-place cannot be combined with --owner-resume")

    options = OwnerCSVEnrichmentOptions(
        input_path=csv_path,
        output_path=output_path,
        in_place=args.owner_in_place,
        resume=args.owner_resume,
        owner_model=args.owner_model,
        skip_existing=args.owner_skip_existing,
    )

    print("=" * 60)
    print("Owner Enrichment Mode")
    print("=" * 60)
    print(f"Input CSV: {csv_path}")
    if options.in_place:
        print("Output: in-place (backup .bak)")
    else:
        target = options.output_path or csv_path.with_name(f"{csv_path.stem}_owner_enriched{csv_path.suffix}")
        print(f"Output CSV: {target}")
    if args.owner_model:
        print(f"OpenRouter model override: {args.owner_model}")
    print("Skip existing owners:" , "yes" if options.skip_existing else "no")
    print("Resume previous run:" , "yes" if options.resume else "no")
    print("=" * 60)

    enricher = OwnerCSVEnricher(config)

    def progress(stats: Dict[str, int]) -> None:
        total = total_rows or stats.get("total_rows", 0)
        processed = stats.get("processed_rows", 0)
        owners = stats.get("owners_found", 0)
        print(
            f"Processed {processed}/{total} rows | Owners found: {owners}",
            end="\r",
            flush=True,
        )

    result = enricher.enrich(options, progress_callback=progress)

    print("\n" + "-" * 60)
    print("Owner enrichment completed")
    print(f"Total rows read: {result.total_rows}")
    print(f"Rows written: {result.processed_rows}")
    print(f"Owners found: {result.owners_found}")
    if result.skipped_existing:
        print(f"Skipped existing owners: {result.skipped_existing}")
    if result.failed_rows:
        print(f"Rows failed: {result.failed_rows}")
    if result.output_path:
        print(f"Output saved to: {result.output_path}")


def migrate_postgres_schema() -> None:
    """Create or upgrade durable scraper tables."""

    store = OrchestratorStore()
    store.initialize_schema()
    print("Postgres scraper/orchestrator schema is ready.")


def run_queue_worker(args: argparse.Namespace) -> None:
    """Run Redis/RQ workers for durable scraper tasks."""

    queue_names = [
        item.strip()
        for item in (args.worker_queues or "").split(",")
        if item.strip()
    ] or None
    run_worker(queue_names=queue_names)


def run_scheduler(args: argparse.Namespace) -> None:
    """Run the Postgres-first scheduler service."""

    service = SchedulerService()
    if getattr(args, "scheduler_once", False):
        result = service.run_once()
        print(f"Scheduler cycle completed: {result}")
        return
    service.run_forever()


def run_runner_service(args: argparse.Namespace) -> None:
    """Run the Postgres-first headed Playwright runner service."""

    service = RunnerService()
    if getattr(args, "runner_once", False):
        handled = service.run_once()
        print("Runner processed a job." if handled else "Runner found no runnable jobs.")
        return
    service.run_forever()


def start_queued_campaign(args: argparse.Namespace) -> str:
    """Create a durable campaign, persist its cells, and enqueue discovery jobs."""

    effective_config = build_effective_config(args)
    settings = effective_config.settings
    bounds = parse_bounds(args.bounds) if args.bounds else settings.grid.default_bounds
    scraping_mode = args.scraping_mode or settings.scraping.default_mode
    review_mode = args.review_mode or settings.scraping.review_mode
    review_window_days = args.review_window_days or settings.scraping.review_window_days
    headless = (
        args.headless
        if getattr(args, "headless", None) is not None
        else settings.browser.headless
    )

    store = PostgresStore()
    store.initialize_schema()
    campaign_id = store.create_campaign(
        search_term=args.search,
        search_input_term=getattr(args, "search_input", None) or args.search,
        total_target=args.total,
        bounds=bounds,
        grid_size=args.grid,
        scraping_mode=scraping_mode,
        review_mode=review_mode,
        review_window_days=review_window_days,
        metadata={
            "config_path": args.config,
            "output_dir": str(Path(settings.files.result_filename).expanduser().parent),
            "headless": headless,
            "max_reviews": args.max_reviews,
            "config_overrides": {
                "extraction": asdict(settings.extraction),
            },
        },
    )

    grid = GridNavigator(
        bounds,
        args.grid,
        settings.grid.default_zoom_level,
    )
    if not grid.validate_bounds():
        raise ValueError("Invalid bounds for queued campaign")
    store.create_grid_cells(campaign_id, grid.grid_cells)

    queued_jobs = []
    for cell in grid.grid_cells:
        queued_jobs.append(
            enqueue_discover_cell(
                campaign_id,
                cell.id,
                config_path=args.config,
            )
        )

    store.mark_campaign_status(campaign_id, "pending")
    print("=" * 60)
    print("Queued Google Maps Campaign")
    print("=" * 60)
    print(f"Campaign ID: {campaign_id}")
    print(f"Search term: {args.search}")
    print(f"Grid size: {args.grid}x{args.grid}")
    print(f"Cells queued: {len(queued_jobs)}")
    print(f"Scraping mode: {scraping_mode}")
    print(f"Review mode: {review_mode}")
    print(f"Extraction groups: {', '.join(settings.extraction.enabled_groups())}")
    if review_mode == "rolling_365d":
        print(f"Review window (days): {review_window_days}")
    print("=" * 60)
    return campaign_id


def export_durable_campaign(args: argparse.Namespace) -> None:
    """Export a durable campaign to CSV."""

    effective_config = build_effective_config(args)
    result_file = Path(getattr(args, "result_file", None) or effective_config.settings.files.result_filename)
    reviews_file = Path(getattr(args, "reviews_file", None) or effective_config.settings.files.reviews_filename)
    store = PostgresStore()
    business_csv, reviews_csv = store.export_campaign_csvs(
        args.campaign_id,
        business_csv=result_file,
        reviews_csv=reviews_file,
    )
    print("Campaign export completed")
    print(f"Business CSV: {business_csv}")
    print(f"Reviews CSV: {reviews_csv}")


def main():
    """Main entry point for the Google Maps scraper."""
    # Load environment variables from .env if present so API keys are available early.
    project_root = Path(__file__).resolve().parent
    load_dotenv(project_root / ".env", override=False)

    try:
        # Parse arguments
        args = parse_arguments()

        # Print configuration and exit if requested
        if getattr(args, "print_config", False):
            print_effective_config(args)
            return

        # Validate arguments for scraping / enrichment
        validate_arguments(args)

        if getattr(args, "migrate_db", False):
            migrate_postgres_schema()
            return

        if getattr(args, "worker", False):
            run_queue_worker(args)
            return

        if getattr(args, "scheduler", False) or getattr(args, "scheduler_once", False):
            run_scheduler(args)
            return

        if getattr(args, "runner_service", False) or getattr(args, "runner_once", False):
            run_runner_service(args)
            return

        if getattr(args, "export_campaign", False):
            export_durable_campaign(args)
            return

        if getattr(args, "owner_enrich_csv", None):
            run_owner_csv_enrichment(args)
            return

        if getattr(args, "queue_start", False):
            start_queued_campaign(args)
            return

        if getattr(args, "campaign_cities_file", None):
            bounds = parse_bounds(args.bounds) if args.bounds else None
            effective_config = build_effective_config(args)

            scraping_mode = args.scraping_mode or effective_config.settings.scraping.default_mode
            review_mode = args.review_mode or "rolling_365d"
            review_window_days = args.review_window_days or effective_config.settings.scraping.review_window_days
            headless = (
                args.headless
                if getattr(args, "headless", None) is not None
                else effective_config.settings.browser.headless
            )
            categories = [
                item.strip()
                for item in (args.campaign_categories or "").split(",")
                if item.strip()
            ]
            if not categories:
                raise ValueError("campaign_categories must contain at least one category")

            options = CityCampaignOptions(
                cities_file=args.campaign_cities_file,
                total_results_per_job=args.total,
                output_dir=args.campaign_output_dir,
                categories=categories,
                search_template=args.campaign_search_template,
                grid_size=args.grid,
                bounds=bounds,
                bounds_cache_path=args.campaign_bounds_cache,
                refresh_bounds=args.campaign_refresh_bounds,
                scraping_mode=scraping_mode,
                review_mode=review_mode,
                review_window_days=review_window_days,
                max_reviews=args.max_reviews,
                headless=headless,
                config_overrides={
                    "extraction": asdict(effective_config.settings.extraction),
                },
                smoke_test=args.campaign_smoke_test,
                smoke_cities=args.campaign_smoke_cities,
                smoke_categories=args.campaign_smoke_categories,
                resume=args.campaign_resume,
            )

            print("=" * 60)
            print("Google Maps City Campaign")
            print("=" * 60)
            print(f"Cities file: {options.cities_file}")
            print(f"Categories: {', '.join(options.categories)}")
            print(f"Per-job target results: {options.total_results_per_job}")
            print(f"Grid size: {options.grid_size}x{options.grid_size}")
            print(f"Scraping mode: {options.scraping_mode}")
            print(f"Review mode: {options.review_mode}")
            print(f"Extraction groups: {', '.join(effective_config.settings.extraction.enabled_groups())}")
            print(f"Review window (days): {options.review_window_days}")
            print(f"Bounds cache: {options.bounds_cache_path or '(disabled)'}")
            if options.refresh_bounds:
                print("Bounds refresh: yes")
            print(f"Headless: {'yes' if options.headless else 'no'}")
            print(f"Smoke test: {'yes' if options.smoke_test else 'no'}")
            if options.smoke_test:
                print(
                    f"Smoke slice: {options.smoke_cities} cities x {options.smoke_categories} categories"
                )
            if options.output_dir:
                print(f"Output directory: {options.output_dir}")
            print("=" * 60)

            result = run_city_campaign(options)

            print("\nCampaign completed")
            print(f"Output directory: {result.output_dir}")
            print(f"Business CSV: {result.business_csv}")
            print(f"Reviews CSV: {result.reviews_csv}")
            if result.summary_csv:
                print(f"Summary CSV: {result.summary_csv}")
            print(f"Manifest: {result.manifest_path}")
            print(
                f"Jobs completed: {result.completed_jobs}/{result.total_jobs}"
                + (f" | Failed: {result.failed_jobs}" if result.failed_jobs else "")
            )
            return

        if args.city:
            effective_config = build_effective_config(args)
            display_search_term, search_input_term, bounds = resolve_city_scrape_inputs(args)

            if getattr(args, "scraping_mode", None) is None:
                args.scraping_mode = effective_config.settings.scraping.default_mode
            if getattr(args, "review_mode", None) is None:
                args.review_mode = effective_config.settings.scraping.review_mode
            if getattr(args, "review_window_days", None) is None:
                args.review_window_days = effective_config.settings.scraping.review_window_days

            if args.cell_workers > 1:
                if args.scraping_mode != "coverage":
                    raise ValueError(
                        "--cell-workers > 1 is currently supported only with --scraping-mode coverage"
                    )

                result_path = Path(effective_config.settings.files.result_filename).expanduser()
                reviews_path = Path(effective_config.settings.files.reviews_filename).expanduser()
                final_result_path = result_path if result_path.is_absolute() else (Path.cwd() / result_path)
                final_reviews_path = (
                    reviews_path if reviews_path.is_absolute() else (Path.cwd() / reviews_path)
                )
                output_dir = str(final_result_path.parent.resolve())

                print("=" * 60)
                print("Google Maps Parallel City Scraper")
                print("=" * 60)
                print(f"City: {args.city}")
                print(f"Search query: {args.query}")
                print(f"Search label: {display_search_term}")
                print(f"Target results: {args.total}")
                print(f"Grid size: {args.grid}x{args.grid}")
                print(f"Scraping mode: {args.scraping_mode}")
                print(f"Review mode: {args.review_mode}")
                print(
                    "Extraction groups: "
                    + ", ".join(effective_config.settings.extraction.enabled_groups())
                )
                if args.review_mode == "rolling_365d":
                    print(f"Review window (days): {args.review_window_days}")
                print(f"Cell workers: {args.cell_workers}")
                print(f"Resolved bounds: {bounds}")
                print(f"Output directory: {output_dir}")
                print("=" * 60)

                result = run_city_cell_workers(
                    CityCellWorkerOptions(
                        city=args.city,
                        query=args.query,
                        display_search_term=display_search_term,
                        search_input_term=search_input_term,
                        bounds=bounds,
                        total_results=args.total,
                        grid_size=args.grid,
                        zoom_level=effective_config.settings.grid.default_zoom_level,
                        config_path=args.config,
                        scraping_mode=args.scraping_mode,
                        review_mode=args.review_mode,
                        review_window_days=args.review_window_days,
                        max_reviews=args.max_reviews,
                        headless=(
                            args.headless
                            if getattr(args, "headless", None) is not None
                            else effective_config.settings.browser.headless
                        ),
                        cell_workers=args.cell_workers,
                        output_dir=output_dir,
                        final_business_csv=str(final_result_path.resolve()),
                        final_reviews_csv=str(final_reviews_path.resolve()),
                        log_level=args.log_level,
                        extract=getattr(args, "extract", None),
                        skip_extract=getattr(args, "skip_extract", None),
                    )
                )

                print("\nParallel city scraping completed")
                print(f"Output directory: {result.output_dir}")
                print(f"Business CSV: {result.business_csv}")
                print(f"Reviews CSV: {result.reviews_csv}")
                if result.summary_csv:
                    print(f"Summary CSV: {result.summary_csv}")
                print(f"Manifest: {result.manifest_path}")
                print(
                    f"Cells completed: {result.completed_cells}/{result.total_cells}"
                    + (f" | Failed: {result.failed_cells}" if result.failed_cells else "")
                )
                return

            scraper = create_scraper_from_args(args)

            print("=" * 60)
            print("Google Maps City Scraper")
            print("=" * 60)
            print(f"City: {args.city}")
            print(f"Search query: {args.query}")
            print(f"Search label: {display_search_term}")
            print(f"Target results: {args.total}")
            print(f"Grid size: {args.grid}x{args.grid}")
            print(f"Scraping mode: {args.scraping_mode}")
            print(f"Cell workers: {args.cell_workers}")
            print(f"Review mode: {args.review_mode}")
            print(
                "Extraction groups: "
                + ", ".join(scraper.config.settings.extraction.enabled_groups())
            )
            if args.review_mode == "rolling_365d":
                print(f"Review window (days): {args.review_window_days}")
            print(f"Resolved bounds: {bounds}")
            print(f"City bounds cache: {args.city_bounds_cache}")
            if args.refresh_city_bounds:
                print("Bounds refresh: yes")
            print(f"Config file: {args.config}")
            print("=" * 60)

            scraper.run(
                search_term=display_search_term,
                search_input_term=search_input_term,
                total_results=args.total,
                bounds=bounds,
                grid_size=args.grid,
                scraping_mode=args.scraping_mode,
            )

            print("\nScraping completed successfully!")
            return

        # Parse bounds if provided
        bounds = None
        if args.bounds:
            bounds = parse_bounds(args.bounds)
        
        # Create and configure scraper
        scraper = create_scraper_from_args(args)

        # Resolve scraping mode: CLI override wins, otherwise use config default
        if getattr(args, "scraping_mode", None) is None:
            args.scraping_mode = scraper.config.settings.scraping.default_mode
        if args.scraping_mode not in ("fast", "coverage"):
            raise ValueError(
                f"Invalid scraping mode '{args.scraping_mode}'. Must be 'fast' or 'coverage'."
            )
        if getattr(args, "review_mode", None) is None:
            args.review_mode = scraper.config.settings.scraping.review_mode
        if getattr(args, "review_window_days", None) is None:
            args.review_window_days = scraper.config.settings.scraping.review_window_days
        if args.review_mode not in ("all_available", "rolling_365d"):
            raise ValueError(
                f"Invalid review mode '{args.review_mode}'. Must be 'all_available' or 'rolling_365d'."
            )
        
        # Display startup information
        print("="*60)
        print("Google Maps Scraper v2.0")
        print("="*60)
        print(f"Search term: {args.search}")
        print(f"Target results: {args.total}")
        print(f"Grid size: {args.grid}x{args.grid}")
        print(f"Scraping mode: {args.scraping_mode}")
        print(f"Review mode: {args.review_mode}")
        print(
            "Extraction groups: "
            + ", ".join(scraper.config.settings.extraction.enabled_groups())
        )
        if args.review_mode == "rolling_365d":
            print(f"Review window (days): {args.review_window_days}")
        if bounds:
            print(f"Bounds: {bounds}")
        print(f"Config file: {args.config}")
        owner_settings = scraper.config.settings.owner_enrichment
        owner_status = "enabled" if owner_settings.enabled else "disabled"
        print(f"Owner enrichment: {owner_status}")
        if owner_settings.enabled:
            print(f"Owner model: {owner_settings.openrouter_default_model}")
        print("="*60)

        # Run the scraper
        scraper.run(
            search_term=args.search,
            search_input_term=getattr(args, "search_input", None),
            total_results=args.total,
            bounds=bounds,
            grid_size=args.grid,
            scraping_mode=args.scraping_mode,
            selected_cell_ids=getattr(args, "cell_id", None),
        )
        
        print("\nScraping completed successfully!")
        
    except KeyboardInterrupt:
        print("\nScraping interrupted by user.")
        sys.exit(1)
        
    except ScraperException as e:
        print(f"\nScraping failed: {e}")
        sys.exit(1)
        
    except ValueError as e:
        print(f"\nInvalid arguments: {e}")
        print("\nUse --help for usage information.")
        sys.exit(1)
        
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        print("Please check the logs for more details.")
        sys.exit(1)


if __name__ == "__main__":
    main()
