"""
Google Maps Scraper - Entry Point

A refactored, maintainable Google Maps scraper with modular architecture.
Extracts business information and reviews from Google Maps using geographic grid search.
"""

import argparse
import sys
from pathlib import Path
from typing import Dict, Tuple

from src.config import Config
from src.google_maps_scraper import GoogleMapsScraper, create_scraper_from_args
from src.services import OwnerCSVEnricher, OwnerCSVEnrichmentOptions
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
        type=bool, 
        help="Run browser in headless mode (overrides config)"
    )
    parser.add_argument(
        "--max-reviews", 
        type=int, 
        help="Maximum reviews per business (overrides config)"
    )
    parser.add_argument(
        "--scraping-mode", 
        type=str,
        choices=['fast', 'coverage'], 
        default='fast',
        help="Scraping mode: 'fast' (sequential) or 'coverage' (distributed) (default: fast)"
    )
    parser.add_argument(
        "--log-level", 
        type=str, 
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help="Logging level (default: INFO)"
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
    parser.set_defaults(owner_skip_existing=True)
    parser.add_argument(
        "--owner-no-skip-existing",
        dest="owner_skip_existing",
        action="store_false",
        help="Re-enrich rows even if they already contain owner information"
    )

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
    if getattr(args, 'owner_enrich_csv', None):
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
    
    # Validate max reviews if provided
    if args.max_reviews is not None and args.max_reviews < 0:
        raise ValueError("Max reviews cannot be negative")

    if getattr(args, 'owner_max_pages', None) is not None and args.owner_max_pages <= 0:
        raise ValueError("Owner max pages must be positive")


def run_owner_csv_enrichment(args: argparse.Namespace) -> None:
    """Run owner enrichment on an existing CSV file."""

    csv_path = Path(args.owner_enrich_csv).expanduser()
    if not csv_path.exists():
        raise FileNotFoundError(f"Owner enrichment CSV not found: {csv_path}")

    config_path = getattr(args, 'config', 'config.yaml')
    try:
        config = Config.from_file(config_path)
    except Exception:
        config = Config()

    output_path = Path(args.owner_output).expanduser() if args.owner_output else None
    if args.owner_in_place and output_path and output_path != csv_path:
        raise ValueError("--owner-in-place cannot be combined with --owner-output")

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
        total = stats.get("total_rows", 0) or total_rows
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


def main():
    """Main entry point for the Google Maps scraper."""
    # Load environment variables from .env if present so API keys are available early.
    project_root = Path(__file__).resolve().parent
    load_dotenv(project_root / ".env", override=False)

    try:
        # Parse and validate arguments
        args = parse_arguments()
        validate_arguments(args)

        if args.owner_enrich_csv:
            run_owner_csv_enrichment(args)
            return

        # Parse bounds if provided
        bounds = None
        if args.bounds:
            bounds = parse_bounds(args.bounds)
        
        # Create and configure scraper
        scraper = create_scraper_from_args(args)
        
        # Display startup information
        print("="*60)
        print("Google Maps Scraper v2.0")
        print("="*60)
        print(f"Search term: {args.search}")
        print(f"Target results: {args.total}")
        print(f"Grid size: {args.grid}x{args.grid}")
        print(f"Scraping mode: {args.scraping_mode}")
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
            total_results=args.total,
            bounds=bounds,
            grid_size=args.grid,
            scraping_mode=args.scraping_mode
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
