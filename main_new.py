"""
Google Maps Scraper - Entry Point

A refactored, maintainable Google Maps scraper with modular architecture.
Extracts business information and reviews from Google Maps using geographic grid search.
"""

import argparse
import sys
from typing import Tuple

from src.google_maps_scraper import GoogleMapsScraper, create_scraper_from_args
from src.utils import ScraperException


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
    
    # Required arguments
    parser.add_argument(
        "-s", "--search", 
        type=str, 
        required=True,
        help="Search term (required)"
    )
    parser.add_argument(
        "-t", "--total", 
        type=int, 
        required=True,
        help="Total number of results to collect (required)"
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
    # Validate search term
    if not args.search or not args.search.strip():
        raise ValueError("Search term cannot be empty")
    
    # Validate total results
    if args.total <= 0:
        raise ValueError("Total results must be positive")
    if args.total > 10000:
        print("Warning: Large result counts may take a very long time")
    
    # Validate grid size
    if args.grid < 1 or args.grid > 10:
        raise ValueError("Grid size must be between 1 and 10")
    
    # Validate max reviews if provided
    if args.max_reviews is not None and args.max_reviews < 0:
        raise ValueError("Max reviews cannot be negative")


def main():
    """Main entry point for the Google Maps scraper."""
    try:
        # Parse and validate arguments
        args = parse_arguments()
        validate_arguments(args)
        
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