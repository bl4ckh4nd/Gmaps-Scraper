"""Main Google Maps scraper orchestrator."""

import os
import subprocess
import sys
from typing import Tuple, Optional, List
from playwright.sync_api import sync_playwright, Browser, Page
import argparse
import time

from .config import Config, Selectors
from .models import Business, OwnerDetails, Review
from .scraper import BusinessScraper, ReviewScraper
from .navigation import GridNavigator, PageNavigator
from .persistence import CSVWriter, ProgressTracker
from .utils import (
    setup_logging,
    ScraperException,
    NavigationException,
    ExtractionException,
)
from .utils.owner_enrichment_service import OwnerEnrichmentService
from .utils.logger import get_component_logger, ScraperLoggerAdapter, log_scraping_progress
from .utils import resolve_chrome_binary
from .utils.review_analyzer import analyze_reviews
from .utils.helpers import extract_place_id


class GoogleMapsScraper:
    """Main scraper orchestrator that coordinates all components."""
    
    def __init__(
        self,
        config: Config,
        log_level: Optional[str] = None,
        log_file: Optional[str] = None,
        configure_root_logger: bool = True,
    ):
        """Initialize the Google Maps scraper.
        
        Args:
            config: Configuration instance
        """
        self.config = config
        self.selectors = Selectors()
        # Configure logging using config-provided format and optional overrides
        files_settings = self.config.settings.files
        effective_level = log_level or "INFO"
        self.logger = setup_logging(
            log_level=effective_level,
            log_file=log_file,
            log_format=files_settings.log_format,
            configure_root=configure_root_logger,
        )
        self.component_logger = get_component_logger('Orchestrator')
        
        # Initialize components
        self.csv_writer = CSVWriter(
            self.config.settings.files.result_filename,
            self.config.settings.files.reviews_filename
        )
        self.progress_tracker = ProgressTracker(
            self.config.settings.files.progress_filename
        )
        self.owner_enrichment_service = OwnerEnrichmentService(
            self.config.settings.owner_enrichment,
            logger=get_component_logger('OwnerEnrichment'),
        )

        # Browser-dependent components (initialized in run)
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None
        self.business_scraper: Optional[BusinessScraper] = None
        self.review_scraper: Optional[ReviewScraper] = None
        self.page_navigator: Optional[PageNavigator] = None
    
    def run(self, search_term: str, total_results: int, 
            bounds: Optional[Tuple[float, float, float, float]] = None,
            grid_size: Optional[int] = None,
            scraping_mode: str = 'fast') -> None:
        """Run the scraping process.
        
        Args:
            search_term: Search query
            total_results: Target number of results
            bounds: Geographic bounds, uses default if None
            grid_size: Grid size, uses default if None
            scraping_mode: Scraping mode ('fast' or 'coverage')
        """
        # Set defaults
        bounds = bounds or self.config.settings.grid.default_bounds
        grid_size = grid_size or self.config.settings.grid.default_grid_size
        
        # Initialize progress tracking
        progress = self.progress_tracker.initialize_job(
            search_term, bounds, grid_size, total_results, scraping_mode
        )
        
        # Create context logger
        context_logger = ScraperLoggerAdapter(
            self.component_logger, search_term
        )
        
        context_logger.info(f"Starting scrape: '{search_term}', "
                          f"target: {total_results}, "
                          f"grid: {grid_size}x{grid_size}, "
                          f"mode: {scraping_mode}")
        
        # Initialize grid navigator
        grid_navigator = GridNavigator(bounds, grid_size, 
                                     self.config.settings.grid.default_zoom_level)
        
        if not grid_navigator.validate_bounds():
            raise ScraperException("Invalid geographic bounds provided")
        
        # Log grid information
        grid_info = grid_navigator.get_progress_info(progress.completed_cells)
        context_logger.info(f"Grid covers {grid_info['total_area_km2']:.1f} km² "
                          f"in {grid_info['total_cells']} cells")
        
        try:
            with sync_playwright() as p:
                self._initialize_browser_components(p)
                self._process_grid_cells(grid_navigator, progress, search_term, 
                                       total_results, context_logger, scraping_mode)
                
        except Exception as e:
            context_logger.error(f"Scraping failed: {e}")
            raise
        finally:
            self._cleanup_browser()
            self._finalize_results(context_logger)
    
    def _initialize_browser_components(self, playwright) -> None:
        """Initialize browser and related components."""
        explicit_path = self.config.settings.browser.executable_path
        resolved_path = resolve_chrome_binary(explicit_path)

        launch_kwargs = {
            "headless": self.config.settings.browser.headless,
        }
        if resolved_path:
            launch_kwargs["executable_path"] = resolved_path
            self.logger.info("Using Chrome executable at %s", resolved_path)
        elif explicit_path:
            raise ScraperException(
                f"Configured Chrome executable not found: {explicit_path}."
            )
        else:
            self.logger.info("No Chrome path provided; using Playwright managed Chromium")

        self.browser = playwright.chromium.launch(**launch_kwargs)
        self.page = self.browser.new_page()
        
        # Initialize browser-dependent components
        self.business_scraper = BusinessScraper(
            self.page, self.config.settings, self.selectors
        )
        self.review_scraper = ReviewScraper(
            self.page, self.config.settings, self.selectors
        )
        self.page_navigator = PageNavigator(
            self.page, self.config.settings, self.selectors
        )

    def _resolve_browser_executable_path(self) -> Optional[str]:
        """Resolve a usable configured browser executable path, or None for Playwright default."""
        configured_path = (self.config.settings.browser.executable_path or "").strip()

        if configured_path and os.path.isfile(configured_path):
            self.component_logger.debug(f"Using configured browser executable: {configured_path}")
            return configured_path

        if configured_path:
            self.component_logger.warning(
                "Configured browser executable not found: %s. Falling back to Playwright default.",
                configured_path,
            )

        self.component_logger.info("Using Playwright-managed Chromium executable.")
        return None

    def _launch_browser_with_fallback(self, playwright) -> Browser:
        """Launch browser with robust fallback across OS/browser binaries."""
        base_options = {
            "headless": self.config.settings.browser.headless,
        }
        resolved_executable = self._resolve_browser_executable_path()

        attempts = []
        if resolved_executable:
            attempts.append(("configured_or_detected", {**base_options, "executable_path": resolved_executable}))
        attempts.append(("playwright_default", dict(base_options)))

        last_error: Optional[Exception] = None
        for label, options in attempts:
            try:
                if label == "playwright_default":
                    self.component_logger.info("Launching with Playwright-managed Chromium.")
                return playwright.chromium.launch(**options)
            except Exception as exc:
                last_error = exc
                # If bundled browser is missing, try installing once and retry this attempt.
                if label == "playwright_default" and self._is_missing_browser_error(exc):
                    self.component_logger.warning(
                        "Playwright Chromium missing. Attempting automatic install and retry."
                    )
                    self._install_playwright_chromium()
                    return playwright.chromium.launch(**options)

                if label == "configured_or_detected":
                    self.component_logger.warning(
                        "Failed to launch browser using executable %s: %s. Falling back to Playwright default.",
                        options.get("executable_path"),
                        exc,
                    )
                else:
                    self.component_logger.error("Failed to launch browser using Playwright default: %s", exc)

        if last_error:
            raise last_error
        raise RuntimeError("Browser launch failed without a captured error")

    @staticmethod
    def _is_missing_browser_error(exc: Exception) -> bool:
        message = str(exc).lower()
        return (
            "executable doesn't exist" in message
            or "please run the following command" in message
            or "browser has not been found" in message
        )

    def _install_playwright_chromium(self) -> None:
        cmd = [sys.executable, "-m", "playwright", "install", "chromium"]
        completed = subprocess.run(cmd, capture_output=True, text=True)
        if completed.returncode != 0:
            stderr_tail = (completed.stderr or "").strip()[-400:]
            stdout_tail = (completed.stdout or "").strip()[-400:]
            raise RuntimeError(
                "Automatic Playwright Chromium install failed. "
                f"stdout: {stdout_tail!r} stderr: {stderr_tail!r}"
            )
        self.component_logger.info("Playwright Chromium installed successfully.")
    
    def _process_grid_cells(self, grid_navigator: GridNavigator, 
                          progress, search_term: str, total_results: int,
                          context_logger: ScraperLoggerAdapter,
                          scraping_mode: str = 'fast') -> None:
        """Process all grid cells for the search."""
        
        # Calculate per-cell limits for coverage mode
        total_cells = len(grid_navigator.grid_cells)
        if scraping_mode == 'coverage':
            import math
            results_per_cell = math.ceil(total_results / total_cells)
            context_logger.info(f"Coverage mode: targeting {results_per_cell} results per cell across {total_cells} cells")
        
        for cell in grid_navigator.grid_cells:
            # Skip completed cells
            if progress.is_cell_completed(cell.id):
                context_logger.info(f"Skipping completed cell {cell.id}")
                continue
            
            # Fast mode: stop if target reached (before processing cell)
            if scraping_mode == 'fast' and progress.results_count >= total_results:
                context_logger.info(f"Fast mode: Reached target of {total_results} results")
                break
            
            # Calculate cell-specific target
            if scraping_mode == 'coverage':
                cell_target = min(
                    results_per_cell,  # Cell's fair share
                    total_results - progress.results_count  # Remaining to collect
                )
            else:  # fast mode
                cell_target = total_results - progress.results_count
            
            if cell_target <= 0:
                context_logger.info(f"Cell {cell.id}: No more results needed")
                continue
                
            # Update context logger with current cell
            cell_logger = ScraperLoggerAdapter(
                self.component_logger, search_term, cell.id
            )
            
            try:
                self._process_single_cell(
                    cell, search_term, cell_target,
                    progress, cell_logger, scraping_mode
                )
                
                # Mark cell as completed
                self.progress_tracker.mark_cell_completed(cell.id)
                
            except Exception as e:
                cell_logger.error(f"Error processing cell {cell.id}: {e}")
                # Continue to next cell on error
                continue
    
    def _process_single_cell(self, cell, search_term: str, cell_target: int,
                           progress, cell_logger: ScraperLoggerAdapter,
                           scraping_mode: str = 'fast') -> None:
        """Process a single grid cell."""
        
        cell_logger.info(f"Processing grid cell {cell.id} (target: {cell_target} results)...")
        
        # Navigate to grid cell
        if not self.page_navigator.navigate_to_grid_cell(cell):
            raise NavigationException(f"Failed to navigate to cell {cell.id}")
        
        # Perform search
        if not self.page_navigator.perform_search(search_term):
            raise NavigationException(f"Search failed in cell {cell.id}")
        
        # Wait for search results
        if not self.page_navigator.wait_for_search_results():
            cell_logger.warning(f"No search results in cell {cell.id}")
            return
        
        # Scroll to load more listings
        max_per_cell = self.config.settings.scraping.max_listings_per_cell
        
        # Use cell_target instead of calculating from total_results
        remaining_target = min(max_per_cell, cell_target)
        
        listing_count = self.page_navigator.scroll_for_listings(remaining_target)
        cell_logger.info(f"Found {listing_count} listings in cell {cell.id}")
        
        # Collect listing URLs, avoiding place IDs we've already seen
        seen_place_ids = progress.get_seen_urls_set()
        listing_urls = self.page_navigator.collect_listing_urls(seen_place_ids)
        
        if not listing_urls:
            cell_logger.warning(f"No listing URLs collected from cell {cell.id}")
            return

        # Persist newly seen place IDs so resumed jobs can skip them
        existing_ids = set(progress.seen_urls)
        new_place_ids: List[str] = []
        for url in listing_urls:
            place_id = extract_place_id(url)
            if place_id and place_id not in existing_ids:
                existing_ids.add(place_id)
                new_place_ids.append(place_id)

        if new_place_ids:
            self.progress_tracker.update_progress(seen_urls=new_place_ids)
        
        # Process each listing
        processed_count = 0
        cell_start_count = progress.results_count
        
        for idx, url in enumerate(listing_urls):
            # Check if we've reached cell target
            if processed_count >= cell_target:
                cell_logger.info(f"Reached cell target of {cell_target} results")
                break
            
            if processed_count >= max_per_cell:
                cell_logger.info(f"Reached max listings per cell ({max_per_cell})")
                break
            
            try:
                if self._process_single_listing(url, cell_logger):
                    processed_count += 1
                    progress.results_count = self.progress_tracker.increment_results_count()
                    
                    # Log progress (use current global total for logging)
                    log_scraping_progress(cell_logger, progress.results_count, 
                                        progress.total_target, "listings")
                
            except Exception as e:
                cell_logger.error(f"Error processing listing {idx+1}: {e}")
                continue
        
        # Track results per cell
        if processed_count > 0:
            self.progress_tracker.add_cell_results(cell.id, processed_count)
        
        cell_logger.info(f"Completed cell {cell.id}: processed {processed_count} listings")
    
    def _process_single_listing(self, url: str, logger) -> bool:
        """Process a single business listing.
        
        Args:
            url: Business listing URL
            logger: Logger instance
            
        Returns:
            True if listing was processed successfully
        """
        try:
            # Navigate to business
            if not self.page_navigator.navigate_to_business(url):
                logger.warning(f"Failed to navigate to business: {url[:50]}...")
                return False
            
            # Extract business data
            business = self.business_scraper.extract_data(url)
            
            if not business.name or business.name == "Extraction Failed":
                logger.warning(f"Failed to extract business data from: {url[:50]}...")
                return False
            
            # Extract reviews if enabled and business has reviews
            reviews = []
            if business.review_count > 0:
                max_reviews = min(business.review_count, 
                                self.config.settings.scraping.max_reviews_per_business)
                
                reviews = self.review_scraper.extract_data(
                    business.name, business.address, business.place_id,
                    business.review_count, max_reviews
                )
                
                if reviews:
                    # Analyze reviews to calculate metrics
                    try:
                        review_metrics = analyze_reviews(reviews)

                        # Update business with review metrics
                        business.reply_rate_good = review_metrics['reply_rate_good']
                        business.reply_rate_bad = review_metrics['reply_rate_bad'] 
                        business.avg_time_between_reviews = review_metrics['avg_time_between_reviews']

                        logger.debug(f"Review metrics: good_reply={review_metrics['reply_rate_good']:.1f}%, "
                                   f"bad_reply={review_metrics['reply_rate_bad']:.1f}%, "
                                   f"avg_days={review_metrics['avg_time_between_reviews']}")

                    except Exception as e:
                        logger.warning(f"Failed to analyze reviews for {business.name}: {e}")

                    # Write reviews to CSV
                    self.csv_writer.write_reviews(reviews)

            if self.owner_enrichment_service.is_enabled():
                try:
                    owner_details = self.owner_enrichment_service.enrich_business(business)
                    business.owner_details = owner_details
                    logger.debug(
                        "Owner enrichment for %s resulted in status %s",
                        business.name,
                        owner_details.status,
                    )
                except Exception as exc:
                    business.owner_details = OwnerDetails.from_response(
                        None,
                        status="error",
                        reason=str(exc),
                    )
                    logger.warning(
                        "Owner enrichment failed for %s: %s",
                        business.name,
                        exc,
                    )

            # Write business data (now with review metrics)
            if not self.csv_writer.write_business(business):
                logger.debug(f"Duplicate business skipped: {business.name}")
                return False  # Was a duplicate
            
            logger.info(f"Successfully processed: {business.name}")
            return True
            
        except Exception as e:
            logger.error(f"Error processing listing {url[:50]}...: {e}")
            return False
    
    def _cleanup_browser(self) -> None:
        """Clean up browser resources."""
        if self.browser:
            try:
                self.browser.close()
            except Exception as e:
                self.component_logger.error(f"Error closing browser: {e}")
    
    def _finalize_results(self, logger) -> None:
        """Finalize and clean up results."""
        try:
            # Deduplicate business data
            duplicates_removed = self.csv_writer.deduplicate_business_data()
            if duplicates_removed > 0:
                logger.info(f"Removed {duplicates_removed} duplicate business entries")
            
            # Log final statistics
            business_count = self.csv_writer.get_business_count()
            review_count = self.csv_writer.get_review_count()
            
            logger.info(f"Scraping completed!")
            logger.info(f"Final results: {business_count} businesses, {review_count} reviews")
            
        except Exception as e:
            logger.error(f"Error finalizing results: {e}")


def create_scraper_from_args(args: argparse.Namespace) -> GoogleMapsScraper:
    """Create scraper instance from command line arguments.
    
    Args:
        args: Parsed command line arguments
        
    Returns:
        Configured GoogleMapsScraper instance
    """
    # Load configuration
    config_path = getattr(args, 'config', 'config.yaml')
    
    try:
        config = Config.from_file(config_path)
    except Exception:
        # Fall back to default config
        config = Config()
    
    # Override config with command line arguments if provided
    if hasattr(args, 'headless') and args.headless is not None:
        config.settings.browser.headless = args.headless
    
    if hasattr(args, 'max_reviews') and args.max_reviews:
        config.settings.scraping.max_reviews_per_business = args.max_reviews

    if getattr(args, 'owner_enrichment', False):
        config.settings.owner_enrichment.enabled = True

    if getattr(args, 'owner_model', None):
        config.settings.owner_enrichment.openrouter_default_model = args.owner_model

    if getattr(args, 'owner_max_pages', None):
        config.settings.owner_enrichment.max_pages = args.owner_max_pages

    log_level = getattr(args, "log_level", None)

    return GoogleMapsScraper(config, log_level=log_level)
