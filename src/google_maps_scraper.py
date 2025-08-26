"""Main Google Maps scraper orchestrator."""

from typing import Tuple, Optional, List
from playwright.sync_api import sync_playwright, Browser, Page
import argparse
import time

from .config import Config, Selectors
from .models import Business, Review
from .scraper import BusinessScraper, ReviewScraper
from .navigation import GridNavigator, PageNavigator
from .persistence import CSVWriter, ProgressTracker
from .utils import setup_logging, ScraperException, NavigationException, ExtractionException
from .utils.logger import get_component_logger, ScraperLoggerAdapter, log_scraping_progress
from .utils.review_analyzer import analyze_reviews


class GoogleMapsScraper:
    """Main scraper orchestrator that coordinates all components."""
    
    def __init__(self, config: Config):
        """Initialize the Google Maps scraper.
        
        Args:
            config: Configuration instance
        """
        self.config = config
        self.selectors = Selectors()
        self.logger = setup_logging()
        self.component_logger = get_component_logger('Orchestrator')
        
        # Initialize components
        self.csv_writer = CSVWriter(
            self.config.settings.files.result_filename,
            self.config.settings.files.reviews_filename
        )
        self.progress_tracker = ProgressTracker(
            self.config.settings.files.progress_filename
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
        self.browser = playwright.chromium.launch(
            executable_path=self.config.settings.browser.executable_path,
            headless=self.config.settings.browser.headless
        )
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
        
        # Collect listing URLs
        seen_urls = progress.get_seen_urls_set()
        listing_urls = self.page_navigator.collect_listing_urls(seen_urls)
        
        if not listing_urls:
            cell_logger.warning(f"No listing URLs collected from cell {cell.id}")
            return
        
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
    
    return GoogleMapsScraper(config)