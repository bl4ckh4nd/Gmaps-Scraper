"""Main Google Maps scraper orchestrator."""

import os
import subprocess
import sys
from pathlib import Path
from typing import Callable, Tuple, Optional, List
from playwright.sync_api import sync_playwright, Browser, BrowserContext, Page
import argparse
import time

from .config import Config, Selectors, apply_argument_overrides
from .models import Business, OwnerDetails, Review
from .scraper import BusinessScraper, ReviewScraper
from .navigation import GridNavigator, PageNavigator
from .persistence import CSVWriter, ProgressTracker, ReviewHashIndex
from .services import CategoryReportService
from .utils import (
    setup_logging,
    CliProgressPrinter,
    ScraperException,
    NavigationException,
    ExtractionException,
)
from .utils.owner_enrichment_service import OwnerEnrichmentService
from .utils.logger import get_component_logger, ScraperLoggerAdapter, log_scraping_progress
from .utils import resolve_chrome_binary
from .utils.review_analyzer import analyze_reviews
from .utils.helpers import extract_place_id
from .utils.website_quality import assess_website_quality


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
        self.review_hash_index = ReviewHashIndex(
            self.config.settings.files.reviews_filename
        )
        self.progress_tracker = ProgressTracker(
            self.config.settings.files.progress_filename
        )
        self.owner_enrichment_service = OwnerEnrichmentService(
            self.config.settings.owner_enrichment,
            logger=get_component_logger('OwnerEnrichment'),
        )
        self.category_report_service = CategoryReportService(
            logger=get_component_logger('CategoryReport')
        )
        self.progress_printer = CliProgressPrinter()

        # Browser-dependent components (initialized in run)
        self.browser: Optional[Browser] = None
        self.browser_context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self.business_scraper: Optional[BusinessScraper] = None
        self.review_scraper: Optional[ReviewScraper] = None
        self.page_navigator: Optional[PageNavigator] = None
        self._should_cancel_callback: Optional[Callable[[], bool]] = None
        self._current_search_term: str = ""
        self._playwright = None
        self._active_browser_headless: Optional[bool] = None
        self._run_failed: bool = False
        self._run_failure_reason: Optional[str] = None
        self._browser_state_file = self._resolve_browser_state_file()
    
    def run(self, search_term: str, total_results: int, 
            bounds: Optional[Tuple[float, float, float, float]] = None,
            grid_size: Optional[int] = None,
            scraping_mode: str = 'fast',
            search_input_term: Optional[str] = None,
            selected_cell_ids: Optional[List[str]] = None,
            should_cancel: Optional[Callable[[], bool]] = None) -> None:
        """Run the scraping process.
        
        Args:
            search_term: Search label used for logs, progress, and CSV metadata
            total_results: Target number of results
            bounds: Geographic bounds, uses default if None
            grid_size: Grid size, uses default if None
            scraping_mode: Scraping mode ('fast' or 'coverage')
            search_input_term: Actual query typed into Google Maps (defaults to search_term)
            selected_cell_ids: Optional subset of grid cell IDs to process
            should_cancel: Optional callback returning True when the run should stop
        """
        self._should_cancel_callback = should_cancel
        self._current_search_term = search_term
        search_input_term = search_input_term or search_term
        # Set defaults
        bounds = bounds or self.config.settings.grid.default_bounds
        grid_size = grid_size or self.config.settings.grid.default_grid_size
        
        # Initialize progress tracking
        progress = self.progress_tracker.initialize_job(
            search_term,
            bounds,
            grid_size,
            total_results,
            scraping_mode,
            self.config.settings.scraping.review_mode,
        )
        
        # Create context logger
        context_logger = ScraperLoggerAdapter(
            self.component_logger, search_term
        )
        
        context_logger.info(f"Starting scrape: '{search_term}', "
                          f"target: {total_results}, "
                          f"grid: {grid_size}x{grid_size}, "
                          f"mode: {scraping_mode}")
        self._run_failed = False
        self._run_failure_reason = None
        
        # Initialize grid navigator
        grid_navigator = GridNavigator(bounds, grid_size, 
                                     self.config.settings.grid.default_zoom_level)
        
        if not grid_navigator.validate_bounds():
            raise ScraperException("Invalid geographic bounds provided")
        
        # Log grid information
        grid_info = grid_navigator.get_progress_info(progress.completed_cells)
        context_logger.info(f"Grid covers {grid_info['total_area_km2']:.1f} km² "
                          f"in {grid_info['total_cells']} cells")

        self.progress_printer.print_run_header(
            search_term=search_term,
            grid_size=grid_size,
            total_cells=grid_info['total_cells'],
            area_km2=grid_info['total_area_km2'],
            total_target=total_results,
            scraping_mode=scraping_mode,
            bounds=bounds,
            result_file=self.config.settings.files.result_filename,
        )
        
        try:
            with sync_playwright() as p:
                self._playwright = p
                self._initialize_browser_components(p)
                self._process_grid_cells(grid_navigator, progress, search_term,
                                        search_input_term,
                                       total_results, context_logger, scraping_mode,
                                       selected_cell_ids=selected_cell_ids)
                
        except Exception as e:
            self._run_failed = True
            self._run_failure_reason = str(e)
            context_logger.error(f"Scraping failed: {e}")
            raise
        finally:
            self._playwright = None
            self._should_cancel_callback = None
            self._cleanup_browser()
            self._finalize_results(context_logger)

    def _check_cancelled(self) -> None:
        """Raise if an external cancellation request was received."""
        if self._should_cancel_callback and self._should_cancel_callback():
            raise ScraperException("Scraping job cancelled")
    
    def _initialize_browser_components(self, playwright=None, headless_override: Optional[bool] = None) -> None:
        """Initialize browser and related components."""
        playwright = playwright or self._playwright
        if playwright is None:
            raise ScraperException("Playwright context is not initialized.")

        explicit_path = self.config.settings.browser.executable_path
        resolved_path = resolve_chrome_binary(explicit_path)

        launch_kwargs = {
            "headless": (
                self.config.settings.browser.headless
                if headless_override is None
                else headless_override
            ),
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
        context_kwargs = {}
        if (
            self._browser_state_file is not None
            and self._browser_state_file.exists()
            and self._browser_state_file.stat().st_size > 0
        ):
            context_kwargs["storage_state"] = str(self._browser_state_file)
            self.component_logger.info(
                "Loading browser session state from %s",
                self._browser_state_file,
            )
        try:
            self.browser_context = self.browser.new_context(**context_kwargs)
        except Exception as exc:
            if "storage_state" not in context_kwargs:
                raise
            self.component_logger.warning(
                "Failed to load browser session state from %s: %s. Starting with a fresh context.",
                self._browser_state_file,
                exc,
            )
            self.browser_context = self.browser.new_context()
        self.page = self.browser_context.new_page()
        self._active_browser_headless = launch_kwargs["headless"]
        
        # Initialize browser-dependent components
        self.business_scraper = BusinessScraper(
            self.page, self.config.settings, self.selectors
        )
        self.review_scraper = ReviewScraper(
            self.page, self.config.settings, self.selectors
        )
        self.page_navigator = PageNavigator(
            self.page,
            self.config.settings,
            self.selectors,
            persist_session_state_callback=self._persist_browser_session_state,
        )

    def _resolve_browser_state_file(self) -> Path:
        configured_path = (self.config.settings.browser.session_state_file or "").strip()
        if configured_path:
            path = Path(configured_path).expanduser()
        else:
            progress_path = Path(self.config.settings.files.progress_filename).expanduser()
            path = progress_path.with_name(f"{progress_path.stem}_browser_state.json")
        path.parent.mkdir(parents=True, exist_ok=True)
        return path.resolve()

    def _persist_browser_session_state(self) -> None:
        if self.browser_context is None or self._browser_state_file is None:
            return

        try:
            self.browser_context.storage_state(path=str(self._browser_state_file))
            self.component_logger.debug(
                "Saved browser session state to %s",
                self._browser_state_file,
            )
        except Exception as exc:
            self.component_logger.warning(
                "Failed to save browser session state to %s: %s",
                self._browser_state_file,
                exc,
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

    def _relaunch_browser_components(self, headless: bool) -> None:
        if self._playwright is None:
            raise ScraperException("Cannot relaunch browser without an active Playwright context.")

        self._cleanup_browser()
        self._initialize_browser_components(headless_override=headless)

    @staticmethod
    def _is_browser_closed_error(exc: Exception) -> bool:
        message = str(exc).lower()
        return any(
            fragment in message
            for fragment in (
                "target page, context or browser has been closed",
                "browser has been closed",
                "target closed",
                "page has been closed",
                "context has been closed",
            )
        )

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
                          progress, search_term: str, search_input_term: str, total_results: int,
                          context_logger: ScraperLoggerAdapter,
                          scraping_mode: str = 'fast',
                          selected_cell_ids: Optional[List[str]] = None) -> None:
        """Process all grid cells for the search."""
        
        # Calculate per-cell limits for coverage mode
        all_cells = list(grid_navigator.grid_cells)
        total_cells = len(all_cells)
        selected_cell_ids_set = set(selected_cell_ids or [])
        if selected_cell_ids_set:
            available_cell_ids = {cell.id for cell in all_cells}
            unknown_cell_ids = sorted(selected_cell_ids_set - available_cell_ids)
            if unknown_cell_ids:
                raise ScraperException(
                    f"Unknown grid cell IDs requested: {', '.join(unknown_cell_ids)}"
                )
            cells_to_process = [cell for cell in all_cells if cell.id in selected_cell_ids_set]
            context_logger.info(
                "Restricted run to %s selected grid cells: %s",
                len(cells_to_process),
                ", ".join(cell.id for cell in cells_to_process),
            )
        else:
            cells_to_process = all_cells

        if scraping_mode == 'coverage':
            import math
            results_per_cell = math.ceil(total_results / total_cells)
            context_logger.info(f"Coverage mode: targeting {results_per_cell} results per cell across {total_cells} cells")

        display_total_cells = len(cells_to_process)
        cell_display_idx = 0

        for cell in cells_to_process:
            self._check_cancelled()
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

            cell_display_idx += 1
                
            # Update context logger with current cell
            cell_logger = ScraperLoggerAdapter(
                self.component_logger, search_term, cell.id
            )

            browser_retry_attempted = False
            while True:
                try:
                    self._process_single_cell(
                        cell, search_term, search_input_term, cell_target,
                        progress, cell_logger, scraping_mode,
                        cell_idx=cell_display_idx,
                        total_cells=display_total_cells,
                    )

                    # Mark cell as completed
                    self.progress_tracker.mark_cell_completed(cell.id)
                    break

                except Exception as e:
                    if self._is_browser_closed_error(e) and not browser_retry_attempted:
                        browser_retry_attempted = True
                        cell_logger.warning(
                            f"Browser/page closed while processing cell {cell.id}; "
                            "relaunching the browser and retrying this cell once."
                        )
                        self._relaunch_browser_components(
                            headless=self.config.settings.browser.headless
                        )
                        continue

                    cell_logger.error(f"Error processing cell {cell.id}: {e}")
                    if self._is_browser_closed_error(e):
                        raise ScraperException(
                            f"Browser/page closed while processing cell {cell.id}: {e}"
                        ) from e

                    # Continue to next cell on non-fatal errors
                    break
    
    def _process_single_cell(
        self,
        cell,
        search_term: str,
        search_input_term: str,
        cell_target: int,
        progress,
        cell_logger: ScraperLoggerAdapter,
        scraping_mode: str = 'fast',
        cell_idx: int = 0,
        total_cells: int = 1,
    ) -> None:
        """Process a single grid cell."""

        cell_start_time = time.time()
        cell_logger.info(f"Processing grid cell {cell.id} (target: {cell_target} results)...")

        progress_printer = getattr(self, "progress_printer", None)
        if progress_printer is not None:
            progress_printer.print_cell_header(
                cell_idx=cell_idx,
                total_cells=total_cells,
                cell_id=cell.id,
                cell_target=cell_target,
                cell_min_lat=getattr(cell, 'min_lat', None),
                cell_max_lat=getattr(cell, 'max_lat', None),
                cell_min_lng=getattr(cell, 'min_lng', None),
                cell_max_lng=getattr(cell, 'max_lng', None),
            )
        # Navigate to grid cell
        if not self.page_navigator.navigate_to_grid_cell(cell):
            raise NavigationException(f"Failed to navigate to cell {cell.id}")
        
        # Perform search
        if not self.page_navigator.perform_search(search_input_term):
            raise NavigationException(f"Search failed in cell {cell.id}")
        
        # Wait for search results
        if not self.page_navigator.wait_for_search_results():
            cell_logger.warning(f"No search results in cell {cell.id}")
            return
        
        # Scroll to load more listings
        max_per_cell = self.config.settings.scraping.max_listings_per_cell
        
        # Use cell_target instead of calculating from total_results
        remaining_target = min(max_per_cell, cell_target)
        
        seen_place_ids = progress.get_seen_urls_set()
        listing_urls = self.page_navigator.collect_listing_urls(
            seen_place_ids,
            target_count=remaining_target,
        )
        
        if not listing_urls:
            cell_logger.warning(f"No listing URLs collected from cell {cell.id}")
            if progress_printer is not None:
                progress_printer.print_cell_summary(
                    cell_idx=cell_idx, total_cells=total_cells, cell_id=cell.id,
                    new_count=0, dupe_count=0, error_count=0,
                    cell_elapsed=time.time() - cell_start_time,
                    global_done=progress.results_count, total_target=progress.total_target,
                )
            return

        cell_logger.info(f"Collected {len(listing_urls)} listing URLs in cell {cell.id}")

        # Process each listing
        processed_count = 0
        dupe_count = 0
        error_count = 0
        cell_start_count = progress.results_count
        
        for idx, url in enumerate(listing_urls):
            self._check_cancelled()
            # Check if we've reached cell target
            if processed_count >= cell_target:
                cell_logger.info(f"Reached cell target of {cell_target} results")
                break
            
            if processed_count >= max_per_cell:
                cell_logger.info(f"Reached max listings per cell ({max_per_cell})")
                break
            
            listing_start = time.time()
            try:
                status, business, review_count = self._process_single_listing_with_recovery(url, cell_logger)

                if status == "new":
                    processed_count += 1
                    self.progress_tracker.add_seen_url(url)
                    progress.results_count = self.progress_tracker.increment_results_count()
                    log_scraping_progress(cell_logger, progress.results_count,
                                          progress.total_target, "listings")
                elif status == "duplicate":
                    dupe_count += 1
                else:
                    error_count += 1

                if progress_printer is not None:
                    progress_printer.print_listing_result(
                        global_done=progress.results_count,
                        total_target=progress.total_target,
                        status=status,
                        business=business,
                        review_count=review_count,
                        listing_elapsed=time.time() - listing_start,
                        url=url,
                    )
                
            except Exception as e:
                if self._is_browser_closed_error(e):
                    raise ScraperException(
                        f"Browser/page closed while processing listing {idx + 1} "
                        f"in cell {cell.id}: {e}"
                    ) from e
                cell_logger.error(f"Error processing listing {idx+1}: {e}")
                error_count += 1
                if progress_printer is not None:
                    progress_printer.print_listing_result(
                        global_done=progress.results_count,
                        total_target=progress.total_target,
                        status="failed",
                        business=None,
                        review_count=0,
                        listing_elapsed=time.time() - listing_start,
                        url=url,
                    )
                continue
        
        # Track results per cell
        if processed_count > 0:
            self.progress_tracker.add_cell_results(cell.id, processed_count)
        
        cell_logger.info(f"Completed cell {cell.id}: processed {processed_count} listings")
        if progress_printer is not None:
            progress_printer.print_cell_summary(
                cell_idx=cell_idx,
                total_cells=total_cells,
                cell_id=cell.id,
                new_count=processed_count,
                dupe_count=dupe_count,
                error_count=error_count,
                cell_elapsed=time.time() - cell_start_time,
                global_done=progress.results_count,
                total_target=progress.total_target,
            )

    def _process_single_listing_with_recovery(self, url: str, logger) -> tuple:
        try:
            return self._normalize_listing_result(
                self._process_single_listing(url, logger)
            )
        except Exception as exc:
            if not self._is_browser_closed_error(exc):
                raise

            logger.warning(
                "Browser/page closed while processing a listing; relaunching the "
                "browser and retrying this listing once."
            )
            self._relaunch_browser_components(headless=self.config.settings.browser.headless)
            return self._normalize_listing_result(
                self._process_single_listing(url, logger)
            )

    @staticmethod
    def _normalize_listing_result(result) -> tuple:
        if isinstance(result, tuple) and len(result) == 3:
            return result
        if result is True:
            return ("new", None, 0)
        if result in (False, None):
            return ("failed", None, 0)
        raise TypeError(
            "_process_single_listing() must return a (status, business, review_count) tuple"
        )
    
    def _process_single_listing(self, url: str, logger) -> tuple:
        """Process a single business listing.
        
        Args:
            url: Business listing URL
            logger: Logger instance
            
        Returns:
            Tuple of (status, business, review_count) where status is "new", "duplicate", or "failed"
        """
        restore_browser_headless: Optional[bool] = None
        extraction = self.config.settings.extraction
        try:
            self._check_cancelled()
            # Navigate to business
            if not self.page_navigator.navigate_to_business(url):
                logger.warning(f"Failed to navigate to business: {url[:50]}...")
                return ("failed", None, 0)
            
            # Extract business data
            business = self.business_scraper.extract_data(url)
            business.source_query = self._current_search_term
            self._check_cancelled()
            
            if not business.name or business.name == "Extraction Failed":
                logger.warning(f"Failed to extract business data from: {url[:50]}...")
                return ("failed", None, 0)

            if self._should_retry_headful_review_collection(business):
                logger.warning(
                    "Limited Maps view detected in headless mode for %s; retrying this listing headful for reviews.",
                    business.name,
                )
                self._relaunch_browser_components(headless=False)
                restore_browser_headless = self.config.settings.browser.headless
                if not self.page_navigator.navigate_to_business(url):
                    logger.warning(f"Headful retry failed to navigate to business: {url[:50]}...")
                    return ("failed", None, 0)
                business = self.business_scraper.extract_data(url)
                business.source_query = self._current_search_term
                self._check_cancelled()

                if not business.name or business.name == "Extraction Failed":
                    logger.warning(
                        f"Failed to extract business data after headful retry from: {url[:50]}..."
                    )
                    return ("failed", None, 0)
            
            # Extract reviews whenever Maps exposes review signals, even when the
            # current layout hides the numeric review count in the summary block.
            reviews = []
            historical_reviews: List[Review] = []
            if self._should_extract_reviews(business):
                historical_reviews = self.review_hash_index.get_reviews(business.place_id)
                known_review_hashes = {review.review_hash for review in historical_reviews if review.review_hash}
                known_review_count = business.review_count if business.review_count > 0 else None
                if self.config.settings.scraping.review_mode == "rolling_365d":
                    max_reviews = None
                elif known_review_count is not None:
                    max_reviews = min(
                        known_review_count,
                        self.config.settings.scraping.max_reviews_per_business,
                    )
                else:
                    max_reviews = self.config.settings.scraping.max_reviews_per_business
                
                reviews = self.review_scraper.extract_data(
                    business.name, business.address, business.place_id,
                    known_review_count, max_reviews, known_hashes=known_review_hashes
                )
                analysis_reviews = self._merge_reviews(historical_reviews, reviews)

                review_metadata = getattr(self.review_scraper, "last_collection_metadata", {})
                if extraction.review_summary and business.review_count <= 0 and analysis_reviews:
                    business.review_count = len(analysis_reviews)
                if (
                    extraction.deleted_review_signals
                    and not business.deleted_review_notice
                    and review_metadata.get("deleted_review_notice")
                ):
                    business.deleted_review_count_min = review_metadata.get("deleted_review_count_min")
                    business.deleted_review_count_max = review_metadata.get("deleted_review_count_max")
                    business.deleted_review_notice = review_metadata.get("deleted_review_notice", "")
                
                # Analyze reviews to calculate metrics
                if extraction.review_analytics:
                    try:
                        review_metrics = analyze_reviews(
                            analysis_reviews,
                            collection_metadata=getattr(
                                self.review_scraper,
                                "last_collection_metadata",
                                {},
                            ),
                            deleted_review_bounds={
                                "min": business.deleted_review_count_min,
                                "max": business.deleted_review_count_max,
                            },
                            review_window_days=self.config.settings.scraping.review_window_days,
                        )

                        # Update business with review metrics
                        business.reply_rate_good = review_metrics['reply_rate_good']
                        business.reply_rate_bad = review_metrics['reply_rate_bad']
                        business.avg_time_between_reviews = review_metrics['avg_time_between_reviews']
                        business.reviews_last_365d_min = review_metrics['reviews_last_365d_min']
                        business.reviews_last_365d_max = review_metrics['reviews_last_365d_max']
                        business.reviews_last_365d_mid = review_metrics['reviews_last_365d_mid']
                        business.deleted_review_rate_min_pct = review_metrics['deleted_review_rate_min_pct']
                        business.deleted_review_rate_max_pct = review_metrics['deleted_review_rate_max_pct']
                        business.deleted_review_rate_mid_pct = review_metrics['deleted_review_rate_mid_pct']
                        business.review_window_coverage_status = review_metrics['review_window_coverage_status']
                        business.review_window_cutoff_observed = review_metrics['review_window_cutoff_observed']

                        logger.debug(f"Review metrics: good_reply={review_metrics['reply_rate_good']:.1f}%, "
                                   f"bad_reply={review_metrics['reply_rate_bad']:.1f}%, "
                                   f"avg_days={review_metrics['avg_time_between_reviews']}")

                    except Exception as e:
                        logger.warning(f"Failed to analyze reviews for {business.name}: {e}")

                if reviews and extraction.review_rows:
                    self.csv_writer.write_reviews(reviews)
                    self.review_hash_index.upsert_reviews(reviews)

            if extraction.website_modernity:
                self._assess_website_quality(business, logger)

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

            self._apply_business_export_flags(business)

            # Write business data (now with review metrics)
            if not self.csv_writer.write_business(business):
                logger.debug(f"Duplicate business skipped: {business.name}")
                return ("duplicate", business, len(reviews))
            
            logger.info(f"Successfully processed: {business.name}")
            return ("new", business, len(reviews))
            
        except Exception as e:
            if self._is_browser_closed_error(e):
                raise
            logger.error(f"Error processing listing {url[:50]}...: {e}")
            return ("failed", None, 0)
        finally:
            if (
                restore_browser_headless is not None
                and self._active_browser_headless != restore_browser_headless
            ):
                logger.info("Restoring preferred browser mode after headful review retry.")
                self._relaunch_browser_components(headless=restore_browser_headless)

    def _should_extract_reviews(self, business: Business) -> bool:
        extraction = self.config.settings.extraction
        if not (extraction.review_rows or extraction.review_analytics):
            return False

        if business.review_count > 0:
            return True

        if business.review_average > 0:
            return True

        if extraction.deleted_review_signals and business.deleted_review_notice:
            return True

        return False

    def _apply_business_export_flags(self, business: Business) -> None:
        extraction = self.config.settings.extraction
        business.export_contact_fields = extraction.contact_fields
        business.export_business_details = extraction.business_details
        business.export_review_summary = extraction.review_summary
        business.export_review_analytics = extraction.review_analytics
        business.export_deleted_review_signals = extraction.deleted_review_signals
        business.export_website_modernity = extraction.website_modernity

    def _assess_website_quality(self, business: Business, logger) -> None:
        assessment = assess_website_quality(business.website)
        business.website_status = assessment.status
        business.website_modernity_score = assessment.modernity_score
        business.website_modernity_reason = assessment.reason
        business.website_uses_https = assessment.uses_https
        business.website_mobile_friendly_hint = assessment.mobile_friendly_hint
        business.website_structured_data_hint = assessment.structured_data_hint
        business.website_stale_or_broken_hint = assessment.stale_or_broken_hint
        logger.debug(
            "Website quality for %s: %s (%s)",
            business.name,
            assessment.status,
            assessment.reason,
        )

    @staticmethod
    def _merge_reviews(existing_reviews: List[Review], new_reviews: List[Review]) -> List[Review]:
        merged_by_hash = {}

        for review in existing_reviews + new_reviews:
            review_hash = review.review_hash or Review.build_review_hash(
                review.place_id,
                review.reviewer_name,
                review.rating,
                review.review_text,
            )
            merged_by_hash[review_hash] = review

        return list(merged_by_hash.values())

    def _should_retry_headful_review_collection(self, business: Business) -> bool:
        if not self._should_extract_reviews(business):
            return False

        if self._active_browser_headless is not True:
            return False

        if self.page_navigator is None:
            return False

        return self.page_navigator.has_limited_view()
    
    def _cleanup_browser(self) -> None:
        """Clean up browser resources."""
        if self.browser_context:
            try:
                self.browser_context.close()
            except Exception as e:
                if "Event loop is closed" not in str(e):
                    self.component_logger.error(f"Error closing browser context: {e}")
            finally:
                self.browser_context = None
        if self.browser:
            try:
                self.browser.close()
            except Exception as e:
                if "Event loop is closed" not in str(e):
                    self.component_logger.error(f"Error closing browser: {e}")
            finally:
                self.browser = None
                self.page = None
                self._active_browser_headless = None
    
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
            category_summary_file = self.category_report_service.build_summary(
                self.config.settings.files.result_filename
            )
            
            if self._run_failed:
                logger.warning(
                    "Scraping stopped before completion."
                    + (
                        f" Last error: {self._run_failure_reason}"
                        if self._run_failure_reason
                        else ""
                    )
                )
                logger.info(
                    f"Partial results: {business_count} businesses, {review_count} reviews"
                )
            else:
                logger.info("Scraping completed!")
                logger.info(f"Final results: {business_count} businesses, {review_count} reviews")
            if category_summary_file:
                logger.info(f"Category summary: {category_summary_file}")

            self.progress_printer.print_run_summary(
                total_businesses=business_count,
                total_reviews=review_count,
                result_file=self.config.settings.files.result_filename,
                reviews_file=self.config.settings.files.reviews_filename,
                failed=self._run_failed,
                failure_reason=self._run_failure_reason or "",
            )
            
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
    apply_argument_overrides(config.settings, args)

    if getattr(args, 'result_file', None):
        config.settings.files.result_filename = args.result_file
    if getattr(args, 'reviews_file', None):
        config.settings.files.reviews_filename = args.reviews_file
    if getattr(args, 'progress_file', None):
        config.settings.files.progress_filename = args.progress_file

    log_level = getattr(args, "log_level", None)
    log_file = getattr(args, "log_file", None)

    return GoogleMapsScraper(config, log_level=log_level, log_file=log_file)
