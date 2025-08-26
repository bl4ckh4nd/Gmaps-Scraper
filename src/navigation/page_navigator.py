"""Page navigation and interaction for Google Maps scraper."""

from typing import List, Optional, Set
from playwright.sync_api import Page
import time
import os
from pathlib import Path
from datetime import datetime

from ..config.selectors import Selectors
from ..config.settings import ScraperSettings
from ..navigation.grid_navigator import GridCell
from ..utils.exceptions import NavigationException
from ..utils.helpers import extract_place_id
from ..utils.logger import get_component_logger


class PageNavigator:
    """Handles page navigation and interaction with Google Maps."""
    
    def __init__(self, page: Page, settings: ScraperSettings, selectors: Selectors):
        """Initialize page navigator.
        
        Args:
            page: Playwright page instance
            settings: Scraper configuration
            selectors: Selector configuration
        """
        self.page = page
        self.settings = settings
        self.selectors = selectors
        self.logger = get_component_logger('PageNavigator')
        self._cookie_banner_handled = False
        
        # Create debug directories
        self.debug_dir = Path('debug')
        self.debug_dir.mkdir(exist_ok=True)
        (self.debug_dir / 'screenshots').mkdir(exist_ok=True)
        (self.debug_dir / 'html').mkdir(exist_ok=True)
    
    def navigate_to_grid_cell(self, grid_cell: GridCell) -> bool:
        """Navigate to a specific grid cell coordinates.
        
        Args:
            grid_cell: Grid cell to navigate to
            
        Returns:
            True if navigation succeeded
            
        Raises:
            NavigationException: If navigation fails
        """
        try:
            maps_url = grid_cell.get_maps_url()
            self.logger.info(f"Navigating to grid cell {grid_cell.id}: {maps_url}")
            
            self.page.goto(maps_url, timeout=self.settings.browser.timeout_navigation)
            self.page.wait_for_timeout(2000)  # Let the page settle
            
            # Save debug screenshot after navigation
            self.save_debug_screenshot("01_after_navigation", grid_cell.id)
            
            # Handle cookie banner if present (only needs to be done once per session)
            if self.settings.browser.handle_cookie_banner and not self._cookie_banner_handled:
                try:
                    self.logger.info("Attempting to handle cookie banner...")
                    self.save_debug_screenshot("02_before_cookie_handling", grid_cell.id)
                    
                    if self.handle_cookie_banner(self.settings.browser.cookie_preference):
                        self._cookie_banner_handled = True
                        self.save_debug_screenshot("03_after_cookie_handling", grid_cell.id)
                        self.logger.info("Cookie banner handled successfully")
                    else:
                        self.logger.info("No cookie banner found")
                        
                except Exception as e:
                    self.logger.warning(f"Cookie banner handling failed: {e}")
                    self.save_debug_screenshot("03_cookie_handling_failed", grid_cell.id)
                    self._cookie_banner_handled = True  # Don't keep trying
            
            # Wait a bit more after cookie handling
            self.page.wait_for_timeout(1000)
            
            return True
            
        except Exception as e:
            self.save_debug_screenshot("navigation_failed", grid_cell.id)
            self.save_page_html("navigation_failed", grid_cell.id)
            raise NavigationException(f"Failed to navigate to grid cell {grid_cell.id}: {e}") from e
    
    def perform_search(self, search_term: str) -> bool:
        """Perform a search on Google Maps.
        
        Args:
            search_term: Search query
            
        Returns:
            True if search was performed successfully
            
        Raises:
            NavigationException: If search fails
        """
        try:
            self.logger.info(f"Searching for: '{search_term}'")
            
            # Save debug screenshot before searching
            self.save_debug_screenshot("04_before_search", "current")
            
            # Log available input elements for debugging
            self.log_available_elements("inputs")
            
            # Try multiple search input selectors
            search_input = None
            for selector in self.selectors.SEARCH_INPUT_SELECTORS:
                try:
                    locator = self.page.locator(selector)
                    if locator.count() > 0:
                        # Check if element is visible and enabled
                        first_elem = locator.first
                        if first_elem.is_visible() and first_elem.is_enabled():
                            search_input = first_elem
                            self.logger.info(f"Found search input with selector: {selector}")
                            break
                        else:
                            self.logger.debug(f"Search input found but not visible/enabled: {selector}")
                except Exception as e:
                    self.logger.debug(f"Failed to locate search input with selector '{selector}': {e}")
                    continue
            
            if search_input is None:
                # Save debug information when search input is not found
                self.save_debug_screenshot("search_input_not_found", "current")
                self.save_page_html("search_input_not_found", "current")
                self.log_available_elements("buttons")  # Also log buttons to see what's available
                raise NavigationException("Search input not found with any selector")
            
            # Clear any existing text and fill search term
            search_input.click()
            self.page.wait_for_timeout(500)
            search_input.fill("")
            search_input.fill(search_term)
            
            # Save debug screenshot after filling search
            self.save_debug_screenshot("05_after_filling_search", "current")
            
            # Press Enter to search
            self.page.keyboard.press("Enter")
            
            # Wait for search to process
            self.page.wait_for_timeout(2000)
            
            # Save debug screenshot after search
            self.save_debug_screenshot("06_after_search", "current")
            
            return True
            
        except Exception as e:
            # Save debug information on search failure
            self.save_debug_screenshot("search_failed", "current")
            self.save_page_html("search_failed", "current")
            raise NavigationException(f"Failed to perform search '{search_term}': {e}") from e
    
    def wait_for_search_results(self, timeout: int = 10000) -> bool:
        """Wait for search results to appear.
        
        Args:
            timeout: Timeout in milliseconds
            
        Returns:
            True if search results appeared
        """
        try:
            self.logger.info("Waiting for search results...")
            self.page.wait_for_selector(self.selectors.SEARCH_RESULTS, timeout=timeout)
            return True
            
        except Exception as e:
            self.logger.warning(f"No search results found: {e}")
            return False
    
    def scroll_for_listings(self, target_count: int = 100) -> int:
        """Scroll to load more listings in search results.
        
        Args:
            target_count: Target number of listings to load
            
        Returns:
            Number of listings found after scrolling
        """
        max_attempts = self.settings.scraping.max_scroll_attempts
        scroll_interval = self.settings.scraping.scroll_interval
        static_count_attempts = 0
        previously_counted = 0
        
        self.logger.info(f"Starting to scroll for listings (target: {target_count})...")
        
        while static_count_attempts < max_attempts:
            current_count = self.page.locator(self.selectors.SEARCH_RESULTS).count()
            self.logger.debug(f"Currently found: {current_count} listings")
            
            # Stop if we have enough listings
            if current_count >= target_count:
                self.logger.info(f"Found sufficient results ({current_count})")
                break
            
            # Check if we're making progress
            if current_count == previously_counted:
                static_count_attempts += 1
                self.logger.debug(f"No new results found. Attempt {static_count_attempts}/{max_attempts}")
            else:
                static_count_attempts = 0
                new_listings = current_count - previously_counted
                self.logger.info(f"Loaded {new_listings} new listings")
                previously_counted = current_count
            
            # Scroll for more results
            self._scroll_results_feed()
            self.page.wait_for_timeout(scroll_interval)
            
            # Break early if we've found a reasonable amount and aren't making progress
            if current_count > 40 and static_count_attempts >= 2:
                self.logger.info("Breaking early - sufficient results found")
                break
        
        final_count = self.page.locator(self.selectors.SEARCH_RESULTS).count()
        self.logger.info(f"Scrolling completed. Final count: {final_count} listings")
        return final_count
    
    def _scroll_results_feed(self) -> bool:
        """Scroll the results feed to load more listings.
        
        Returns:
            True if scrolling succeeded
        """
        try:
            # Try JavaScript scrolling first
            if self.page.locator(self.selectors.RESULTS_FEED).count() > 0:
                success = self.page.evaluate("""(selector) => {
                    const element = document.querySelector(selector);
                    if (element) {
                        element.scrollTop = element.scrollHeight;
                        return true;
                    }
                    return false;
                }""", self.selectors.RESULTS_FEED)
                
                if success:
                    return True
        
        except Exception as e:
            self.logger.debug(f"JavaScript scrolling failed: {e}")
        
        # Fallback to mouse wheel
        try:
            self.page.mouse.wheel(0, 20000)
            return True
        except Exception as e:
            self.logger.debug(f"Mouse wheel scrolling failed: {e}")
            return False
    
    def collect_listing_urls(self, seen_urls: Optional[Set[str]] = None) -> List[str]:
        """Collect all listing URLs from the current search results.
        
        Args:
            seen_urls: Set of already seen URLs to filter out
            
        Returns:
            List of unique listing URLs
        """
        seen_urls = seen_urls or set()
        unique_urls = []
        
        try:
            # Wait for listings to be available
            self.page.wait_for_selector(self.selectors.SEARCH_RESULTS, timeout=5000)
            
            # Get all listing elements
            all_listings = self.page.locator(self.selectors.SEARCH_RESULTS).all()
            self.logger.info(f"Found {len(all_listings)} total listing elements")
            
            # Extract URLs from visible and accessible listings
            for idx, listing in enumerate(all_listings):
                try:
                    # Check if listing is visible
                    if not listing.is_visible():
                        continue
                    
                    # Get the href attribute
                    url = listing.get_attribute('href', timeout=3000)
                    if not url:
                        continue
                    
                    # Extract place ID for deduplication
                    place_id = extract_place_id(url)
                    
                    # Only add if not already seen
                    if place_id not in seen_urls:
                        unique_urls.append(url)
                        seen_urls.add(place_id)
                        
                        if idx < 10 or idx % 10 == 0:  # Log progress for first 10 and every 10th
                            self.logger.debug(f"Added URL #{len(unique_urls)}: {url[:50]}...")
                    
                except Exception as e:
                    self.logger.debug(f"Error extracting URL from listing {idx}: {e}")
                    continue
            
            self.logger.info(f"Collected {len(unique_urls)} unique URLs")
            return unique_urls
            
        except Exception as e:
            self.logger.error(f"Failed to collect listing URLs: {e}")
            return unique_urls
    
    def navigate_to_business(self, url: str, timeout: int = 30000) -> bool:
        """Navigate to a specific business listing.
        
        Args:
            url: Business listing URL
            timeout: Timeout in milliseconds
            
        Returns:
            True if navigation succeeded
            
        Raises:
            NavigationException: If navigation fails
        """
        try:
            self.logger.debug(f"Navigating to business: {url[:50]}...")
            self.page.goto(url, timeout=timeout)
            
            # Wait for business details to load
            return self._wait_for_business_details()
            
        except Exception as e:
            raise NavigationException(f"Failed to navigate to business {url}: {e}") from e
    
    def _wait_for_business_details(self, timeout: int = 10000) -> bool:
        """Wait for business details to load.
        
        Args:
            timeout: Timeout in milliseconds
            
        Returns:
            True if business details loaded
        """
        try:
            # Wait for business name to appear
            self.page.wait_for_selector('//div[@class="TIHn2 "]//h1[@class="DUwDvf lfPIob"]', 
                                       timeout=timeout)
            self.logger.debug("Business details loaded successfully")
            return True
            
        except Exception as e:
            self.logger.warning(f"Business details failed to load: {e}")
            return False
    
    def get_current_url(self) -> str:
        """Get the current page URL.
        
        Returns:
            Current page URL
        """
        return self.page.url
    
    def take_screenshot(self, filename: str) -> bool:
        """Take a screenshot of the current page.
        
        Args:
            filename: Screenshot filename
            
        Returns:
            True if screenshot was taken successfully
        """
        try:
            self.page.screenshot(path=filename)
            self.logger.info(f"Screenshot saved: {filename}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to take screenshot: {e}")
            return False
    
    def reload_page(self) -> bool:
        """Reload the current page.
        
        Returns:
            True if reload succeeded
        """
        try:
            self.page.reload(timeout=self.settings.browser.timeout_navigation)
            return True
        except Exception as e:
            self.logger.error(f"Failed to reload page: {e}")
            return False
    
    def wait_for_page_idle(self, timeout: int = 5000) -> bool:
        """Wait for page to become idle (no network activity).
        
        Args:
            timeout: Timeout in milliseconds
            
        Returns:
            True if page became idle
        """
        try:
            self.page.wait_for_load_state('networkidle', timeout=timeout)
            return True
        except Exception as e:
            self.logger.debug(f"Page idle timeout: {e}")
            return False
    
    def check_for_captcha(self) -> bool:
        """Check if a CAPTCHA is present on the page.
        
        Returns:
            True if CAPTCHA is detected
        """
        captcha_selectors = [
            'iframe[src*="recaptcha"]',
            '.g-recaptcha',
            '#captcha',
            '[data-captcha]',
            'div[class*="captcha"]'
        ]
        
        for selector in captcha_selectors:
            if self.page.locator(selector).count() > 0:
                self.logger.warning("CAPTCHA detected on page")
                return True
        
        return False
    
    def handle_rate_limiting(self, delay: int = 30000) -> None:
        """Handle rate limiting by waiting.
        
        Args:
            delay: Delay in milliseconds
        """
        self.logger.warning(f"Rate limiting detected, waiting {delay/1000} seconds...")
        self.page.wait_for_timeout(delay)
    
    def handle_cookie_banner(self, preference: str = "reject") -> bool:
        """Handle cookie consent banner if present.
        
        Args:
            preference: "reject" or "accept" cookies (default: "reject")
            
        Returns:
            True if banner was found and handled, False if no banner found
        """
        try:
            # Wait briefly for page to load
            self.page.wait_for_timeout(2000)
            
            # Check if we're on a full-page consent form
            is_consent_page = self.is_on_consent_page()
            
            if is_consent_page:
                self.logger.info("Detected full-page Google consent form")
                self.save_debug_screenshot("consent_page_detected", "consent")
                
                # Scroll the consent page to reveal buttons
                self.scroll_consent_page()
                self.save_debug_screenshot("after_consent_scroll", "consent")
            
            # Check if any reject or accept buttons are present (after potential scrolling)
            banner_found = False
            cookie_buttons_found = []
            
            for selector in self.selectors.REJECT_ALL_BUTTON_SELECTORS + self.selectors.ACCEPT_ALL_BUTTON_SELECTORS:
                try:
                    locator = self.page.locator(selector)
                    if locator.count() > 0:
                        button = locator.first
                        if button.is_visible():
                            cookie_buttons_found.append(selector)
                            banner_found = True
                except Exception as e:
                    self.logger.debug(f"Failed to check cookie button selector '{selector}': {e}")
                    continue
            
            if not banner_found:
                if is_consent_page:
                    self.logger.warning("On consent page but no cookie buttons found after scrolling")
                    self.save_debug_screenshot("consent_no_buttons", "consent")
                    self.save_page_html("consent_no_buttons", "consent")
                else:
                    self.logger.debug("No cookie banner or cookie buttons found")
                
                # Log available buttons to see what's on the page
                self.log_available_elements("buttons")
                return False
            
            self.logger.info(f"Cookie consent detected (found {len(cookie_buttons_found)} cookie buttons)")
            if is_consent_page:
                self.logger.info("Processing full-page consent form")
            self.logger.info(f"Attempting to {preference} cookies")
            
            # Try to handle the banner based on preference
            if preference == "reject":
                # Try to click "Reject All" buttons
                button_found = False
                for selector in self.selectors.REJECT_ALL_BUTTON_SELECTORS:
                    try:
                        locator = self.page.locator(selector)
                        if locator.count() > 0:
                            reject_button = locator.first
                            if reject_button.is_visible() and reject_button.is_enabled():
                                self.logger.info(f"Clicking reject button: {selector}")
                                reject_button.click()
                                
                                # Wait for potential navigation after clicking
                                if is_consent_page:
                                    self.logger.info("Waiting for navigation back to Maps after rejecting cookies")
                                    self._wait_for_navigation_from_consent()
                                else:
                                    self.page.wait_for_timeout(1000)  # Wait for banner to disappear
                                    
                                self.logger.info("Successfully rejected cookies")
                                button_found = True
                                return True
                            else:
                                self.logger.debug(f"Reject button found but not clickable: {selector}")
                    except Exception as e:
                        self.logger.debug(f"Failed to click reject button {selector}: {e}")
                        continue
                
                if not button_found:
                    self.logger.warning("No clickable reject button found with standard selectors")
                    # Try aggressive fallback - look for any button with "ablehnen" in text
                    try:
                        all_buttons = self.page.locator("button").all()
                        for button in all_buttons:
                            try:
                                text = button.inner_text().lower()
                                aria_label = button.get_attribute('aria-label')
                                aria_label = aria_label.lower() if aria_label else ""
                                
                                if ("ablehnen" in text or "reject" in text or 
                                    "ablehnen" in aria_label or "reject" in aria_label):
                                    if button.is_visible() and button.is_enabled():
                                        self.logger.info(f"Found reject button via fallback: text='{text}', aria-label='{aria_label}'")
                                        button.click()
                                        
                                        # Wait for potential navigation after clicking
                                        if is_consent_page:
                                            self.logger.info("Waiting for navigation back to Maps after rejecting cookies")
                                            self._wait_for_navigation_from_consent()
                                        else:
                                            self.page.wait_for_timeout(1000)
                                            
                                        self.logger.info("Successfully rejected cookies via fallback")
                                        button_found = True
                                        return True
                            except Exception:
                                continue
                    except Exception as e:
                        self.logger.debug(f"Aggressive fallback failed: {e}")
                    
                    if not button_found:
                        # Log all buttons for debugging
                        self.log_available_elements("buttons")
                        
                # Fallback to accept if reject not found
                if not button_found:
                    self.logger.warning("Reject button not found, falling back to accept")
                    preference = "accept"
            
            if preference == "accept":
                # Try to click "Accept All" buttons
                button_found = False
                for selector in self.selectors.ACCEPT_ALL_BUTTON_SELECTORS:
                    try:
                        locator = self.page.locator(selector)
                        if locator.count() > 0:
                            accept_button = locator.first
                            if accept_button.is_visible() and accept_button.is_enabled():
                                self.logger.info(f"Clicking accept button: {selector}")
                                accept_button.click()
                                
                                # Wait for potential navigation after clicking
                                if is_consent_page:
                                    self.logger.info("Waiting for navigation back to Maps after accepting cookies")
                                    self._wait_for_navigation_from_consent()
                                else:
                                    self.page.wait_for_timeout(1000)  # Wait for banner to disappear
                                    
                                self.logger.info("Successfully accepted cookies")
                                button_found = True
                                return True
                            else:
                                self.logger.debug(f"Accept button found but not clickable: {selector}")
                    except Exception as e:
                        self.logger.debug(f"Failed to click accept button {selector}: {e}")
                        continue
                
                if not button_found:
                    self.logger.warning("No clickable accept button found with standard selectors")
                    # Try aggressive fallback - look for any button with "akzeptieren" in text
                    try:
                        all_buttons = self.page.locator("button").all()
                        for button in all_buttons:
                            try:
                                text = button.inner_text().lower()
                                aria_label = button.get_attribute('aria-label')
                                aria_label = aria_label.lower() if aria_label else ""
                                
                                if ("akzeptieren" in text or "accept" in text or 
                                    "akzeptieren" in aria_label or "accept" in aria_label):
                                    if button.is_visible() and button.is_enabled():
                                        self.logger.info(f"Found accept button via fallback: text='{text}', aria-label='{aria_label}'")
                                        button.click()
                                        
                                        # Wait for potential navigation after clicking
                                        if is_consent_page:
                                            self.logger.info("Waiting for navigation back to Maps after accepting cookies")
                                            self._wait_for_navigation_from_consent()
                                        else:
                                            self.page.wait_for_timeout(1000)
                                            
                                        self.logger.info("Successfully accepted cookies via fallback")
                                        button_found = True
                                        return True
                            except Exception:
                                continue
                    except Exception as e:
                        self.logger.debug(f"Aggressive accept fallback failed: {e}")
                    
                    if not button_found:
                        self.logger.warning("No clickable accept button found")
                        # Log all buttons for debugging
                        self.log_available_elements("buttons")
            
            # If we get here, we found a banner but couldn't handle it
            self.logger.warning("Cookie banner found but could not be handled")
            return False
            
        except Exception as e:
            self.logger.error(f"Error handling cookie banner: {e}")
            return False
    
    def is_on_consent_page(self) -> bool:
        """Check if we're on Google's consent page.
        
        Returns:
            True if currently on a consent/privacy page
        """
        try:
            current_url = self.page.url.lower()
            consent_indicators = [
                "consent.google.com",
                "consent.youtube.com", 
                "privacy.google.com",
                "consent" in current_url,
                "privacy" in current_url,
                "cookiechoices" in current_url
            ]
            
            is_consent = any(indicator for indicator in consent_indicators if 
                           (isinstance(indicator, str) and indicator in current_url) or 
                           (isinstance(indicator, bool) and indicator))
            
            if is_consent:
                self.logger.info(f"Detected consent page: {self.page.url}")
                
            return is_consent
        except Exception as e:
            self.logger.debug(f"Error checking consent page: {e}")
            return False
    
    def scroll_consent_page(self) -> None:
        """Scroll the full consent page to reveal buttons at the bottom.
        
        This handles Google's full-page consent forms that require scrolling
        to access the reject/accept buttons.
        """
        try:
            self.logger.info("Scrolling consent page to reveal buttons")
            
            # Multiple scroll attempts to handle dynamic loading
            for attempt in range(4):
                # Scroll to bottom using JavaScript
                self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                self.page.wait_for_timeout(1000)
                
                # Also try keyboard scrolling
                self.page.keyboard.press("End")
                self.page.wait_for_timeout(500)
                
                self.logger.debug(f"Scroll attempt {attempt + 1}/4 completed")
            
            # Additional scroll with Page Down keys
            for _ in range(5):
                self.page.keyboard.press("PageDown")
                self.page.wait_for_timeout(300)
            
            # Final wait for content to stabilize
            self.page.wait_for_timeout(2000)
            self.logger.info("Consent page scrolling completed")
            
        except Exception as e:
            self.logger.warning(f"Error scrolling consent page: {e}")
            # Don't fail the whole process if scrolling fails
    
    def _wait_for_navigation_from_consent(self, timeout: int = 10000) -> bool:
        """Wait for navigation away from consent page back to Maps.
        
        Args:
            timeout: Maximum time to wait in milliseconds
            
        Returns:
            True if navigation occurred, False if timeout
        """
        try:
            self.logger.info("Waiting for navigation away from consent page...")
            
            # Wait for URL to change away from consent domains
            start_url = self.page.url
            wait_time = 0
            check_interval = 500
            
            while wait_time < timeout:
                self.page.wait_for_timeout(check_interval)
                current_url = self.page.url
                
                # Check if we've navigated away from consent page
                if not self.is_on_consent_page():
                    self.logger.info(f"Successfully navigated from consent page to: {current_url}")
                    # Additional wait for the new page to load
                    self.page.wait_for_timeout(2000)
                    return True
                
                wait_time += check_interval
                
                if wait_time % 2000 == 0:  # Log every 2 seconds
                    self.logger.debug(f"Still on consent page after {wait_time/1000}s, waiting...")
            
            self.logger.warning(f"Navigation timeout after {timeout/1000}s, still on: {self.page.url}")
            return False
            
        except Exception as e:
            self.logger.warning(f"Error waiting for consent navigation: {e}")
            # Give it a basic wait as fallback
            self.page.wait_for_timeout(3000)
            return False
    
    def save_debug_screenshot(self, step_name: str, grid_cell_id: str = "unknown") -> str:
        """Save a debug screenshot with timestamp.
        
        Args:
            step_name: Description of the step
            grid_cell_id: Current grid cell ID
            
        Returns:
            Path to saved screenshot
        """
        if not self.settings.browser.enable_debug_mode:
            return ""
            
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
            filename = f"{grid_cell_id}_{step_name}_{timestamp}.png"
            screenshot_path = self.debug_dir / 'screenshots' / filename
            
            self.page.screenshot(path=str(screenshot_path))
            self.logger.debug(f"Debug screenshot saved: {screenshot_path}")
            return str(screenshot_path)
        except Exception as e:
            self.logger.warning(f"Failed to save debug screenshot: {e}")
            return ""
    
    def save_page_html(self, step_name: str, grid_cell_id: str = "unknown") -> str:
        """Save current page HTML for debugging.
        
        Args:
            step_name: Description of the step
            grid_cell_id: Current grid cell ID
            
        Returns:
            Path to saved HTML file
        """
        if not self.settings.browser.enable_debug_mode:
            return ""
            
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
            filename = f"{grid_cell_id}_{step_name}_{timestamp}.html"
            html_path = self.debug_dir / 'html' / filename
            
            html_content = self.page.content()
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            self.logger.debug(f"Debug HTML saved: {html_path}")
            return str(html_path)
        except Exception as e:
            self.logger.warning(f"Failed to save debug HTML: {e}")
            return ""
    
    def log_available_elements(self, selector_type: str = "inputs") -> None:
        """Log available elements for debugging.
        
        Args:
            selector_type: Type of elements to log (inputs, buttons, etc.)
        """
        try:
            if selector_type == "inputs":
                elements = self.page.locator("input").all()
                self.logger.info(f"Found {len(elements)} input elements:")
                for i, elem in enumerate(elements[:10]):  # Limit to first 10
                    try:
                        attrs = {
                            'id': elem.get_attribute('id'),
                            'name': elem.get_attribute('name'),
                            'class': elem.get_attribute('class'),
                            'placeholder': elem.get_attribute('placeholder'),
                            'aria-label': elem.get_attribute('aria-label'),
                            'visible': elem.is_visible()
                        }
                        self.logger.info(f"  Input {i}: {attrs}")
                    except Exception:
                        continue
            elif selector_type == "buttons":
                elements = self.page.locator("button").all()
                self.logger.info(f"Found {len(elements)} button elements:")
                for i, elem in enumerate(elements[:10]):  # Limit to first 10
                    try:
                        text_content = elem.text_content()[:50] if elem.text_content() else ""
                        attrs = {
                            'text': text_content,
                            'class': elem.get_attribute('class'),
                            'aria-label': elem.get_attribute('aria-label'),
                            'visible': elem.is_visible()
                        }
                        self.logger.info(f"  Button {i}: {attrs}")
                    except Exception:
                        continue
        except Exception as e:
            self.logger.warning(f"Failed to log available elements: {e}")