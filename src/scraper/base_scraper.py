"""Base scraper class with common functionality."""

from abc import ABC, abstractmethod
from typing import List, Optional, Union
import time
import logging
from playwright.sync_api import Page, Locator

from ..config.selectors import Selectors
from ..config.settings import ScraperSettings
from ..utils.exceptions import ExtractionException, NavigationException
from ..utils.logger import get_component_logger


class BaseScraper(ABC):
    """Abstract base class for all scrapers."""
    
    def __init__(self, page: Page, settings: ScraperSettings, selectors: Selectors):
        """Initialize base scraper.
        
        Args:
            page: Playwright page instance
            settings: Scraper configuration
            selectors: Selector configuration
        """
        self.page = page
        self.settings = settings
        self.selectors = selectors
        self.logger = get_component_logger(self.__class__.__name__)
    
    def wait_for_element(self, selector: str, timeout: Optional[int] = None) -> bool:
        """Wait for an element to appear on the page.
        
        Args:
            selector: CSS or XPath selector
            timeout: Timeout in milliseconds (uses default if None)
            
        Returns:
            True if element appeared, False if timeout
        """
        timeout = timeout or self.settings.browser.timeout_element
        
        try:
            self.page.wait_for_selector(selector, timeout=timeout)
            return True
        except Exception as e:
            self.logger.debug(f"Element not found: {selector} - {e}")
            return False
    
    def get_element_text(self, selector: str, timeout: Optional[int] = None, 
                        required: bool = False) -> str:
        """Get text content from an element.
        
        Args:
            selector: CSS or XPath selector
            timeout: Timeout in milliseconds
            required: Whether to raise exception if element not found
            
        Returns:
            Element text content, empty string if not found
            
        Raises:
            ExtractionException: If required element not found
        """
        timeout = timeout or self.settings.browser.timeout_short
        
        try:
            if self.page.locator(selector).count() > 0:
                return self.page.locator(selector).inner_text(timeout=timeout).strip()
            
            if required:
                raise ExtractionException(f"Required element not found: {selector}")
            
            return ""
            
        except Exception as e:
            if required:
                raise ExtractionException(f"Failed to get text from {selector}: {e}") from e
            self.logger.debug(f"Could not get text from {selector}: {e}")
            return ""
    
    def get_element_attribute(self, selector: str, attribute: str, 
                             timeout: Optional[int] = None, required: bool = False) -> str:
        """Get attribute value from an element.
        
        Args:
            selector: CSS or XPath selector
            attribute: Attribute name
            timeout: Timeout in milliseconds
            required: Whether to raise exception if element not found
            
        Returns:
            Attribute value, empty string if not found
            
        Raises:
            ExtractionException: If required element not found
        """
        timeout = timeout or self.settings.browser.timeout_short
        
        try:
            if self.page.locator(selector).count() > 0:
                value = self.page.locator(selector).get_attribute(attribute, timeout=timeout)
                return value or ""
            
            if required:
                raise ExtractionException(f"Required element not found: {selector}")
            
            return ""
            
        except Exception as e:
            if required:
                raise ExtractionException(f"Failed to get attribute {attribute} from {selector}: {e}") from e
            self.logger.debug(f"Could not get attribute {attribute} from {selector}: {e}")
            return ""
    
    def try_multiple_selectors(self, selectors: List[str], operation: str = "text", 
                              timeout: Optional[int] = None, **kwargs) -> str:
        """Try multiple selectors until one succeeds.
        
        Args:
            selectors: List of selectors to try
            operation: Operation to perform ('text', 'attribute')
            timeout: Timeout per selector attempt
            **kwargs: Additional arguments for the operation
            
        Returns:
            Result from first successful selector, empty string if all fail
        """
        timeout = timeout or self.settings.browser.timeout_very_short
        
        for selector in selectors:
            try:
                if operation == "text":
                    if self.page.locator(selector).count() > 0:
                        return self.page.locator(selector).inner_text(timeout=timeout).strip()
                        
                elif operation == "attribute":
                    attribute = kwargs.get("attribute", "")
                    if self.page.locator(selector).count() > 0:
                        value = self.page.locator(selector).get_attribute(attribute, timeout=timeout)
                        return value or ""
                        
            except Exception as e:
                self.logger.debug(f"Selector {selector} failed: {e}")
                continue
        
        return ""
    
    def safe_click(self, selector: str, timeout: Optional[int] = None, 
                   required: bool = False) -> bool:
        """Safely click an element with error handling.
        
        Args:
            selector: CSS or XPath selector
            timeout: Timeout in milliseconds
            required: Whether to raise exception if click fails
            
        Returns:
            True if click succeeded, False otherwise
            
        Raises:
            NavigationException: If required click fails
        """
        timeout = timeout or self.settings.browser.timeout_short
        
        try:
            if self.page.locator(selector).count() > 0:
                self.page.locator(selector).click(timeout=timeout)
                return True
            
            if required:
                raise NavigationException(f"Required clickable element not found: {selector}")
            
            return False
            
        except Exception as e:
            if required:
                raise NavigationException(f"Failed to click {selector}: {e}") from e
            self.logger.debug(f"Could not click {selector}: {e}")
            return False
    
    def scroll_element(self, selector: str, scroll_amount: int = 2000) -> bool:
        """Scroll within an element.
        
        Args:
            selector: CSS or XPath selector for scrollable element
            scroll_amount: Amount to scroll in pixels
            
        Returns:
            True if scroll succeeded, False otherwise
        """
        try:
            if selector.startswith('xpath='):
                # For XPath selectors
                success = self.page.evaluate("""
                    (xpath, amount) => {
                        const result = document.evaluate(xpath, document, null, 
                                                        XPathResult.FIRST_ORDERED_NODE_TYPE, null);
                        const element = result.singleNodeValue;
                        if (element) {
                            element.scrollTop += amount;
                            return true;
                        }
                        return false;
                    }
                """, selector.replace('xpath=', ''), scroll_amount)
                
                if success:
                    self.logger.debug(f"Scrolled {selector} by {scroll_amount}px")
                    return True
                    
            else:
                # For CSS selectors
                success = self.page.evaluate("""
                    (selector, amount) => {
                        const element = document.querySelector(selector);
                        if (element) {
                            element.scrollTop += amount;
                            return true;
                        }
                        return false;
                    }
                """, selector, scroll_amount)
                
                if success:
                    self.logger.debug(f"Scrolled {selector} by {scroll_amount}px")
                    return True
                    
        except Exception as e:
            self.logger.debug(f"JavaScript scroll failed for {selector}: {e}")
        
        # Fallback to mouse wheel
        try:
            if self.page.locator(selector).count() > 0:
                # Scroll into view first
                self.page.locator(selector).first.scroll_into_view_if_needed()
                # Then use mouse wheel
                self.page.mouse.wheel(0, scroll_amount)
                self.logger.debug(f"Used mouse wheel fallback for {selector}")
                return True
                
        except Exception as e:
            self.logger.debug(f"Mouse wheel fallback failed for {selector}: {e}")
        
        return False
    
    def wait_for_page_load(self, timeout: Optional[int] = None) -> None:
        """Wait for page to load completely.
        
        Args:
            timeout: Timeout in milliseconds
        """
        timeout = timeout or self.settings.browser.timeout_navigation
        
        try:
            self.page.wait_for_load_state('networkidle', timeout=timeout)
        except Exception as e:
            self.logger.warning(f"Page load timeout: {e}")
    
    def safe_wait(self, milliseconds: int) -> None:
        """Safe wait with logging.
        
        Args:
            milliseconds: Time to wait in milliseconds
        """
        self.page.wait_for_timeout(milliseconds)
        self.logger.debug(f"Waited {milliseconds}ms")
    
    @abstractmethod
    def extract_data(self, *args, **kwargs):
        """Extract data from the current page. Must be implemented by subclasses."""
        pass