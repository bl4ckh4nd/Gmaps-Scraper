"""Review scraper for Google Maps businesses."""

from typing import List, Optional
from playwright.sync_api import Page

from ..models.review import Review  
from ..config.selectors import Selectors
from ..config.settings import ScraperSettings
from ..utils.helpers import parse_star_rating, detect_language, clean_text
from ..utils.exceptions import ExtractionException
from .base_scraper import BaseScraper


class ReviewScraper(BaseScraper):
    """Scraper for extracting reviews from Google Maps businesses."""
    
    def __init__(self, page: Page, settings: ScraperSettings, selectors: Selectors):
        """Initialize review scraper.
        
        Args:
            page: Playwright page instance
            settings: Scraper configuration
            selectors: Selector configuration
        """
        super().__init__(page, settings, selectors)
    
    def extract_data(self, business_name: str, business_address: str, place_id: str,
                    total_reviews_count: Optional[int] = None, 
                    max_reviews: Optional[int] = None) -> List[Review]:
        """Extract reviews for the current business listing.
        
        Args:
            business_name: Name of the business
            business_address: Address of the business  
            place_id: Place ID of the business
            total_reviews_count: Total number of reviews available
            max_reviews: Maximum number of reviews to extract
            
        Returns:
            List of Review instances
        """
        reviews = []
        
        try:
            # Click on reviews tab if available
            if not self._navigate_to_reviews_tab():
                self.logger.info("Could not access reviews tab, checking for existing reviews")
            
            # Wait for review containers to appear
            if not self.wait_for_element(self.selectors.REVIEW_CONTAINERS, timeout=5000):
                self.logger.info("No reviews found for this location")
                return reviews
            
            # Determine target review count
            target_reviews = self._calculate_target_reviews(
                total_reviews_count, max_reviews
            )
            
            # Load reviews with scrolling if needed
            review_containers = self._load_reviews_with_scrolling(
                target_reviews, total_reviews_count
            )
            
            # Process reviews in batches
            reviews = self._process_review_containers(
                review_containers, business_name, business_address, 
                place_id, target_reviews
            )
            
            self.logger.info(f"Successfully extracted {len(reviews)} reviews")
            return reviews
            
        except Exception as e:
            self.logger.error(f"Error in extract_reviews: {e}")
            return reviews
    
    def _navigate_to_reviews_tab(self) -> bool:
        """Navigate to the reviews tab.
        
        Returns:
            True if successfully navigated to reviews tab
        """
        # Try each reviews tab selector
        for selector in self.selectors.REVIEWS_TAB_SELECTORS:
            if self.safe_click(selector):
                self.logger.info(f"Clicked reviews tab with selector: {selector}")
                self.safe_wait(2000)  # Wait for reviews to load
                return True
                
        self.logger.info("Could not find or click reviews tab")
        return False
    
    def _calculate_target_reviews(self, total_reviews_count: Optional[int], 
                                 max_reviews: Optional[int]) -> int:
        """Calculate how many reviews to target for extraction.
        
        Args:
            total_reviews_count: Total reviews available
            max_reviews: Maximum reviews requested
            
        Returns:
            Target number of reviews to extract
        """
        if total_reviews_count and max_reviews:
            target = min(total_reviews_count, max_reviews)
        elif total_reviews_count:
            target = total_reviews_count
        elif max_reviews:
            target = max_reviews
        else:
            target = self.settings.scraping.default_max_reviews
        
        # Cap at the maximum per business setting
        target = min(target, self.settings.scraping.max_reviews_per_business)
        
        self.logger.info(f"Target reviews to extract: {target}")
        return target
    
    def _load_reviews_with_scrolling(self, target_reviews: int, 
                                   total_reviews_count: Optional[int]) -> List:
        """Load reviews by scrolling if necessary.
        
        Args:
            target_reviews: Number of reviews to target
            total_reviews_count: Total reviews available
            
        Returns:
            List of review container elements
        """
        # Get initial review containers
        review_containers = self.page.locator(self.selectors.REVIEW_CONTAINERS).all()
        initial_count = len(review_containers)
        
        self.logger.info(f"Found {initial_count} initial reviews")
        
        # Only scroll if we need more reviews and there are more available
        if initial_count < target_reviews and self._should_scroll_for_more_reviews(
            initial_count, target_reviews, total_reviews_count
        ):
            review_containers = self._scroll_for_more_reviews(
                target_reviews, total_reviews_count, initial_count
            )
        
        return review_containers
    
    def _should_scroll_for_more_reviews(self, current_count: int, target_count: int,
                                      total_available: Optional[int]) -> bool:
        """Determine if we should scroll to load more reviews.
        
        Args:
            current_count: Currently loaded reviews
            target_count: Target review count
            total_available: Total reviews available
            
        Returns:
            True if we should scroll for more reviews
        """
        if current_count >= target_count:
            return False
            
        if total_available and current_count >= total_available:
            return False
            
        return True
    
    def _scroll_for_more_reviews(self, target_reviews: int, 
                               total_reviews_count: Optional[int],
                               initial_count: int) -> List:
        """Scroll the reviews container to load more reviews.
        
        Args:
            target_reviews: Target number of reviews
            total_reviews_count: Total reviews available  
            initial_count: Initial number of loaded reviews
            
        Returns:
            List of review container elements after scrolling
        """
        scroll_attempts = 0
        previous_count = initial_count
        max_attempts = self.settings.scraping.max_scroll_attempts
        scroll_interval = self.settings.scraping.scroll_interval
        
        while scroll_attempts < max_attempts:
            # Try scrolling with different selectors
            scroll_success = False
            
            for selector in self.selectors.REVIEW_FEED_SELECTORS:
                if self.scroll_element(selector, 2000):
                    scroll_success = True
                    break
            
            # Fallback: scroll with mouse wheel if JavaScript failed
            if not scroll_success:
                try:
                    if self.page.locator(self.selectors.REVIEW_CONTAINERS).count() > 0:
                        self.page.locator(self.selectors.REVIEW_CONTAINERS).first.scroll_into_view_if_needed()
                        self.page.mouse.wheel(0, 2000)
                        scroll_success = True
                        self.logger.debug("Used mouse wheel fallback for scrolling")
                except Exception as e:
                    self.logger.warning(f"Mouse wheel scrolling failed: {e}")
            
            # Wait for new content to load
            self.safe_wait(scroll_interval)
            
            # Check if we got more reviews
            current_containers = self.page.locator(self.selectors.REVIEW_CONTAINERS).all()
            current_count = len(current_containers)
            
            self.logger.debug(f"After scroll: {current_count}/{target_reviews} reviews")
            
            # Stop if we've reached all available reviews
            if total_reviews_count and current_count >= total_reviews_count:
                self.logger.info(f"Loaded all {total_reviews_count} available reviews")
                break
            
            # Stop if we've reached our target
            if current_count >= target_reviews:
                self.logger.info(f"Reached target of {target_reviews} reviews")
                break
            
            # Check if we're making progress
            if current_count <= previous_count:
                scroll_attempts += 1
                self.logger.debug(f"No new reviews loaded. Attempt {scroll_attempts}/{max_attempts}")
                
                # Give up if we've tried multiple times with no progress
                if scroll_attempts >= max_attempts:
                    self.logger.info("Reached maximum scroll attempts, assuming all reviews loaded")
                    break
            else:
                # Reset attempts if we got new reviews
                scroll_attempts = 0
                new_reviews_loaded = current_count - previous_count
                self.logger.info(f"Loaded {new_reviews_loaded} new reviews")
            
            previous_count = current_count
        
        return self.page.locator(self.selectors.REVIEW_CONTAINERS).all()
    
    def _process_review_containers(self, containers: List, business_name: str,
                                 business_address: str, place_id: str,
                                 target_reviews: int) -> List[Review]:
        """Process review containers and extract review data.
        
        Args:
            containers: List of review container elements
            business_name: Name of the business
            business_address: Address of the business
            place_id: Place ID of the business
            target_reviews: Target number of reviews
            
        Returns:
            List of extracted Review instances
        """
        reviews = []
        batch_size = self.settings.scraping.review_batch_size
        
        # Process reviews in batches
        review_count_to_process = min(len(containers), target_reviews)
        
        for i in range(0, review_count_to_process, batch_size):
            end_idx = min(i + batch_size, review_count_to_process)
            batch_reviews = []
            
            for j in range(i, end_idx):
                review = self._extract_single_review(
                    containers[j], business_name, business_address, place_id
                )
                
                if review and review.is_valid():
                    batch_reviews.append(review)
                    reviews.append(review)
            
            # Log batch processing progress  
            if batch_reviews:
                self.logger.info(f"Processed batch: {len(batch_reviews)} reviews "
                               f"({len(reviews)}/{review_count_to_process} total)")
        
        return reviews
    
    def _extract_single_review(self, container, business_name: str,
                             business_address: str, place_id: str) -> Optional[Review]:
        """Extract data from a single review container.
        
        Args:
            container: Review container element
            business_name: Name of the business
            business_address: Address of the business  
            place_id: Place ID of the business
            
        Returns:
            Review instance or None if extraction failed
        """
        try:
            # Extract reviewer name
            reviewer_name = self._extract_with_fallback_selectors(
                container, self.selectors.REVIEWER_NAME_SELECTORS
            )
            
            # Extract review text
            review_text = self._extract_with_fallback_selectors(
                container, self.selectors.REVIEW_TEXT_SELECTORS
            )
            
            # Extract star rating
            stars = self._extract_review_rating(container)
            
            # Extract review date
            review_date = self._extract_with_fallback_selectors(
                container, self.selectors.REVIEW_DATE_SELECTORS
            )
            
            # Extract owner response
            owner_response = self._extract_with_fallback_selectors(
                container, self.selectors.OWNER_RESPONSE_SELECTORS
            )
            
            # Detect language
            language = detect_language(review_text)
            
            # Create review object
            review = Review(
                place_id=place_id,
                business_name=business_name,
                business_address=business_address,
                reviewer_name=clean_text(reviewer_name),
                review_text=clean_text(review_text),
                rating=stars,
                review_date=clean_text(review_date),
                owner_response=clean_text(owner_response),
                language=language
            )
            
            return review
            
        except Exception as e:
            self.logger.error(f"Error extracting single review: {e}")
            return None
    
    def _extract_with_fallback_selectors(self, container, selectors: List[str]) -> str:
        """Extract text using multiple fallback selectors.
        
        Args:
            container: Container element to search within
            selectors: List of selectors to try
            
        Returns:
            Extracted text or empty string
        """
        for selector in selectors:
            try:
                if container.locator(selector).count() > 0:
                    return container.locator(selector).inner_text()
            except Exception:
                continue
        return ""
    
    def _extract_review_rating(self, container) -> int:
        """Extract star rating from review container.
        
        Args:
            container: Review container element
            
        Returns:
            Star rating (0-5)
        """
        for selector in self.selectors.REVIEW_STARS_SELECTORS:
            try:
                if container.locator(selector).count() > 0:
                    stars_text = container.locator(selector).get_attribute('aria-label')
                    return parse_star_rating(stars_text)
            except Exception:
                continue
        return 0