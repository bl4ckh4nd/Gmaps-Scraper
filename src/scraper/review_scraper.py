"""Review scraper for Google Maps businesses."""

import re
from typing import Any, Dict, List, Optional, Set
from playwright.sync_api import Page

from ..models.review import Review  
from ..config.selectors import Selectors
from ..config.settings import ScraperSettings
from ..utils.deleted_review_extraction import (
    DeletedReviewNotice,
    extract_deleted_review_notice_text,
    parse_deleted_review_notice,
)
from ..utils.helpers import (
    clean_text,
    detect_language,
    extract_review_date_text,
    parse_star_rating,
)
from ..utils.review_analyzer import inspect_review_date
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
        self.last_collection_metadata: Dict[str, Any] = {}
        self._known_review_hashes: Set[str] = set()
        self._current_place_id: str = ""
    
    def extract_data(self, business_name: str, business_address: str, place_id: str,
                    total_reviews_count: Optional[int] = None, 
                    max_reviews: Optional[int] = None,
                    known_hashes: Optional[Set[str]] = None) -> List[Review]:
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
        self._current_place_id = place_id
        self._known_review_hashes = set(known_hashes or set())
        self.last_collection_metadata = {
            "review_mode": self.settings.scraping.review_mode,
            "coverage_status": "not_requested",
            "oldest_review_date_text": "",
            "boundary_reached": False,
            "has_one_year_bucket": False,
            "sort_applied": False,
            "hit_review_cap": False,
            "known_hash_encountered": False,
            "deleted_review_count_min": None,
            "deleted_review_count_max": None,
            "deleted_review_notice": "",
        }
        
        try:
            # Click on reviews tab if available
            if not self._navigate_to_reviews_tab():
                self.logger.info("Could not access reviews tab, checking for existing reviews")

            self._open_full_review_list()
            deleted_notice = self._extract_deleted_review_notice()
            self.last_collection_metadata["deleted_review_count_min"] = deleted_notice.min_count
            self.last_collection_metadata["deleted_review_count_max"] = deleted_notice.max_count
            self.last_collection_metadata["deleted_review_notice"] = deleted_notice.raw_text

            if self.settings.scraping.review_mode == "rolling_365d":
                self.last_collection_metadata["sort_applied"] = self._apply_review_sorting()
            
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
        if self._has_review_surface():
            return True

        # Try each reviews tab selector
        for selector in self.selectors.REVIEWS_TAB_SELECTORS:
            if self.safe_click(selector):
                self.safe_wait(2000)  # Wait for reviews to load
                if self._has_review_surface():
                    self.logger.info(f"Clicked reviews tab with selector: {selector}")
                    return True

        self.logger.info("Could not find or click reviews tab")
        return False

    def _open_full_review_list(self) -> bool:
        if self._has_review_containers():
            return True

        for selector in self.selectors.REVIEW_FULL_LIST_OPENERS:
            if self.safe_click(selector):
                self.safe_wait(2000)
                if self._has_review_containers() or self._has_review_surface():
                    self.logger.info(f"Opened full review list with selector: {selector}")
                    return True

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
        if self.settings.scraping.review_mode == "rolling_365d":
            if total_reviews_count:
                target = total_reviews_count
            elif max_reviews:
                target = max_reviews
            else:
                target = self.settings.scraping.default_max_reviews
            self.logger.info(
                "Rolling 365d mode will keep scrolling until the cutoff boundary is reached"
            )
        elif total_reviews_count and max_reviews:
            target = min(total_reviews_count, max_reviews)
        elif total_reviews_count:
            target = total_reviews_count
        elif max_reviews:
            target = max_reviews
        else:
            target = self.settings.scraping.default_max_reviews
        
        if self.settings.scraping.review_mode != "rolling_365d":
            target = min(target, self.settings.scraping.max_reviews_per_business)
        
        self.logger.info(f"Target reviews to extract: {target}")
        return target

    def _apply_review_sorting(self) -> bool:
        """Sort reviews by newest when running rolling-window analysis."""

        if self.settings.scraping.review_sort_order != "newest":
            return False

        sort_clicked = False
        for selector in self.selectors.REVIEW_SORT_BUTTON_SELECTORS:
            if self.safe_click(selector):
                sort_clicked = True
                self.safe_wait(1000)
                break

        if not sort_clicked:
            self.logger.info("Could not open review sorting controls")
            return False

        for selector in self.selectors.REVIEW_SORT_NEWEST_SELECTORS:
            if self.safe_click(selector):
                self.safe_wait(1500)
                self.logger.info("Applied newest-first review sorting")
                return True

        self.logger.info("Could not select newest review sorting")
        return False
    
    def _load_reviews_with_scrolling(self, target_reviews: int, 
                                   total_reviews_count: Optional[int]) -> List:
        """Load reviews by scrolling if necessary.
        
        Args:
            target_reviews: Number of reviews to target
            total_reviews_count: Total reviews available
            
        Returns:
            List of review container elements
        """
        review_containers = self.page.locator(self.selectors.REVIEW_CONTAINERS).all()
        initial_count = len(review_containers)
        
        self.logger.info(f"Found {initial_count} initial reviews")

        if self.settings.scraping.review_mode == "rolling_365d":
            review_containers = self._load_reviews_for_window(
                total_reviews_count,
                initial_count,
            )
            return review_containers
        
        # Only scroll if we need more reviews and there are more available
        if initial_count < target_reviews and self._should_scroll_for_more_reviews(
            initial_count, target_reviews, total_reviews_count
        ):
            review_containers = self._scroll_for_more_reviews(
                target_reviews, total_reviews_count, initial_count
            )
        
        return review_containers

    def _load_reviews_for_window(
        self,
        total_reviews_count: Optional[int],
        initial_count: int,
    ) -> List:
        """Load reviews until the rolling review window is bounded or safety limits are hit."""

        scroll_attempts = 0
        previous_count = initial_count
        max_attempts = self.settings.scraping.max_scroll_attempts
        scroll_interval = self.settings.scraping.scroll_interval

        while True:
            current_containers = self.page.locator(self.selectors.REVIEW_CONTAINERS).all()
            current_count = len(current_containers)
            boundary = self._evaluate_window_boundary(current_containers)
            self.last_collection_metadata.update(boundary)

            if boundary["boundary_reached"]:
                return current_containers

            if self._known_hash_is_loaded(current_containers):
                self.last_collection_metadata["known_hash_encountered"] = True
                self.last_collection_metadata["boundary_reached"] = True
                if self.last_collection_metadata["coverage_status"] == "collecting":
                    self.last_collection_metadata["coverage_status"] = "incremental_resume"
                return current_containers

            if total_reviews_count and current_count >= total_reviews_count:
                self.last_collection_metadata["coverage_status"] = "exact"
                self.last_collection_metadata["boundary_reached"] = True
                return current_containers

            scroll_success = False
            for selector in self.selectors.REVIEW_FEED_SELECTORS:
                if self.scroll_element(selector, 2000):
                    scroll_success = True
                    break

            if not scroll_success:
                self.last_collection_metadata["coverage_status"] = "incomplete"
                return current_containers

            self.safe_wait(scroll_interval)
            current_count = self.page.locator(self.selectors.REVIEW_CONTAINERS).count()

            if current_count <= previous_count:
                scroll_attempts += 1
                if scroll_attempts >= max_attempts:
                    self.last_collection_metadata["coverage_status"] = "incomplete"
                    return self.page.locator(self.selectors.REVIEW_CONTAINERS).all()
            else:
                previous_count = current_count
                scroll_attempts = 0

    def _evaluate_window_boundary(self, containers: List) -> Dict[str, Any]:
        """Check whether the loaded reviews fully bound the rolling 365-day window."""

        date_infos = []
        for container in containers:
            review_date = self._extract_with_fallback_selectors(
                container, self.selectors.REVIEW_DATE_SELECTORS
            )
            if not review_date:
                continue
            date_infos.append(
                inspect_review_date(
                    review_date,
                    window_days=self.settings.scraping.review_window_days,
                )
            )

        if not date_infos:
            return {
                "coverage_status": "incomplete",
                "oldest_review_date_text": "",
                "boundary_reached": False,
                "has_one_year_bucket": False,
            }

        has_one_year_bucket = any(info["ambiguous_one_year_bucket"] for info in date_infos)
        oldest_review = date_infos[-1]
        coverage_status = "collecting"
        boundary_reached = False

        if has_one_year_bucket and oldest_review["older_than_one_year_bucket"]:
            coverage_status = "estimated"
            boundary_reached = True
        elif not has_one_year_bucket and oldest_review["definitely_outside_window"]:
            coverage_status = "exact"
            boundary_reached = True

        return {
            "coverage_status": coverage_status,
            "oldest_review_date_text": oldest_review["raw_text"],
            "boundary_reached": boundary_reached,
            "has_one_year_bucket": has_one_year_bucket,
        }
    
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
        seen_hashes = set()

        containers_to_process = self._determine_review_containers_to_process(
            containers,
            target_reviews,
        )
        review_count_to_process = len(containers_to_process)

        for i in range(0, review_count_to_process, batch_size):
            end_idx = min(i + batch_size, review_count_to_process)
            batch_reviews = []
            
            for j in range(i, end_idx):
                review = self._extract_single_review(
                    containers_to_process[j], business_name, business_address, place_id
                )

                if review and review.is_valid():
                    if review.review_hash in seen_hashes:
                        continue
                    seen_hashes.add(review.review_hash)
                    batch_reviews.append(review)
                    reviews.append(review)
            
            # Log batch processing progress  
            if batch_reviews:
                self.logger.info(f"Processed batch: {len(batch_reviews)} reviews "
                               f"({len(reviews)}/{review_count_to_process} total)")
        
        return reviews

    def _determine_review_containers_to_process(
        self,
        containers: List,
        target_reviews: int,
    ) -> List:
        if self.settings.scraping.review_mode != "rolling_365d":
            return containers[:target_reviews]

        filtered_containers = []
        for container in containers:
            if self._container_matches_known_hash(container):
                self.last_collection_metadata["known_hash_encountered"] = True
                self.last_collection_metadata["boundary_reached"] = True
                self.last_collection_metadata["coverage_status"] = "incremental_resume"
                break

            review_date = self._extract_review_date(container)
            date_info = inspect_review_date(
                review_date,
                window_days=self.settings.scraping.review_window_days,
            )
            if date_info["definitely_outside_window"] or date_info["older_than_one_year_bucket"]:
                continue
            filtered_containers.append(container)

        return filtered_containers

    def _known_hash_is_loaded(self, containers: List) -> bool:
        if not self._known_review_hashes:
            return False

        for container in containers:
            if self._container_matches_known_hash(container):
                return True

        return False

    def _container_matches_known_hash(self, container) -> bool:
        if not self._known_review_hashes:
            return False

        reviewer_name = self._extract_with_fallback_selectors(
            container, self.selectors.REVIEWER_NAME_SELECTORS
        )
        review_text = self._extract_with_fallback_selectors(
            container, self.selectors.REVIEW_TEXT_SELECTORS
        )
        rating = self._extract_review_rating(container)
        review_hash = Review.build_review_hash(
            self._current_place_id,
            clean_text(reviewer_name),
            rating,
            clean_text(review_text),
        )
        return review_hash in self._known_review_hashes
    
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
            
            # Extract review date
            review_date = self._extract_review_date(container)

            # Extract star rating
            stars = self._extract_review_rating(container)
            
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
                    return container.locator(selector).first.inner_text()
            except Exception:
                continue
        return ""

    def _extract_review_date(self, container) -> str:
        review_date = self._extract_with_fallback_selectors(
            container, self.selectors.REVIEW_DATE_SELECTORS
        )
        if review_date:
            return review_date

        try:
            container_text = container.inner_text()
        except Exception:
            return ""

        return extract_review_date_text(container_text)
    
    def _extract_review_rating(self, container) -> int:
        """Extract star rating from review container.
        
        Args:
            container: Review container element
            
        Returns:
            Star rating (0-5)
        """
        for selector in self.selectors.REVIEW_STARS_SELECTORS:
            try:
                locator = container.locator(selector)
                if locator.count() > 0:
                    stars_text = locator.first.get_attribute('aria-label') or locator.first.inner_text()
                    rating = parse_star_rating(stars_text)
                    if rating > 0:
                        return rating
            except Exception:
                continue

        try:
            role_images = container.locator('[role="img"]')
            for idx in range(min(role_images.count(), 5)):
                stars_text = role_images.nth(idx).get_attribute('aria-label') or ""
                if re.search(r"stern|star", stars_text, flags=re.IGNORECASE):
                    rating = parse_star_rating(stars_text)
                    if rating > 0:
                        return rating
        except Exception:
            pass

        try:
            container_text = container.inner_text()
        except Exception:
            container_text = ""

        if container_text:
            rating_match = re.search(r"\b([1-5])\s*/\s*5\b", container_text)
            if rating_match:
                return parse_star_rating(rating_match.group(0))

        return 0

    def _has_review_surface(self) -> bool:
        if self._has_review_containers():
            return True

        for selector in (
            self.selectors.REVIEW_FEED_SELECTORS
            + self.selectors.REVIEW_SORT_BUTTON_SELECTORS
            + self.selectors.REVIEW_FULL_LIST_OPENERS
        ):
            try:
                if self.page.locator(selector).count() > 0:
                    return True
            except Exception:
                continue

        return False

    def _has_review_containers(self) -> bool:
        try:
            return self.page.locator(self.selectors.REVIEW_CONTAINERS).count() > 0
        except Exception:
            return False

    def _extract_deleted_review_notice(self) -> DeletedReviewNotice:
        candidate_texts: List[str] = []

        for selector in [
            'div.fontBodyMedium.zpEcLb',
            'div.OM45F',
            'div.dGZREb.bPJxoc.ZAIggb',
            'div.m6QErb.DxyBCb.kA9KIf.dS8AEf.XiKgde',
            'body',
        ]:
            try:
                locator = self.page.locator(selector)
                if locator.count() > 0:
                    text = locator.first.inner_text()
                    if text:
                        candidate_texts.append(text)
            except Exception:
                continue

        for candidate in candidate_texts:
            notice_text = extract_deleted_review_notice_text(candidate)
            if notice_text:
                return parse_deleted_review_notice(notice_text)

        return DeletedReviewNotice()
