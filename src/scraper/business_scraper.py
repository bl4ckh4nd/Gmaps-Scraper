"""Business information scraper for Google Maps."""

from typing import Optional, List

from playwright.sync_api import Page
from ..models.business import Business
from ..config.selectors import Selectors
from ..config.settings import ScraperSettings
from ..utils.helpers import (
    extract_place_id, clean_website_url, clean_phone_number,
    parse_review_count, parse_rating_value, clean_text
)
from ..utils.deleted_review_extraction import (
    DeletedReviewNotice,
    extract_deleted_review_notice_text,
    parse_deleted_review_notice,
)
from .base_scraper import BaseScraper


class BusinessScraper(BaseScraper):
    """Scraper for extracting business information from Google Maps."""
    
    def __init__(self, page: Page, settings: ScraperSettings, selectors: Selectors):
        """Initialize business scraper.
        
        Args:
            page: Playwright page instance
            settings: Scraper configuration  
            selectors: Selector configuration
        """
        super().__init__(page, settings, selectors)
    
    def extract_data(self, maps_url: str) -> Business:
        """Extract business data from the current page.
        
        Args:
            maps_url: The Google Maps URL for this business
            
        Returns:
            Business instance with extracted data
            
        Raises:
            ExtractionException: If extraction fails critically
        """
        try:
            # Extract place ID from URL
            place_id = extract_place_id(maps_url)
            extraction = self.settings.extraction
            should_extract_website = (
                extraction.contact_fields
                or extraction.website_modernity
                or self.settings.owner_enrichment.enabled
            )
            should_extract_review_summary = (
                extraction.review_summary
                or extraction.review_rows
                or extraction.review_analytics
            )
            
            # Wait for main business info to load
            self.wait_for_element(self.selectors.BUSINESS_NAME, timeout=10000)
            
            # Extract basic business information
            business_name = self.get_element_text(self.selectors.BUSINESS_NAME, required=True)
            address = self.get_element_text(self.selectors.BUSINESS_ADDRESS)
            website = self._extract_website() if should_extract_website else None
            phone = self._extract_phone() if extraction.contact_fields else None
            business_type = (
                self.get_element_text(self.selectors.BUSINESS_TYPE)
                if extraction.business_details
                else ""
            )
            introduction = self._extract_introduction() if extraction.business_details else "None Found"
            
            # Extract review information
            review_count, review_average = (
                self._extract_review_info() if should_extract_review_summary else (0, 0.0)
            )

            deleted_review_notice = (
                self._extract_deleted_review_notice()
                if extraction.deleted_review_signals
                else DeletedReviewNotice()
            )
            
            # Extract operating hours
            opens_at = self._extract_opening_hours() if extraction.business_details else ""
            
            # Extract service information
            store_shopping, in_store_pickup, store_delivery = (
                self._extract_service_info()
                if extraction.business_details
                else ("No", "No", "No")
            )
            
            # Create and return business instance
            business = Business(
                place_id=place_id,
                name=business_name,
                address=address,
                website=website,
                phone=phone,
                review_count=review_count,
                review_average=review_average,
                deleted_review_count_min=deleted_review_notice.min_count,
                deleted_review_count_max=deleted_review_notice.max_count,
                deleted_review_notice=deleted_review_notice.raw_text,
                store_shopping=store_shopping,
                in_store_pickup=in_store_pickup,
                store_delivery=store_delivery,
                place_type=business_type,
                opens_at=opens_at,
                introduction=introduction,
                maps_url=maps_url
            )
            
            self.logger.info(f"Extracted business data: {business_name}")
            return business
            
        except Exception as e:
            self.logger.error(f"Failed to extract business data from {maps_url}: {e}")
            # Return minimal business with place_id so we don't lose the URL
            return Business(
                place_id=extract_place_id(maps_url),
                name="Extraction Failed",
                maps_url=maps_url
            )
    
    def _extract_website(self) -> Optional[str]:
        """Extract website URL."""
        website = self.get_element_text(self.selectors.BUSINESS_WEBSITE)
        return clean_website_url(website) if website else None
    
    def _extract_phone(self) -> Optional[str]:
        """Extract phone number."""
        phone = self.get_element_text(self.selectors.BUSINESS_PHONE)
        return clean_phone_number(phone) if phone else None
    
    def _extract_introduction(self) -> str:
        """Extract business introduction/description."""
        # Try multiple selectors for introduction
        intro_selectors = [
            self.selectors.BUSINESS_INTRO,
            '//*[@id="QA0Szd"]/div/div/div[1]/div[2]/div/div[1]/div/div/div[8]/button/div[3]/div/div[1]',
            '//div[@class="WeS02d fontBodyMedium"]//div[@class="PYvSYb"]',
            '//button//div[@class="WeS02d fontBodyMedium"]//div[@class="PYvSYb"]'
        ]
        
        introduction = self.try_multiple_selectors(intro_selectors, "text")
        return clean_text(introduction) if introduction else "None Found"
    
    def _extract_review_info(self) -> tuple[int, float]:
        """Extract review count and average rating.
        
        Returns:
            Tuple of (review_count, review_average)
        """
        review_count = 0
        review_average = 0.0

        for selector in self.selectors.REVIEW_SUMMARY_ARIA_SELECTORS:
            summary_aria = self.get_element_attribute(selector, "aria-label")
            if not summary_aria:
                continue

            if "rezension" in summary_aria.lower() or "review" in summary_aria.lower():
                review_count = max(review_count, parse_review_count(summary_aria))
            if "stern" in summary_aria.lower() or "star" in summary_aria.lower():
                review_average = max(review_average, parse_rating_value(summary_aria))

        if review_average <= 0:
            rating_text = self.get_element_attribute(
                self.selectors.RATING_SELECTOR,
                "aria-label"
            )
            if rating_text:
                review_average = parse_rating_value(rating_text)
                self.logger.debug(f"Found rating: {rating_text} -> {review_average}")

        if review_count <= 0:
            review_count_text = self.get_element_text(self.selectors.REVIEWS_COUNT)
            review_count = parse_review_count(review_count_text)

        if review_count <= 0:
            review_count = self._extract_review_count_from_summary_texts()

        if review_average <= 0:
            for selector in self.selectors.REVIEWS_AVERAGE:
                rating_text = self.get_element_text(selector)
                if rating_text:
                    review_average = parse_rating_value(rating_text)
                    if review_average > 0:
                        break
        
        return review_count, review_average

    def _extract_review_count_from_summary_texts(self) -> int:
        review_count = 0

        for selector in self.selectors.REVIEW_SUMMARY_TEXT_SELECTORS:
            summary_text = self.get_element_text(selector)
            if not summary_text:
                continue

            review_count = max(review_count, parse_review_count(summary_text))

        return review_count

    def _extract_deleted_review_notice(self) -> DeletedReviewNotice:
        """Extract the deleted-review notice text and normalize its count bounds."""

        candidate_texts = []

        try:
            selector_text = self.try_multiple_selectors(
                self.selectors.DELETED_REVIEWS_SELECTORS,
                "text",
            )
            if selector_text:
                candidate_texts.append(selector_text)
        except Exception as exc:
            self.logger.debug(f"Deleted-review selector lookup failed: {exc}")

        try:
            body_text = self.page.locator("body").inner_text(timeout=self.settings.browser.timeout_short)
            if body_text:
                candidate_texts.append(body_text)
        except Exception as exc:
            self.logger.debug(f"Deleted-review body scan failed: {exc}")

        for candidate in candidate_texts:
            notice_text = extract_deleted_review_notice_text(candidate)
            if notice_text:
                return parse_deleted_review_notice(notice_text)

        return DeletedReviewNotice()
    
    def _extract_opening_hours(self) -> str:
        """Extract opening hours information."""
        # Try primary selector
        opens_text = self.get_element_text(self.selectors.OPENS_AT)
        
        if not opens_text:
            # Try alternative selector
            opens_text = self.get_element_text(self.selectors.OPENS_AT_ALT)
        
        if opens_text:
            # Clean up the text
            opens = opens_text.split('⋅')
            if len(opens) > 1:
                opens_text = opens[1]
            
            # Remove special unicode spaces
            opens_text = opens_text.replace("\u202f", "").strip()
            
            return clean_text(opens_text)
        
        return ""
    
    def _extract_service_info(self) -> tuple[str, str, str]:
        """Extract service type information (shopping, pickup, delivery).
        
        Returns:
            Tuple of (store_shopping, in_store_pickup, store_delivery)
        """
        store_shopping = "No"
        in_store_pickup = "No" 
        store_delivery = "No"
        
        # Check each info section
        for info_selector in [self.selectors.INFO1, self.selectors.INFO2, self.selectors.INFO3]:
            info_text = self.get_element_text(info_selector)
            if info_text and '·' in info_text:
                # Split on bullet point and get second part
                parts = info_text.split('·')
                if len(parts) > 1:
                    service_text = parts[1].replace("\n", "").lower()
                    
                    if 'shop' in service_text:
                        store_shopping = "Yes"
                    elif 'pickup' in service_text:
                        in_store_pickup = "Yes"  
                    elif 'delivery' in service_text:
                        store_delivery = "Yes"
        
        return store_shopping, in_store_pickup, store_delivery
    
    def wait_for_business_details(self, timeout: int = 10000) -> bool:
        """Wait for business details to load on the page.
        
        Args:
            timeout: Timeout in milliseconds
            
        Returns:
            True if details loaded, False if timeout
        """
        return self.wait_for_element(self.selectors.BUSINESS_NAME, timeout=timeout)
    
    def get_available_info_sections(self) -> List[str]:
        """Get all available info sections for debugging.
        
        Returns:
            List of info section texts
        """
        info_sections = []
        
        for i, selector in enumerate([self.selectors.INFO1, self.selectors.INFO2, self.selectors.INFO3], 1):
            text = self.get_element_text(selector)
            if text:
                info_sections.append(f"Info{i}: {text}")
        
        return info_sections
