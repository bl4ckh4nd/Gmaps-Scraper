from src.config.selectors import Selectors
from src.config.settings import ScraperSettings
from src.scraper.business_scraper import BusinessScraper


def test_extract_review_info_falls_back_to_summary_texts(monkeypatch):
    selectors = Selectors()
    scraper = BusinessScraper(None, ScraperSettings(), selectors)

    def fake_get_element_attribute(selector, attribute, timeout=None):
        if selector == selectors.RATING_SELECTOR:
            return "4,6 Sterne"
        return ""

    def fake_get_element_text(selector, timeout=None, required=False):
        if selector == selectors.REVIEWS_COUNT:
            return ""
        if selector == selectors.REVIEW_SUMMARY_TEXT_SELECTORS[0]:
            return "4,6(816)"
        return ""

    monkeypatch.setattr(scraper, "get_element_attribute", fake_get_element_attribute)
    monkeypatch.setattr(scraper, "get_element_text", fake_get_element_text)

    review_count, review_average = scraper._extract_review_info()

    assert review_count == 816
    assert review_average == 4.6
