from src.config.selectors import Selectors
from src.config.settings import ScraperSettings
from src.scraper.review_scraper import ReviewScraper


class _FakeLocator:
    def __init__(self, containers):
        self._containers = containers

    def all(self):
        return self._containers

    def count(self):
        return len(self._containers)


class _FakePage:
    def __init__(self, containers):
        self._containers = containers

    def locator(self, selector):
        return _FakeLocator(self._containers)


def test_calculate_target_reviews_ignores_cap_in_rolling_mode():
    settings = ScraperSettings()
    settings.scraping.review_mode = "rolling_365d"
    settings.scraping.max_reviews_per_business = 5
    scraper = ReviewScraper(None, settings, Selectors())

    target = scraper._calculate_target_reviews(total_reviews_count=200, max_reviews=5)

    assert target == 200


def test_calculate_target_reviews_still_caps_all_available_mode():
    settings = ScraperSettings()
    settings.scraping.review_mode = "all_available"
    settings.scraping.max_reviews_per_business = 5
    scraper = ReviewScraper(None, settings, Selectors())

    target = scraper._calculate_target_reviews(total_reviews_count=200, max_reviews=50)

    assert target == 5


def test_determine_review_containers_to_process_keeps_only_rolling_window(monkeypatch):
    settings = ScraperSettings()
    settings.scraping.review_mode = "rolling_365d"
    scraper = ReviewScraper(None, settings, Selectors())
    containers = ["vor 2 Monaten", "vor einem Jahr", "vor 13 Monaten", "vor 2 Jahren"]

    monkeypatch.setattr(scraper, "_extract_review_date", lambda container: container)

    selected = scraper._determine_review_containers_to_process(containers, target_reviews=999)

    assert selected == ["vor 2 Monaten", "vor einem Jahr"]


def test_load_reviews_for_window_marks_exact_when_all_reviews_are_loaded(monkeypatch):
    settings = ScraperSettings()
    settings.scraping.review_mode = "rolling_365d"
    containers = ["vor 2 Wochen", "vor 3 Monaten"]
    scraper = ReviewScraper(_FakePage(containers), settings, Selectors())
    scraper.last_collection_metadata = {
        "coverage_status": "collecting",
        "oldest_review_date_text": "",
        "boundary_reached": False,
        "has_one_year_bucket": False,
        "hit_review_cap": False,
    }

    monkeypatch.setattr(
        scraper,
        "_evaluate_window_boundary",
        lambda loaded: {
            "coverage_status": "collecting",
            "oldest_review_date_text": "vor 3 Monaten",
            "boundary_reached": False,
            "has_one_year_bucket": False,
        },
    )

    loaded = scraper._load_reviews_for_window(total_reviews_count=2, initial_count=2)

    assert loaded == containers
    assert scraper.last_collection_metadata["coverage_status"] == "exact"
    assert scraper.last_collection_metadata["boundary_reached"] is True
    assert scraper.last_collection_metadata["hit_review_cap"] is False


def test_determine_review_containers_to_process_stops_at_known_hash(monkeypatch):
    settings = ScraperSettings()
    settings.scraping.review_mode = "rolling_365d"
    scraper = ReviewScraper(None, settings, Selectors())
    scraper.last_collection_metadata = {
        "known_hash_encountered": False,
        "boundary_reached": False,
        "coverage_status": "collecting",
    }
    scraper._known_review_hashes = {"known"}

    monkeypatch.setattr(scraper, "_container_matches_known_hash", lambda container: container == "known")
    monkeypatch.setattr(scraper, "_extract_review_date", lambda container: "vor 2 Monaten")

    selected = scraper._determine_review_containers_to_process(
        ["new-1", "new-2", "known", "older"],
        target_reviews=999,
    )

    assert selected == ["new-1", "new-2"]
    assert scraper.last_collection_metadata["known_hash_encountered"] is True
    assert scraper.last_collection_metadata["boundary_reached"] is True
    assert scraper.last_collection_metadata["coverage_status"] == "incremental_resume"
