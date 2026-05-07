import logging
from types import SimpleNamespace

from src.config.settings import ScraperSettings
from src.google_maps_scraper import GoogleMapsScraper
from src.models.business import Business
from src.models.review import Review
from src.utils.exceptions import ScraperException


def _scraper():
    scraper = GoogleMapsScraper.__new__(GoogleMapsScraper)
    scraper.config = SimpleNamespace(settings=ScraperSettings())
    return scraper


def test_should_extract_reviews_when_review_count_is_present():
    scraper = _scraper()
    business = Business(place_id="p1", name="Test", review_count=25)

    assert scraper._should_extract_reviews(business) is True


def test_should_extract_reviews_when_rating_exists_but_count_is_hidden():
    scraper = _scraper()
    business = Business(place_id="p1", name="Test", review_count=0, review_average=4.3)

    assert scraper._should_extract_reviews(business) is True


def test_should_extract_reviews_when_deleted_review_notice_exists():
    scraper = _scraper()
    business = Business(
        place_id="p1",
        name="Test",
        review_count=0,
        review_average=0.0,
        deleted_review_notice="Sechs bis zehn Bewertungen entfernt.",
    )

    assert scraper._should_extract_reviews(business) is True


def test_should_not_extract_reviews_without_any_review_signal():
    scraper = _scraper()
    business = Business(place_id="p1", name="Test", review_count=0, review_average=0.0)

    assert scraper._should_extract_reviews(business) is False


def test_should_retry_headful_review_collection_for_limited_headless_view():
    scraper = _scraper()
    scraper._active_browser_headless = True
    scraper.page_navigator = SimpleNamespace(has_limited_view=lambda: True)
    business = Business(place_id="p1", name="Test", review_count=0, review_average=4.3)

    assert scraper._should_retry_headful_review_collection(business) is True


def test_should_not_retry_headful_when_view_is_not_limited():
    scraper = _scraper()
    scraper._active_browser_headless = True
    scraper.page_navigator = SimpleNamespace(has_limited_view=lambda: False)
    business = Business(place_id="p1", name="Test", review_count=0, review_average=4.3)

    assert scraper._should_retry_headful_review_collection(business) is False


def test_merge_reviews_deduplicates_by_review_hash():
    existing = Review(
        place_id="p1",
        business_name="Test",
        business_address="Addr",
        reviewer_name="Alice",
        review_text="Great stay",
        rating=5,
        review_date="vor 2 Monaten",
    )
    duplicate_new = Review(
        place_id="p1",
        business_name="Test",
        business_address="Addr",
        reviewer_name="Alice",
        review_text="Great   stay",
        rating=5,
        review_date="vor 2 Monaten",
    )
    fresh = Review(
        place_id="p1",
        business_name="Test",
        business_address="Addr",
        reviewer_name="Bob",
        review_text="Nice staff",
        rating=4,
        review_date="vor 1 Monat",
    )

    merged = GoogleMapsScraper._merge_reviews([existing], [duplicate_new, fresh])

    assert len(merged) == 2
    assert {review.review_hash for review in merged} == {
        existing.review_hash,
        fresh.review_hash,
    }


def test_process_single_cell_marks_seen_only_after_successful_listing_processing():
    scraper = _scraper()
    scraper._check_cancelled = lambda: None

    collected_urls = [
        "https://www.google.com/maps/place/One/data=!4m7!3m6!1s0x1:0x2!8m2!3d1!4d2!16s%2Fg%2F1!19sChIJONE?authuser=0",
        "https://www.google.com/maps/place/Two/data=!4m7!3m6!1s0x3:0x4!8m2!3d3!4d4!16s%2Fg%2F2!19sChIJTWO?authuser=0",
    ]
    scraper.page_navigator = SimpleNamespace(
        navigate_to_grid_cell=lambda cell: True,
        perform_search=lambda term: True,
        wait_for_search_results=lambda: True,
        collect_listing_urls=lambda seen_urls, target_count=None: list(collected_urls),
    )

    class _FakeProgressTracker:
        def __init__(self):
            self.seen = []
            self.count = 0
            self.cell_results = []

        def add_seen_url(self, url):
            self.seen.append(url)

        def increment_results_count(self):
            self.count += 1
            return self.count

        def add_cell_results(self, cell_id, count):
            self.cell_results.append((cell_id, count))

    tracker = _FakeProgressTracker()
    scraper.progress_tracker = tracker

    outcomes = iter([True, False])
    scraper._process_single_listing = lambda url, logger: next(outcomes)

    progress = SimpleNamespace(
        results_count=0,
        total_target=10,
        get_seen_urls_set=lambda: set(),
    )
    cell = SimpleNamespace(id="1_1")
    logger = SimpleNamespace(info=lambda *args, **kwargs: None, warning=lambda *args, **kwargs: None, error=lambda *args, **kwargs: None)

    scraper._process_single_cell(cell, "Restaurants in Münster", "Restaurants", 10, progress, logger, "coverage")

    assert tracker.seen == [collected_urls[0]]
    assert tracker.count == 1
    assert tracker.cell_results == [("1_1", 1)]


def test_process_single_listing_restores_headless_after_headful_retry():
    scraper = _scraper()
    scraper.config.settings.browser.headless = True
    scraper._check_cancelled = lambda: None
    scraper._current_search_term = "Restaurants in Münster"
    scraper._active_browser_headless = True
    scraper.page_navigator = SimpleNamespace(
        navigate_to_business=lambda url: True,
        has_limited_view=lambda: True,
    )
    business = Business(place_id="p1", name="Test", review_count=0, review_average=4.3)
    scraper.business_scraper = SimpleNamespace(extract_data=lambda url: business)
    scraper.review_hash_index = SimpleNamespace(get_reviews=lambda place_id: [], upsert_reviews=lambda reviews: None)
    scraper.review_scraper = SimpleNamespace(
        extract_data=lambda *args, **kwargs: [],
        last_collection_metadata={},
    )
    scraper.csv_writer = SimpleNamespace(write_business=lambda business: True, write_reviews=lambda reviews: None)
    scraper.owner_enrichment_service = SimpleNamespace(is_enabled=lambda: False)

    relaunch_modes = []

    def _relaunch(headless):
        relaunch_modes.append(headless)
        scraper._active_browser_headless = headless

    scraper._relaunch_browser_components = _relaunch

    logger = SimpleNamespace(
        info=lambda *args, **kwargs: None,
        warning=lambda *args, **kwargs: None,
        error=lambda *args, **kwargs: None,
        debug=lambda *args, **kwargs: None,
    )

    status, returned_business, review_count = scraper._process_single_listing(
        "https://www.google.com/maps/place/Test", logger
    )

    assert status == "new"
    assert returned_business is business
    assert review_count == 0
    assert relaunch_modes == [False, True]
    assert scraper._active_browser_headless is True


def test_process_single_listing_reraises_browser_closed_errors():
    scraper = _scraper()
    scraper._check_cancelled = lambda: None
    scraper.page_navigator = SimpleNamespace(
        navigate_to_business=lambda url: (_ for _ in ()).throw(
            RuntimeError("Page.goto: Target page, context or browser has been closed")
        )
    )
    logger = SimpleNamespace(
        info=lambda *args, **kwargs: None,
        warning=lambda *args, **kwargs: None,
        error=lambda *args, **kwargs: None,
        debug=lambda *args, **kwargs: None,
    )

    try:
        scraper._process_single_listing("https://www.google.com/maps/place/Test", logger)
    except RuntimeError as exc:
        assert "browser has been closed" in str(exc).lower()
    else:
        raise AssertionError("Expected browser-closed errors to propagate for recovery")


def test_process_grid_cells_retries_closed_browser_cell_once():
    scraper = _scraper()
    scraper.config.settings.browser.headless = True
    scraper._check_cancelled = lambda: None
    scraper.component_logger = logging.getLogger("test.google_maps_scraper.retry")
    call_count = 0

    def _process_single_cell(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("Page.goto: Target page, context or browser has been closed")

    relaunch_modes = []
    scraper._process_single_cell = _process_single_cell
    scraper._relaunch_browser_components = lambda headless: relaunch_modes.append(headless)

    completed = []
    scraper.progress_tracker = SimpleNamespace(mark_cell_completed=lambda cell_id: completed.append(cell_id))
    progress = SimpleNamespace(
        is_cell_completed=lambda cell_id: False,
        results_count=0,
        total_target=10,
        completed_cells=[],
    )
    grid_navigator = SimpleNamespace(grid_cells=[SimpleNamespace(id="1_1")])
    logger = SimpleNamespace(info=lambda *args, **kwargs: None, warning=lambda *args, **kwargs: None, error=lambda *args, **kwargs: None)

    scraper._process_grid_cells(grid_navigator, progress, "Restaurants in Münster", "Restaurants", 10, logger, "coverage")

    assert call_count == 2
    assert relaunch_modes == [True]
    assert completed == ["1_1"]


def test_process_grid_cells_raises_when_browser_recovery_fails():
    scraper = _scraper()
    scraper.config.settings.browser.headless = True
    scraper._check_cancelled = lambda: None
    scraper.component_logger = logging.getLogger("test.google_maps_scraper.fail")
    scraper._process_single_cell = lambda *args, **kwargs: (_ for _ in ()).throw(
        RuntimeError("Page.goto: Target page, context or browser has been closed")
    )
    scraper._relaunch_browser_components = lambda headless: None
    scraper.progress_tracker = SimpleNamespace(mark_cell_completed=lambda cell_id: None)
    progress = SimpleNamespace(
        is_cell_completed=lambda cell_id: False,
        results_count=0,
        total_target=10,
        completed_cells=[],
    )
    grid_navigator = SimpleNamespace(grid_cells=[SimpleNamespace(id="1_1")])
    logger = SimpleNamespace(info=lambda *args, **kwargs: None, warning=lambda *args, **kwargs: None, error=lambda *args, **kwargs: None)

    try:
        scraper._process_grid_cells(grid_navigator, progress, "Restaurants in Münster", "Restaurants", 10, logger, "coverage")
    except ScraperException as exc:
        assert "browser/page closed while processing cell 1_1" in str(exc).lower()
    else:
        raise AssertionError("Expected fatal browser-closure errors to abort the run")


def test_process_grid_cells_can_be_restricted_to_selected_cells():
    scraper = _scraper()
    scraper._check_cancelled = lambda: None
    scraper.component_logger = logging.getLogger("test.google_maps_scraper.selected_cells")
    processed = []
    scraper._process_single_cell = lambda cell, *args, **kwargs: processed.append(cell.id)
    scraper._relaunch_browser_components = lambda headless: None
    completed = []
    scraper.progress_tracker = SimpleNamespace(mark_cell_completed=lambda cell_id: completed.append(cell_id))
    progress = SimpleNamespace(
        is_cell_completed=lambda cell_id: False,
        results_count=0,
        total_target=10,
        completed_cells=[],
    )
    grid_navigator = SimpleNamespace(
        grid_cells=[SimpleNamespace(id="1_1"), SimpleNamespace(id="1_2"), SimpleNamespace(id="1_3")]
    )
    logger = SimpleNamespace(info=lambda *args, **kwargs: None, warning=lambda *args, **kwargs: None, error=lambda *args, **kwargs: None)

    scraper._process_grid_cells(
        grid_navigator,
        progress,
        "Restaurants in Münster",
        "Restaurants",
        10,
        logger,
        "coverage",
        selected_cell_ids=["1_2"],
    )

    assert processed == ["1_2"]
    assert completed == ["1_2"]
