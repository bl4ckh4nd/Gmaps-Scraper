from typing import Optional

from src.config.selectors import Selectors
from src.config.settings import ScraperSettings
from src.navigation.page_navigator import PageNavigator
from src.utils.helpers import extract_place_id


def _maps_place_url(place_id: str, slug: str) -> str:
    return (
        f"https://www.google.com/maps/place/{slug}/data=!4m7!3m6!1s0x123:0x456!"
        f"8m2!3d51.0!4d7.0!16s%2Fg%2F11abc!19s{place_id}?authuser=0&hl=de&rclk=1"
    )


class _FakeListing:
    def __init__(self, href: str):
        self.href = href

    def get_attribute(self, name: str, timeout: Optional[int] = None):
        if name == "href":
            return self.href
        return None


class _FakeLocator:
    def __init__(self, page: "_FakePage"):
        self.page = page

    def count(self) -> int:
        return len(self.page.batches[self.page.batch_index])

    def nth(self, idx: int) -> _FakeListing:
        return _FakeListing(self.page.batches[self.page.batch_index][idx])


class _FakePage:
    def __init__(self, batches: list[list[str]]):
        self.batches = batches
        self.batch_index = 0

    def wait_for_selector(self, selector: str, timeout: int = 0):
        return None

    def wait_for_timeout(self, ms: int):
        return None

    def locator(self, selector: str) -> _FakeLocator:
        return _FakeLocator(self)


class _ScrollingPageNavigator(PageNavigator):
    def _scroll_results_feed(self) -> bool:
        if self.page.batch_index < len(self.page.batches) - 1:
            self.page.batch_index += 1
            return True
        return False


class _FakeSearchElement:
    def __init__(self):
        self.value = ""

    def is_visible(self) -> bool:
        return True

    def is_enabled(self) -> bool:
        return True

    def click(self) -> None:
        return None

    def fill(self, value: str) -> None:
        self.value = value


class _DynamicLocator:
    def __init__(self, count_func, first):
        self._count_func = count_func
        self.first = first

    def count(self) -> int:
        return self._count_func()


class _FakeButtonElement:
    def is_visible(self) -> bool:
        return True

    def is_enabled(self) -> bool:
        return True


class _FakeSearchPage:
    def __init__(self, selectors: Selectors):
        self.selectors = selectors
        self.url = "https://www.google.com/maps"
        self.search_ready = False
        self.search_input = _FakeSearchElement()
        self.cookie_button = _FakeButtonElement()
        self.keyboard = type("Keyboard", (), {"press": lambda self, key: None})()

    def wait_for_timeout(self, ms: int):
        return None

    def locator(self, selector: str):
        if selector in self.selectors.SEARCH_INPUT_SELECTORS:
            return _DynamicLocator(lambda: 1 if self.search_ready else 0, self.search_input)
        if (
            selector in self.selectors.REJECT_ALL_BUTTON_SELECTORS
            or selector in self.selectors.ACCEPT_ALL_BUTTON_SELECTORS
        ):
            return _DynamicLocator(lambda: 0 if self.search_ready else 1, self.cookie_button)
        return _DynamicLocator(lambda: 0, self.cookie_button)


def test_collect_listing_urls_harvests_across_virtualized_batches():
    settings = ScraperSettings()
    settings.scraping.max_scroll_attempts = 2
    selectors = Selectors()
    page = _FakePage(
        [
            [
                _maps_place_url("ChIJPLACE001", "one"),
                _maps_place_url("ChIJPLACE002", "two"),
            ],
            [
                _maps_place_url("ChIJPLACE002", "two"),
                _maps_place_url("ChIJPLACE003", "three"),
            ],
            [
                _maps_place_url("ChIJPLACE003", "three"),
                _maps_place_url("ChIJPLACE004", "four"),
            ],
        ]
    )
    navigator = _ScrollingPageNavigator(page, settings, selectors)

    urls = navigator.collect_listing_urls(target_count=4)

    assert len(urls) == 4
    assert [extract_place_id(url) for url in urls] == [
        "ChIJPLACE001",
        "ChIJPLACE002",
        "ChIJPLACE003",
        "ChIJPLACE004",
    ]


def test_collect_listing_urls_respects_seen_ids_and_target_limit():
    settings = ScraperSettings()
    settings.scraping.max_scroll_attempts = 2
    selectors = Selectors()
    page = _FakePage(
        [
            [
                _maps_place_url("ChIJPLACE010", "ten"),
                _maps_place_url("ChIJPLACE011", "eleven"),
            ],
            [
                _maps_place_url("ChIJPLACE011", "eleven"),
                _maps_place_url("ChIJPLACE012", "twelve"),
            ],
        ]
    )
    navigator = _ScrollingPageNavigator(page, settings, selectors)

    urls = navigator.collect_listing_urls(
        seen_urls={"ChIJPLACE010"},
        target_count=2,
    )

    assert [extract_place_id(url) for url in urls] == [
        "ChIJPLACE011",
        "ChIJPLACE012",
    ]


def test_perform_search_retries_after_cookie_consent_blocks_input(monkeypatch):
    settings = ScraperSettings()
    selectors = Selectors()
    page = _FakeSearchPage(selectors)
    navigator = PageNavigator(page, settings, selectors)

    monkeypatch.setattr(navigator, "save_debug_screenshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(navigator, "save_page_html", lambda *args, **kwargs: None)
    monkeypatch.setattr(navigator, "log_available_elements", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        navigator,
        "handle_cookie_banner",
        lambda preference: setattr(page, "search_ready", True) or True,
    )

    assert navigator.perform_search("Restaurants") is True
    assert navigator._cookie_banner_handled is True
    assert page.search_input.value == "Restaurants"
