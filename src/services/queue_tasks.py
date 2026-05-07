"""RQ task functions for durable scraper campaigns."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from playwright.sync_api import sync_playwright

from ..config import Config, Selectors, apply_settings_overrides
from ..navigation import PageNavigator
from ..persistence.postgres_store import PostgresStore
from ..scraper import BusinessScraper, ReviewScraper
from ..utils import resolve_chrome_binary
from ..utils.exceptions import ScraperException
from ..utils.logger import get_component_logger
from ..utils.review_analyzer import analyze_reviews
from ..utils.website_quality import assess_website_quality
from .queue_service import enqueue_scrape_listing


def _load_config(config_path: str, campaign) -> Config:
    try:
        config = Config.from_file(config_path)
    except Exception:
        config = Config()

    settings = config.settings
    settings.scraping.review_mode = campaign.review_mode
    settings.scraping.review_window_days = campaign.review_window_days
    output_dir = Path(campaign.metadata.get("output_dir") or "queue_outputs").expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    settings.files.result_filename = str(output_dir / f"{campaign.campaign_id}_businesses.csv")
    settings.files.reviews_filename = str(output_dir / f"{campaign.campaign_id}_reviews.csv")
    settings.files.progress_filename = str(output_dir / f"{campaign.campaign_id}_progress.json")
    if campaign.metadata.get("max_reviews") is not None:
        settings.scraping.max_reviews_per_business = int(campaign.metadata["max_reviews"])
    if campaign.metadata.get("headless") is not None:
        settings.browser.headless = bool(campaign.metadata["headless"])
    if campaign.metadata.get("config_overrides"):
        apply_settings_overrides(settings, campaign.metadata["config_overrides"])
    return config


def _new_context(playwright, config: Config):
    explicit_path = config.settings.browser.executable_path
    resolved_path = resolve_chrome_binary(explicit_path)
    launch_kwargs = {"headless": config.settings.browser.headless}
    if resolved_path:
        launch_kwargs["executable_path"] = resolved_path
    elif explicit_path:
        raise ScraperException(f"Configured Chrome executable not found: {explicit_path}")
    browser = playwright.chromium.launch(**launch_kwargs)
    context = browser.new_context()
    page = context.new_page()
    return browser, context, page


def _close_browser(browser, context) -> None:
    try:
        if context is not None:
            context.close()
    finally:
        if browser is not None:
            browser.close()


def _listing_mode_for_campaign(campaign) -> str:
    if campaign.review_mode == "rolling_365d":
        return "rolling_365d"
    return "tile_full"


def discover_cell_task(campaign_id: str, cell_id: str, config_path: str = "config.yaml") -> dict:
    """Discover visible listing URLs for one grid cell and enqueue listing jobs."""

    store = PostgresStore()
    store.initialize_schema()
    campaign = store.get_campaign(campaign_id)
    cell = store.get_cell(campaign_id, cell_id)
    config = _load_config(config_path, campaign)
    selectors = Selectors()
    logger = get_component_logger("QueueDiscoverCell")
    mode = _listing_mode_for_campaign(campaign)

    store.mark_campaign_status(campaign_id, "running")
    store.mark_cell_started(campaign_id, cell_id)
    store.record_attempt(
        campaign_id=campaign_id,
        cell_id=cell_id,
        task_type="discover_cell",
        status="started",
    )

    browser = None
    context = None
    try:
        with sync_playwright() as playwright:
            browser, context, page = _new_context(playwright, config)
            try:
                navigator = PageNavigator(page, config.settings, selectors)
                grid_cell = cell.to_grid_cell()
                if not navigator.navigate_to_grid_cell(grid_cell):
                    raise ScraperException(f"Failed to navigate to cell {cell_id}")
                if not navigator.perform_search(campaign.search_input_term):
                    raise ScraperException(f"Search failed in cell {cell_id}")
                if not navigator.wait_for_search_results():
                    store.mark_cell_completed(campaign_id, cell_id, 0)
                    store.record_attempt(
                        campaign_id=campaign_id,
                        cell_id=cell_id,
                        task_type="discover_cell",
                        status="completed",
                    )
                    store.refresh_campaign_status(campaign_id)
                    return {"cell_id": cell_id, "discovered": 0}

                listing_urls = navigator.collect_listing_urls(
                    seen_urls=set(),
                    target_count=min(
                        config.settings.scraping.max_listings_per_cell,
                        max(1, int(campaign.total_target or 1)),
                    ),
                )
            finally:
                _close_browser(browser, context)
                browser = None

        discovered_place_ids = []
        for url in listing_urls:
            place_id = store.upsert_listing(
                campaign_id=campaign_id,
                cell_id=cell_id,
                maps_url=url,
                mode=mode,
                review_mode=campaign.review_mode,
            )
            if place_id:
                discovered_place_ids.append(place_id)
                enqueue_scrape_listing(
                    campaign_id,
                    place_id,
                    mode=mode,
                    config_path=config_path,
                )

        store.mark_cell_completed(campaign_id, cell_id, len(discovered_place_ids))
        store.record_attempt(
            campaign_id=campaign_id,
            cell_id=cell_id,
            task_type="discover_cell",
            status="completed",
        )
        store.refresh_campaign_status(campaign_id)
        logger.info("Cell %s discovered %s listings", cell_id, len(discovered_place_ids))
        return {"cell_id": cell_id, "discovered": len(discovered_place_ids)}
    except Exception as exc:
        store.mark_cell_failed(campaign_id, cell_id, str(exc))
        store.record_attempt(
            campaign_id=campaign_id,
            cell_id=cell_id,
            task_type="discover_cell",
            status="failed",
            error=exc,
        )
        store.refresh_campaign_status(campaign_id)
        raise
    finally:
        if browser is not None:
            _close_browser(browser, context)


def scrape_listing_task(
    campaign_id: str,
    place_id: str,
    mode: str,
    config_path: str = "config.yaml",
) -> dict:
    """Scrape one listing and persist business/review state to Postgres."""

    store = PostgresStore()
    store.initialize_schema()
    campaign = store.get_campaign(campaign_id)
    listing = store.get_listing(place_id)
    config = _load_config(config_path, campaign)
    selectors = Selectors()
    logger = get_component_logger("QueueScrapeListing")

    store.mark_listing_started(campaign_id, place_id, mode)
    store.record_attempt(
        campaign_id=campaign_id,
        place_id=place_id,
        mode=mode,
        task_type="scrape_listing",
        status="started",
    )

    browser = None
    context = None
    try:
        with sync_playwright() as playwright:
            browser, context, page = _new_context(playwright, config)
            try:
                navigator = PageNavigator(page, config.settings, selectors)
                if not navigator.navigate_to_business(listing.maps_url):
                    raise ScraperException(f"Failed to navigate to listing {place_id}")

                business_scraper = BusinessScraper(page, config.settings, selectors)
                review_scraper = ReviewScraper(page, config.settings, selectors)
                business = business_scraper.extract_data(listing.maps_url)
                business.source_query = campaign.search_term
                if not business.place_id:
                    business.place_id = place_id
                if not business.name or business.name == "Extraction Failed":
                    raise ScraperException(f"Business extraction failed for {place_id}")

                extraction = config.settings.extraction
                reviews = []
                coverage_status = "not_requested"
                if _should_extract_reviews(business, config):
                    known_hashes = store.get_review_hashes(business.place_id)
                    max_reviews: Optional[int]
                    if campaign.review_mode == "rolling_365d":
                        max_reviews = None
                    elif business.review_count:
                        max_reviews = min(
                            business.review_count,
                            config.settings.scraping.max_reviews_per_business,
                        )
                    else:
                        max_reviews = config.settings.scraping.max_reviews_per_business

                    reviews = review_scraper.extract_data(
                        business.name,
                        business.address,
                        business.place_id,
                        business.review_count or None,
                        max_reviews,
                        known_hashes=known_hashes,
                    )
                    review_metadata = getattr(review_scraper, "last_collection_metadata", {})
                    coverage_status = review_metadata.get("coverage_status", "not_requested")
                    if (
                        extraction.deleted_review_signals
                        and not business.deleted_review_notice
                        and review_metadata.get("deleted_review_notice")
                    ):
                        business.deleted_review_count_min = review_metadata.get("deleted_review_count_min")
                        business.deleted_review_count_max = review_metadata.get("deleted_review_count_max")
                        business.deleted_review_notice = review_metadata.get("deleted_review_notice", "")
                    if extraction.review_analytics:
                        _apply_review_metrics(business, reviews, review_metadata, config)
                    if extraction.review_rows:
                        store.upsert_reviews(reviews)
                else:
                    reviews = []
                    coverage_status = "not_requested"
                if config.settings.extraction.website_modernity:
                    assessment = assess_website_quality(business.website)
                    business.website_status = assessment.status
                    business.website_modernity_score = assessment.modernity_score
                    business.website_modernity_reason = assessment.reason
                    business.website_uses_https = assessment.uses_https
                    business.website_mobile_friendly_hint = assessment.mobile_friendly_hint
                    business.website_structured_data_hint = assessment.structured_data_hint
                    business.website_stale_or_broken_hint = assessment.stale_or_broken_hint
                _apply_business_export_flags(business, config)
            finally:
                _close_browser(browser, context)
                browser = None

            store.upsert_business(
                campaign_id=campaign_id,
                mode=mode,
                business=business,
                coverage_status=coverage_status,
            )
            store.record_attempt(
                campaign_id=campaign_id,
                place_id=place_id,
                mode=mode,
                task_type="scrape_listing",
                status="completed",
            )
            status = store.refresh_campaign_status(campaign_id)
            logger.info("Listing %s completed in mode %s", place_id, mode)
            return {
                "place_id": place_id,
                "mode": mode,
                "reviews": len(reviews),
                "campaign_status": status,
            }
    except Exception as exc:
        store.mark_listing_failed(campaign_id, place_id, mode, str(exc))
        store.record_attempt(
            campaign_id=campaign_id,
            place_id=place_id,
            mode=mode,
            task_type="scrape_listing",
            status="failed",
            error=exc,
        )
        store.refresh_campaign_status(campaign_id)
        raise
    finally:
        if browser is not None:
            _close_browser(browser, context)


def _should_extract_reviews(business, config: Config) -> bool:
    extraction = config.settings.extraction
    if not (extraction.review_rows or extraction.review_analytics):
        return False
    return bool(
        business.review_count > 0
        or business.review_average > 0
        or (extraction.deleted_review_signals and business.deleted_review_notice)
    )


def _apply_business_export_flags(business, config: Config) -> None:
    extraction = config.settings.extraction
    business.export_contact_fields = extraction.contact_fields
    business.export_business_details = extraction.business_details
    business.export_review_summary = extraction.review_summary
    business.export_review_analytics = extraction.review_analytics
    business.export_deleted_review_signals = extraction.deleted_review_signals
    business.export_website_modernity = extraction.website_modernity


def _apply_review_metrics(business, reviews, review_metadata, config: Config) -> None:
    try:
        metrics = analyze_reviews(
            reviews,
            collection_metadata=review_metadata,
            deleted_review_bounds={
                "min": business.deleted_review_count_min,
                "max": business.deleted_review_count_max,
            },
            review_window_days=config.settings.scraping.review_window_days,
        )
    except Exception:
        return

    business.reply_rate_good = metrics["reply_rate_good"]
    business.reply_rate_bad = metrics["reply_rate_bad"]
    business.avg_time_between_reviews = metrics["avg_time_between_reviews"]
    business.reviews_last_365d_min = metrics["reviews_last_365d_min"]
    business.reviews_last_365d_max = metrics["reviews_last_365d_max"]
    business.reviews_last_365d_mid = metrics["reviews_last_365d_mid"]
    business.deleted_review_rate_min_pct = metrics["deleted_review_rate_min_pct"]
    business.deleted_review_rate_max_pct = metrics["deleted_review_rate_max_pct"]
    business.deleted_review_rate_mid_pct = metrics["deleted_review_rate_mid_pct"]
    business.review_window_coverage_status = metrics["review_window_coverage_status"]
    business.review_window_cutoff_observed = metrics["review_window_cutoff_observed"]
