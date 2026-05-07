import argparse
from types import SimpleNamespace

import pytest

import main_new
from src.config import Config


def _args(**overrides):
    defaults = {
        "search": None,
        "total": 50,
        "bounds": None,
        "grid": 2,
        "config": "config.yaml",
        "headless": None,
        "browser_state_file": None,
        "max_reviews": None,
        "review_mode": "rolling_365d",
        "review_window_days": 365,
        "city": None,
        "query": None,
        "cell_workers": 1,
        "city_bounds_cache": "cache.json",
        "refresh_city_bounds": False,
        "campaign_cities_file": None,
        "campaign_categories": "Hotels",
        "campaign_output_dir": None,
        "campaign_bounds_cache": "campaign-cache.json",
        "campaign_refresh_bounds": False,
        "campaign_search_template": "{category} in {city}",
        "campaign_smoke_test": False,
        "campaign_smoke_cities": 2,
        "campaign_smoke_categories": 2,
        "campaign_resume": False,
        "scraping_mode": "fast",
        "log_level": "INFO",
        "print_config": False,
        "migrate_db": False,
        "queue_start": False,
        "worker": False,
        "worker_queues": "",
        "scheduler": False,
        "scheduler_once": False,
        "runner_service": False,
        "runner_once": False,
        "export_campaign": False,
        "campaign_id": None,
        "owner_enrichment": False,
        "owner_model": None,
        "owner_max_pages": None,
        "owner_enrich_csv": None,
        "owner_output": None,
        "owner_in_place": False,
        "owner_resume": False,
        "owner_skip_existing": True,
        "extract": None,
        "skip_extract": None,
        "search_input": None,
        "cell_id": None,
        "result_file": None,
        "reviews_file": None,
        "progress_file": None,
        "log_file": None,
    }
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


def test_validate_arguments_accepts_city_mode():
    args = _args(city="Düsseldorf", query="Cafes")

    main_new.validate_arguments(args)


def test_validate_arguments_rejects_search_and_city_together():
    args = _args(city="Düsseldorf", query="Cafes", search="Cafes in Düsseldorf")

    with pytest.raises(ValueError, match="--city mode cannot be combined with --search"):
        main_new.validate_arguments(args)


def test_validate_arguments_requires_query_in_city_mode():
    args = _args(city="Düsseldorf", query=None)

    with pytest.raises(ValueError, match="--query is required in city mode"):
        main_new.validate_arguments(args)


def test_resolve_city_scrape_inputs_uses_raw_query_and_city_label(monkeypatch):
    args = _args(city="Düsseldorf", query="Cafes", city_bounds_cache="bounds.json")

    monkeypatch.setattr(
        main_new,
        "resolve_city_bounds",
        lambda cities, cache_path=None, refresh=False: {
            "Düsseldorf": (51.1, 6.6, 51.3, 6.9)
        },
    )

    label, search_input, bounds = main_new.resolve_city_scrape_inputs(args)

    assert label == "Cafes in Düsseldorf"
    assert search_input == "Cafes"
    assert bounds == (51.1, 6.6, 51.3, 6.9)


def test_validate_arguments_rejects_non_positive_cell_workers():
    args = _args(city="Düsseldorf", query="Cafes", cell_workers=0)

    with pytest.raises(ValueError, match="cell_workers must be positive"):
        main_new.validate_arguments(args)


def test_build_effective_config_applies_browser_state_file():
    args = _args(browser_state_file="state/browser-state.json")

    config = main_new.build_effective_config(args)

    assert config.settings.browser.session_state_file == "state/browser-state.json"


def test_build_effective_config_applies_extraction_group_overrides():
    args = _args(extract="contact_fields,website_modernity", skip_extract="contact_fields")

    config = main_new.build_effective_config(args)

    assert config.settings.extraction.contact_fields is False
    assert config.settings.extraction.website_modernity is True
    assert config.settings.extraction.review_rows is False


def test_validate_arguments_accepts_scheduler_mode():
    args = _args(scheduler=True)

    main_new.validate_arguments(args)


def test_main_uses_parallel_city_runner_when_cell_workers_gt_one(monkeypatch, tmp_path):
    args = _args(
        city="Münster",
        query="Restaurants",
        total=40000,
        grid=10,
        scraping_mode="coverage",
        cell_workers=4,
        config="config.yaml",
        headless=True,
    )
    effective_config = Config()
    effective_config.settings.files.result_filename = str(tmp_path / "city_businesses.csv")
    effective_config.settings.files.reviews_filename = str(tmp_path / "city_reviews.csv")
    effective_config.settings.grid.default_zoom_level = 13

    called = {}

    monkeypatch.setattr(main_new, "parse_arguments", lambda: args)
    monkeypatch.setattr(main_new, "validate_arguments", lambda parsed: None)
    monkeypatch.setattr(main_new, "build_effective_config", lambda parsed: effective_config)
    monkeypatch.setattr(
        main_new,
        "resolve_city_scrape_inputs",
        lambda parsed: ("Restaurants in Münster", "Restaurants", (51.8, 7.4, 52.0, 7.7)),
    )

    def _run_city_cell_workers(options):
        called["options"] = options
        return SimpleNamespace(
            output_dir=str(tmp_path),
            manifest_path=str(tmp_path / "city_cell_manifest.json"),
            business_csv=str(tmp_path / "city_businesses.csv"),
            reviews_csv=str(tmp_path / "city_reviews.csv"),
            summary_csv=None,
            total_cells=100,
            completed_cells=100,
            failed_cells=0,
        )

    monkeypatch.setattr(main_new, "run_city_cell_workers", _run_city_cell_workers)

    main_new.main()

    assert called["options"].cell_workers == 4
    assert called["options"].city == "Münster"
    assert called["options"].query == "Restaurants"
    assert called["options"].grid_size == 10
