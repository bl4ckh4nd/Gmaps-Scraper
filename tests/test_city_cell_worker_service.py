from pathlib import Path

import pandas as pd

from src.services.city_cell_worker_service import (
    CityCellJobSpec,
    CityCellWorkerOptions,
    CityCellWorkerRunner,
    _merge_business_frames,
    _merge_review_frames,
)


def test_merge_business_frames_deduplicates_by_place_id():
    frame_a = pd.DataFrame(
        [
            {"Place ID": "p1", "Names": "A", "Address": "Street 1"},
            {"Place ID": "p2", "Names": "B", "Address": "Street 2"},
        ]
    )
    frame_b = pd.DataFrame(
        [
            {"Place ID": "p2", "Names": "B duplicate", "Address": "Street 2"},
            {"Place ID": "p3", "Names": "C", "Address": "Street 3"},
        ]
    )

    merged = _merge_business_frames([frame_a, frame_b])

    assert merged["Place ID"].tolist() == ["p1", "p2", "p3"]


def test_merge_review_frames_deduplicates_by_review_hash():
    frame_a = pd.DataFrame(
        [
            {"place_id": "p1", "review_hash": "h1", "review_text": "A"},
            {"place_id": "p2", "review_hash": "h2", "review_text": "B"},
        ]
    )
    frame_b = pd.DataFrame(
        [
            {"place_id": "p2", "review_hash": "h2", "review_text": "B duplicate"},
            {"place_id": "p3", "review_hash": "h3", "review_text": "C"},
        ]
    )

    merged = _merge_review_frames([frame_a, frame_b])

    assert merged["review_hash"].tolist() == ["h1", "h2", "h3"]


def test_build_worker_command_uses_internal_cell_flags(tmp_path):
    runner = CityCellWorkerRunner()
    options = CityCellWorkerOptions(
        city="Münster",
        query="Restaurants",
        display_search_term="Restaurants in Münster",
        search_input_term="Restaurants",
        bounds=(51.8, 7.4, 52.0, 7.7),
        total_results=40000,
        grid_size=10,
        zoom_level=13,
        config_path="config.yaml",
        review_mode="rolling_365d",
        review_window_days=365,
        headless=True,
        cell_workers=3,
        output_dir=str(tmp_path),
        final_business_csv=str(tmp_path / "businesses.csv"),
        final_reviews_csv=str(tmp_path / "reviews.csv"),
    )
    job = CityCellJobSpec(
        cell_id="1_2",
        result_file=str(tmp_path / "cell_businesses.csv"),
        reviews_file=str(tmp_path / "cell_reviews.csv"),
        progress_file=str(tmp_path / "cell_progress.json"),
        log_file=str(tmp_path / "cell.log"),
    )

    command = runner._build_worker_command(options, job, 400)

    assert "--cell-id" in command
    assert "1_2" in command
    assert "--search-input" in command
    assert "Restaurants" in command
    assert "--result-file" in command
    assert str(Path(tmp_path / "cell_businesses.csv")) in command
    assert "--browser-state-file" in command
    assert str((tmp_path / "browser_state.json").resolve()) in command
    assert "--headless" in command
