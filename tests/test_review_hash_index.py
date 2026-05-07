import csv
import sqlite3

import pandas as pd

from src.models.review import Review
from src.persistence.csv_writer import CSVWriter
from src.persistence.review_hash_index import ReviewHashIndex


def test_review_hash_normalizes_case_and_whitespace():
    first = Review.build_review_hash("p1", "Alice", 5, "Great   stay")
    second = Review.build_review_hash("p1", "  alice ", 5, "great stay")

    assert first == second


def test_review_hash_differs_between_places_for_same_review_content():
    first = Review.build_review_hash("p1", "Alice", 5, "Great stay")
    second = Review.build_review_hash("p2", "Alice", 5, "Great stay")

    assert first != second


def test_review_hash_index_bootstraps_from_existing_reviews_csv(tmp_path):
    reviews_csv = tmp_path / "reviews.csv"
    with reviews_csv.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "place_id",
                "business_name",
                "business_address",
                "reviewer_name",
                "review_text",
                "rating",
                "review_date",
                "owner_response",
                "language",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "place_id": "p1",
                "business_name": "Hotel",
                "business_address": "Addr",
                "reviewer_name": "Alice",
                "review_text": "Great stay",
                "rating": 5,
                "review_date": "vor 2 Monaten",
                "owner_response": "",
                "language": "en",
            }
        )

    index = ReviewHashIndex(str(reviews_csv))

    stored_hashes = index.get_hashes("p1")
    stored_reviews = index.get_reviews("p1")

    assert len(stored_hashes) == 1
    assert len(stored_reviews) == 1
    assert stored_reviews[0].review_hash in stored_hashes


def test_review_hash_index_migrates_existing_sqlite_hashes(tmp_path):
    reviews_csv = tmp_path / "reviews.csv"
    db_path = tmp_path / "reviews_hash_index.sqlite"
    old_hash = Review.build_review_hash("", "Alice", 5, "Great stay")

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE review_hash_index (
                place_id TEXT NOT NULL,
                review_hash TEXT NOT NULL,
                business_name TEXT NOT NULL,
                business_address TEXT NOT NULL,
                reviewer_name TEXT NOT NULL,
                review_text TEXT NOT NULL,
                rating INTEGER NOT NULL,
                review_date TEXT NOT NULL,
                owner_response TEXT NOT NULL,
                language TEXT NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                PRIMARY KEY (place_id, review_hash)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO review_hash_index (
                place_id, review_hash, business_name, business_address,
                reviewer_name, review_text, rating, review_date,
                owner_response, language, first_seen_at, last_seen_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "p1",
                old_hash,
                "Hotel",
                "Addr",
                "Alice",
                "Great stay",
                5,
                "vor 2 Monaten",
                "",
                "en",
                "2026-01-01T00:00:00",
                "2026-01-01T00:00:00",
            ),
        )

    index = ReviewHashIndex(str(reviews_csv), db_filename=str(db_path))
    migrated_hashes = index.get_hashes("p1")
    current_hash = Review.build_review_hash("p1", "Alice", 5, "Great stay")

    assert migrated_hashes == {current_hash}


def test_review_hash_index_upserts_and_reads_reviews(tmp_path):
    reviews_csv = tmp_path / "reviews.csv"
    index = ReviewHashIndex(str(reviews_csv))
    review = Review(
        place_id="p1",
        business_name="Hotel",
        business_address="Addr",
        reviewer_name="Alice",
        review_text="Great stay",
        rating=5,
        review_date="vor 2 Monaten",
        owner_response="Thanks!",
        language="en",
    )

    written = index.upsert_reviews([review])

    assert written == 1
    assert review.review_hash in index.get_hashes("p1")
    loaded = index.get_reviews("p1")
    assert len(loaded) == 1
    assert loaded[0].owner_response == "Thanks!"


def test_csv_writer_upgrades_existing_reviews_with_blank_text_fields(tmp_path):
    reviews_csv = tmp_path / "reviews.csv"
    pd.DataFrame(
        [
            {
                "place_id": "p1",
                "business_name": "Hotel",
                "business_address": "Addr",
                "reviewer_name": "Alice",
                "review_text": "Great stay",
                "rating": 5,
                "review_date": "vor 2 Monaten",
                "owner_response": "",
                "language": "en",
                "review_hash": "",
            }
        ]
    ).to_csv(reviews_csv, index=False)

    writer = CSVWriter(reviews_filename=str(reviews_csv))
    new_review = Review(
        place_id="p1",
        business_name="Hotel",
        business_address="Addr",
        reviewer_name="Bob",
        review_text="Nice staff",
        rating=4,
        review_date="vor 1 Monat",
    )

    written = writer.write_reviews([new_review])

    assert written == 1
    upgraded = pd.read_csv(reviews_csv).fillna("")
    assert len(upgraded) == 2
    assert upgraded.iloc[0]["review_hash"] == Review.build_review_hash(
        "p1", "Alice", 5, "Great stay"
    )
    assert upgraded.iloc[1]["review_hash"] == new_review.review_hash
