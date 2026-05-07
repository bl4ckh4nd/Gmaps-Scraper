"""SQLite-backed review hash index for incremental review scraping."""

from __future__ import annotations

import csv
import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Set

from ..models.review import Review
from ..utils.exceptions import PersistenceException


class ReviewHashIndex:
    """Persist and query known review hashes per listing."""

    HASH_VERSION_KEY = "review_hash_version"

    def __init__(self, reviews_filename: str, db_filename: str | None = None):
        self.reviews_filename = reviews_filename
        self.db_filename = db_filename or self._derive_db_filename(reviews_filename)
        self.logger = logging.getLogger(__name__)
        self._initialize()

    @staticmethod
    def _derive_db_filename(reviews_filename: str) -> str:
        path = Path(reviews_filename)
        suffix = path.suffix or ".csv"
        return str(path.with_name(f"{path.stem}_hash_index.sqlite").with_suffix(".sqlite"))

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_filename)

    def _initialize(self) -> None:
        try:
            Path(self.db_filename).parent.mkdir(parents=True, exist_ok=True)
            with self._connect() as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS review_hash_index (
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
                    CREATE INDEX IF NOT EXISTS idx_review_hash_index_place_id
                    ON review_hash_index (place_id)
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS review_hash_index_meta (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL
                    )
                    """
                )
                row_count = conn.execute(
                    "SELECT COUNT(*) FROM review_hash_index"
                ).fetchone()[0]
                if row_count == 0:
                    self._bootstrap_from_reviews_csv(conn)
                    self._set_hash_version(conn)
                    return

                stored_version = self._get_hash_version(conn)
                if stored_version != str(Review.HASH_VERSION):
                    self._rebuild_index_for_current_hash_version(conn)
                    self._set_hash_version(conn)
        except Exception as exc:
            raise PersistenceException(
                f"Failed to initialize review hash index {self.db_filename}: {exc}"
            ) from exc

    def get_hashes(self, place_id: str) -> Set[str]:
        if not place_id:
            return set()

        with self._connect() as conn:
            rows = conn.execute(
                "SELECT review_hash FROM review_hash_index WHERE place_id = ?",
                (place_id,),
            ).fetchall()
        return {row[0] for row in rows}

    def get_reviews(self, place_id: str) -> List[Review]:
        if not place_id:
            return []

        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT place_id, business_name, business_address, reviewer_name,
                       review_text, rating, review_date, owner_response, language, review_hash
                FROM review_hash_index
                WHERE place_id = ?
                """,
                (place_id,),
            ).fetchall()

        return [
            Review(
                place_id=row[0],
                business_name=row[1],
                business_address=row[2],
                reviewer_name=row[3],
                review_text=row[4],
                rating=row[5],
                review_date=row[6],
                owner_response=row[7],
                language=row[8],
                review_hash=row[9],
            )
            for row in rows
        ]

    def upsert_reviews(self, reviews: Iterable[Review]) -> int:
        prepared_reviews = [review for review in reviews if review.is_valid() and review.review_hash]
        if not prepared_reviews:
            return 0

        now = datetime.now().isoformat()
        payload = [
            (
                review.place_id,
                review.review_hash,
                review.business_name,
                review.business_address,
                review.reviewer_name,
                review.review_text,
                int(review.rating or 0),
                review.review_date,
                review.owner_response,
                review.language,
                now,
                now,
            )
            for review in prepared_reviews
        ]

        try:
            with self._connect() as conn:
                conn.executemany(
                    """
                    INSERT INTO review_hash_index (
                        place_id, review_hash, business_name, business_address,
                        reviewer_name, review_text, rating, review_date,
                        owner_response, language, first_seen_at, last_seen_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(place_id, review_hash) DO UPDATE SET
                        business_name = excluded.business_name,
                        business_address = excluded.business_address,
                        reviewer_name = excluded.reviewer_name,
                        review_text = excluded.review_text,
                        rating = excluded.rating,
                        review_date = excluded.review_date,
                        owner_response = excluded.owner_response,
                        language = excluded.language,
                        last_seen_at = excluded.last_seen_at
                    """,
                    payload,
                )
            return len(prepared_reviews)
        except Exception as exc:
            raise PersistenceException(f"Failed to upsert review hashes: {exc}") from exc

    def _bootstrap_from_reviews_csv(self, conn: sqlite3.Connection) -> None:
        prepared_reviews = self._load_reviews_from_csv()

        if not prepared_reviews:
            return

        self.logger.info(
            "Bootstrapping review hash index from %s with %s reviews",
            self.reviews_filename,
            len(prepared_reviews),
        )

        now = datetime.now().isoformat()
        conn.executemany(
            """
            INSERT OR IGNORE INTO review_hash_index (
                place_id, review_hash, business_name, business_address,
                reviewer_name, review_text, rating, review_date,
                owner_response, language, first_seen_at, last_seen_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            self._serialize_reviews(prepared_reviews, now),
        )

    def _load_reviews_from_csv(self) -> List[Review]:
        csv_path = Path(self.reviews_filename)
        if not csv_path.exists():
            return []

        prepared_reviews: List[Review] = []
        try:
            with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    review = Review.from_dict(row)
                    if review.is_valid():
                        prepared_reviews.append(review)
        except Exception as exc:
            raise PersistenceException(
                f"Failed to bootstrap review hash index from {self.reviews_filename}: {exc}"
            ) from exc

        return prepared_reviews

    def _load_reviews_from_index(self, conn: sqlite3.Connection) -> List[Review]:
        rows = conn.execute(
            """
            SELECT place_id, business_name, business_address, reviewer_name,
                   review_text, rating, review_date, owner_response, language
            FROM review_hash_index
            """
        ).fetchall()
        return [
            Review(
                place_id=row[0],
                business_name=row[1],
                business_address=row[2],
                reviewer_name=row[3],
                review_text=row[4],
                rating=row[5],
                review_date=row[6],
                owner_response=row[7],
                language=row[8],
            )
            for row in rows
        ]

    def _rebuild_index_for_current_hash_version(self, conn: sqlite3.Connection) -> None:
        reviews = self._load_reviews_from_csv() or self._load_reviews_from_index(conn)
        conn.execute("DELETE FROM review_hash_index")
        if not reviews:
            return

        self.logger.info(
            "Rebuilding review hash index %s for hash version %s",
            self.db_filename,
            Review.HASH_VERSION,
        )
        now = datetime.now().isoformat()
        conn.executemany(
            """
            INSERT OR REPLACE INTO review_hash_index (
                place_id, review_hash, business_name, business_address,
                reviewer_name, review_text, rating, review_date,
                owner_response, language, first_seen_at, last_seen_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            self._serialize_reviews(reviews, now),
        )

    @staticmethod
    def _serialize_reviews(reviews: Iterable[Review], timestamp: str) -> list[tuple]:
        return [
            (
                review.place_id,
                review.review_hash,
                review.business_name,
                review.business_address,
                review.reviewer_name,
                review.review_text,
                int(review.rating or 0),
                review.review_date,
                review.owner_response,
                review.language,
                timestamp,
                timestamp,
            )
            for review in reviews
            if review.is_valid() and review.review_hash
        ]

    def _get_hash_version(self, conn: sqlite3.Connection) -> str:
        row = conn.execute(
            "SELECT value FROM review_hash_index_meta WHERE key = ?",
            (self.HASH_VERSION_KEY,),
        ).fetchone()
        return row[0] if row else ""

    def _set_hash_version(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            INSERT INTO review_hash_index_meta (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (self.HASH_VERSION_KEY, str(Review.HASH_VERSION)),
        )
