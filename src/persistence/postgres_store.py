"""Postgres-backed durable scraper state.

CSV files remain useful exports, but this module is the queue-safe source of
truth for campaigns, cells, listings, per-mode completion, reviews, and errors.
"""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional, Union

from ..models.business import Business
from ..models.review import Review
from ..navigation.grid_navigator import GridCell
from ..utils.exceptions import PersistenceException
from ..utils.helpers import extract_place_id


DEFAULT_DATABASE_URL = "postgresql://gmaps:gmaps@localhost:5432/gmaps_scraper"


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS scrape_campaigns (
    campaign_id TEXT PRIMARY KEY,
    search_term TEXT NOT NULL,
    search_input_term TEXT NOT NULL,
    scraping_mode TEXT NOT NULL,
    review_mode TEXT NOT NULL,
    review_window_days INTEGER NOT NULL,
    bounds JSONB NOT NULL,
    grid_size INTEGER NOT NULL,
    total_target INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS grid_cells (
    campaign_id TEXT NOT NULL REFERENCES scrape_campaigns(campaign_id) ON DELETE CASCADE,
    cell_id TEXT NOT NULL,
    bounds JSONB NOT NULL,
    center_lat DOUBLE PRECISION NOT NULL,
    center_lng DOUBLE PRECISION NOT NULL,
    zoom INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    discovered_listing_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (campaign_id, cell_id)
);

CREATE TABLE IF NOT EXISTS listings (
    place_id TEXT PRIMARY KEY,
    maps_url TEXT NOT NULL,
    name TEXT,
    address TEXT,
    category TEXT,
    business_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS listing_mode_state (
    campaign_id TEXT NOT NULL REFERENCES scrape_campaigns(campaign_id) ON DELETE CASCADE,
    place_id TEXT NOT NULL REFERENCES listings(place_id) ON DELETE CASCADE,
    mode TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    discovered_cell_id TEXT,
    review_mode TEXT NOT NULL DEFAULT 'all_available',
    coverage_status TEXT NOT NULL DEFAULT 'not_requested',
    last_error TEXT,
    last_started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (campaign_id, place_id, mode)
);

CREATE TABLE IF NOT EXISTS reviews (
    place_id TEXT NOT NULL REFERENCES listings(place_id) ON DELETE CASCADE,
    review_hash TEXT NOT NULL,
    business_name TEXT NOT NULL,
    business_address TEXT NOT NULL,
    reviewer_name TEXT NOT NULL,
    review_text TEXT NOT NULL,
    rating INTEGER NOT NULL,
    review_date TEXT NOT NULL,
    owner_response TEXT NOT NULL,
    language TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (place_id, review_hash)
);

CREATE TABLE IF NOT EXISTS scrape_attempts (
    id BIGSERIAL PRIMARY KEY,
    campaign_id TEXT REFERENCES scrape_campaigns(campaign_id) ON DELETE CASCADE,
    cell_id TEXT,
    place_id TEXT,
    mode TEXT,
    task_type TEXT NOT NULL,
    status TEXT NOT NULL,
    error_type TEXT,
    error_message TEXT,
    artifact_path TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_grid_cells_status
    ON grid_cells (campaign_id, status);
CREATE INDEX IF NOT EXISTS idx_listing_mode_state_status
    ON listing_mode_state (campaign_id, mode, status);
CREATE INDEX IF NOT EXISTS idx_reviews_place_id
    ON reviews (place_id);
"""


@dataclass
class CampaignRecord:
    campaign_id: str
    search_term: str
    search_input_term: str
    scraping_mode: str
    review_mode: str
    review_window_days: int
    bounds: tuple[float, float, float, float]
    grid_size: int
    total_target: int
    status: str
    metadata: dict[str, Any]


@dataclass
class CellRecord:
    campaign_id: str
    cell_id: str
    bounds: tuple[float, float, float, float]
    center_lat: float
    center_lng: float
    zoom: int
    status: str

    def to_grid_cell(self) -> GridCell:
        min_lat, min_lng, max_lat, max_lng = self.bounds
        return GridCell(
            id=self.cell_id,
            center_lat=self.center_lat,
            center_lng=self.center_lng,
            zoom=self.zoom,
            min_lat=min_lat,
            min_lng=min_lng,
            max_lat=max_lat,
            max_lng=max_lng,
        )


@dataclass
class ListingRecord:
    place_id: str
    maps_url: str
    name: str = ""
    address: str = ""
    category: str = ""


class PostgresStore:
    """Small raw-SQL repository for queue-safe scraper state."""

    def __init__(self, database_url: Optional[str] = None) -> None:
        self.database_url = database_url or os.getenv("DATABASE_URL") or DEFAULT_DATABASE_URL

    def _connect(self):
        try:
            import psycopg
        except ImportError as exc:
            raise PersistenceException(
                "Postgres support requires psycopg. Install with `pip install -r requirements.txt`."
            ) from exc

        return psycopg.connect(self.database_url)

    def initialize_schema(self) -> None:
        try:
            with self._connect() as conn:
                conn.execute(SCHEMA_SQL)
        except Exception as exc:
            raise PersistenceException(f"Failed to initialize Postgres schema: {exc}") from exc

    def create_campaign(
        self,
        *,
        search_term: str,
        total_target: int,
        bounds: tuple[float, float, float, float],
        grid_size: int,
        scraping_mode: str,
        review_mode: str,
        review_window_days: int,
        search_input_term: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
        campaign_id: Optional[str] = None,
    ) -> str:
        campaign_id = campaign_id or str(uuid.uuid4())
        payload = (
            campaign_id,
            search_term,
            search_input_term or search_term,
            scraping_mode,
            review_mode,
            review_window_days,
            json.dumps(list(bounds)),
            grid_size,
            total_target,
            json.dumps(metadata or {}),
        )
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO scrape_campaigns (
                    campaign_id, search_term, search_input_term, scraping_mode,
                    review_mode, review_window_days, bounds, grid_size,
                    total_target, metadata
                ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s::jsonb)
                ON CONFLICT (campaign_id) DO UPDATE SET
                    updated_at = now()
                """,
                payload,
            )
        return campaign_id

    def create_grid_cells(self, campaign_id: str, cells: Iterable[GridCell]) -> None:
        rows = [
            (
                campaign_id,
                cell.id,
                json.dumps([cell.min_lat, cell.min_lng, cell.max_lat, cell.max_lng]),
                cell.center_lat,
                cell.center_lng,
                cell.zoom,
            )
            for cell in cells
        ]
        if not rows:
            return
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO grid_cells (
                        campaign_id, cell_id, bounds, center_lat, center_lng, zoom
                    ) VALUES (%s, %s, %s::jsonb, %s, %s, %s)
                    ON CONFLICT (campaign_id, cell_id) DO NOTHING
                    """,
                    rows,
                )

    def get_campaign(self, campaign_id: str) -> CampaignRecord:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT campaign_id, search_term, search_input_term, scraping_mode,
                       review_mode, review_window_days, bounds, grid_size,
                       total_target, status, metadata
                FROM scrape_campaigns
                WHERE campaign_id = %s
                """,
                (campaign_id,),
            ).fetchone()
        if row is None:
            raise PersistenceException(f"Campaign not found: {campaign_id}")
        return CampaignRecord(
            campaign_id=row[0],
            search_term=row[1],
            search_input_term=row[2],
            scraping_mode=row[3],
            review_mode=row[4],
            review_window_days=row[5],
            bounds=tuple(float(value) for value in row[6]),
            grid_size=row[7],
            total_target=row[8],
            status=row[9],
            metadata=dict(row[10] or {}),
        )

    def get_cell(self, campaign_id: str, cell_id: str) -> CellRecord:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT campaign_id, cell_id, bounds, center_lat, center_lng, zoom, status
                FROM grid_cells
                WHERE campaign_id = %s AND cell_id = %s
                """,
                (campaign_id, cell_id),
            ).fetchone()
        if row is None:
            raise PersistenceException(f"Cell not found: {campaign_id}/{cell_id}")
        return CellRecord(
            campaign_id=row[0],
            cell_id=row[1],
            bounds=tuple(float(value) for value in row[2]),
            center_lat=float(row[3]),
            center_lng=float(row[4]),
            zoom=int(row[5]),
            status=row[6],
        )

    def get_listing(self, place_id: str) -> ListingRecord:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT place_id, maps_url, COALESCE(name, ''), COALESCE(address, ''),
                       COALESCE(category, '')
                FROM listings
                WHERE place_id = %s
                """,
                (place_id,),
            ).fetchone()
        if row is None:
            raise PersistenceException(f"Listing not found: {place_id}")
        return ListingRecord(
            place_id=row[0],
            maps_url=row[1],
            name=row[2],
            address=row[3],
            category=row[4],
        )

    def list_campaigns(self, limit: int = 50) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT campaign_id, search_term, scraping_mode, review_mode, status,
                       created_at, updated_at, total_target
                FROM scrape_campaigns
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (limit,),
            ).fetchall()
        return [
            {
                "campaign_id": row[0],
                "search_term": row[1],
                "scraping_mode": row[2],
                "review_mode": row[3],
                "status": row[4],
                "created_at": row[5].isoformat() if row[5] else None,
                "updated_at": row[6].isoformat() if row[6] else None,
                "total_target": row[7],
            }
            for row in rows
        ]

    def mark_campaign_status(self, campaign_id: str, status: str) -> None:
        completed_at = "now()" if status in {"completed", "failed", "cancelled"} else "NULL"
        with self._connect() as conn:
            conn.execute(
                f"""
                UPDATE scrape_campaigns
                SET status = %s, updated_at = now(), completed_at = {completed_at}
                WHERE campaign_id = %s
                """,
                (status, campaign_id),
            )

    def mark_cell_started(self, campaign_id: str, cell_id: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE grid_cells
                SET status = 'running', attempts = attempts + 1,
                    started_at = COALESCE(started_at, now()), updated_at = now()
                WHERE campaign_id = %s AND cell_id = %s
                """,
                (campaign_id, cell_id),
            )

    def mark_cell_completed(
        self,
        campaign_id: str,
        cell_id: str,
        discovered_listing_count: int,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE grid_cells
                SET status = 'completed', discovered_listing_count = %s,
                    completed_at = now(), updated_at = now(), last_error = NULL
                WHERE campaign_id = %s AND cell_id = %s
                """,
                (discovered_listing_count, campaign_id, cell_id),
            )

    def mark_cell_failed(self, campaign_id: str, cell_id: str, error: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE grid_cells
                SET status = 'failed', last_error = %s, updated_at = now()
                WHERE campaign_id = %s AND cell_id = %s
                """,
                (error[:2000], campaign_id, cell_id),
            )

    def upsert_listing(
        self,
        *,
        campaign_id: str,
        cell_id: str,
        maps_url: str,
        mode: str,
        review_mode: str,
    ) -> Optional[str]:
        place_id = extract_place_id(maps_url)
        if not place_id:
            return None
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO listings (place_id, maps_url)
                VALUES (%s, %s)
                ON CONFLICT (place_id) DO UPDATE SET
                    maps_url = EXCLUDED.maps_url,
                    last_seen_at = now()
                """,
                (place_id, maps_url),
            )
            conn.execute(
                """
                INSERT INTO listing_mode_state (
                    campaign_id, place_id, mode, discovered_cell_id, review_mode
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (campaign_id, place_id, mode) DO UPDATE SET
                    discovered_cell_id = COALESCE(
                        listing_mode_state.discovered_cell_id,
                        EXCLUDED.discovered_cell_id
                    ),
                    review_mode = EXCLUDED.review_mode,
                    updated_at = now()
                """,
                (campaign_id, place_id, mode, cell_id, review_mode),
            )
        return place_id

    def mark_listing_started(self, campaign_id: str, place_id: str, mode: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE listing_mode_state
                SET status = 'running', attempts = attempts + 1,
                    last_started_at = now(), updated_at = now()
                WHERE campaign_id = %s AND place_id = %s AND mode = %s
                """,
                (campaign_id, place_id, mode),
            )

    def mark_listing_failed(
        self,
        campaign_id: str,
        place_id: str,
        mode: str,
        error: str,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE listing_mode_state
                SET status = 'failed', last_error = %s, updated_at = now()
                WHERE campaign_id = %s AND place_id = %s AND mode = %s
                """,
                (error[:2000], campaign_id, place_id, mode),
            )

    def upsert_business(
        self,
        *,
        campaign_id: str,
        mode: str,
        business: Business,
        coverage_status: str = "not_requested",
    ) -> None:
        payload = business.to_dict()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO listings (
                    place_id, maps_url, name, address, category, business_payload
                ) VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (place_id) DO UPDATE SET
                    maps_url = EXCLUDED.maps_url,
                    name = EXCLUDED.name,
                    address = EXCLUDED.address,
                    category = EXCLUDED.category,
                    business_payload = EXCLUDED.business_payload,
                    last_seen_at = now()
                """,
                (
                    business.place_id,
                    business.maps_url,
                    business.name,
                    business.address,
                    business.place_type,
                    json.dumps(payload, default=str),
                ),
            )
            conn.execute(
                """
                UPDATE listing_mode_state
                SET status = 'completed', coverage_status = %s, last_error = NULL,
                    completed_at = now(), updated_at = now()
                WHERE campaign_id = %s AND place_id = %s AND mode = %s
                """,
                (coverage_status, campaign_id, business.place_id, mode),
            )

    def get_review_hashes(self, place_id: str) -> set[str]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT review_hash FROM reviews WHERE place_id = %s",
                (place_id,),
            ).fetchall()
        return {row[0] for row in rows}

    def upsert_reviews(self, reviews: Iterable[Review]) -> int:
        prepared = [review for review in reviews if review.is_valid() and review.review_hash]
        if not prepared:
            return 0
        rows = [
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
            )
            for review in prepared
        ]
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO reviews (
                        place_id, review_hash, business_name, business_address,
                        reviewer_name, review_text, rating, review_date,
                        owner_response, language
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (place_id, review_hash) DO UPDATE SET
                        business_name = EXCLUDED.business_name,
                        business_address = EXCLUDED.business_address,
                        reviewer_name = EXCLUDED.reviewer_name,
                        review_text = EXCLUDED.review_text,
                        rating = EXCLUDED.rating,
                        review_date = EXCLUDED.review_date,
                        owner_response = EXCLUDED.owner_response,
                        language = EXCLUDED.language,
                        last_seen_at = now()
                    """,
                    rows,
                )
        return len(prepared)

    def record_attempt(
        self,
        *,
        task_type: str,
        status: str,
        campaign_id: Optional[str] = None,
        cell_id: Optional[str] = None,
        place_id: Optional[str] = None,
        mode: Optional[str] = None,
        error: Optional[Union[BaseException, str]] = None,
        artifact_path: Optional[str] = None,
    ) -> None:
        error_type = None
        error_message = None
        if error is not None:
            error_type = error.__class__.__name__ if isinstance(error, BaseException) else "Error"
            error_message = str(error)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO scrape_attempts (
                    campaign_id, cell_id, place_id, mode, task_type, status,
                    error_type, error_message, artifact_path
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    campaign_id,
                    cell_id,
                    place_id,
                    mode,
                    task_type,
                    status,
                    error_type,
                    (error_message or "")[:4000] if error_message else None,
                    artifact_path,
                ),
            )

    def get_campaign_progress(self, campaign_id: str) -> dict[str, Any]:
        with self._connect() as conn:
            cells = conn.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE status = 'completed') AS completed,
                    COUNT(*) FILTER (WHERE status = 'failed') AS failed,
                    COUNT(*) FILTER (WHERE status = 'running') AS running
                FROM grid_cells
                WHERE campaign_id = %s
                """,
                (campaign_id,),
            ).fetchone()
            listings = conn.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE status = 'completed') AS completed,
                    COUNT(*) FILTER (WHERE status = 'failed') AS failed,
                    COUNT(*) FILTER (WHERE status = 'running') AS running
                FROM listing_mode_state
                WHERE campaign_id = %s
                """,
                (campaign_id,),
            ).fetchone()
        listing_total = int(listings[0] or 0)
        listing_done = int(listings[1] or 0)
        percentage = (listing_done / listing_total * 100) if listing_total else 0
        return {
            "cells_total": int(cells[0] or 0),
            "cells_completed": int(cells[1] or 0),
            "cells_failed": int(cells[2] or 0),
            "cells_running": int(cells[3] or 0),
            "listings_total": listing_total,
            "listings_completed": listing_done,
            "listings_failed": int(listings[2] or 0),
            "listings_running": int(listings[3] or 0),
            "percentage": round(percentage, 2),
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }

    def refresh_campaign_status(self, campaign_id: str) -> str:
        progress = self.get_campaign_progress(campaign_id)
        if progress["cells_failed"] and progress["cells_completed"] + progress["cells_failed"] >= progress["cells_total"]:
            status = "failed" if progress["listings_completed"] == 0 else "completed_with_errors"
        elif progress["cells_total"] and progress["cells_completed"] >= progress["cells_total"] and (
            progress["listings_total"] == 0
            or progress["listings_completed"] + progress["listings_failed"] >= progress["listings_total"]
        ):
            status = "completed"
        elif progress["cells_running"] or progress["listings_running"] or progress["listings_total"]:
            status = "running"
        else:
            status = "pending"
        self.mark_campaign_status(campaign_id, status)
        return status

    def export_campaign_csvs(
        self,
        campaign_id: str,
        *,
        business_csv: Union[str, Path],
        reviews_csv: Union[str, Path],
    ) -> tuple[str, str]:
        import pandas as pd

        business_path = Path(business_csv).expanduser().resolve()
        reviews_path = Path(reviews_csv).expanduser().resolve()
        business_path.parent.mkdir(parents=True, exist_ok=True)
        reviews_path.parent.mkdir(parents=True, exist_ok=True)

        with self._connect() as conn:
            business_rows = conn.execute(
                """
                SELECT l.business_payload
                FROM listings l
                JOIN listing_mode_state s ON s.place_id = l.place_id
                WHERE s.campaign_id = %s AND s.status = 'completed'
                ORDER BY l.name NULLS LAST, l.place_id
                """,
                (campaign_id,),
            ).fetchall()
            review_rows = conn.execute(
                """
                SELECT r.place_id, r.business_name, r.business_address, r.reviewer_name,
                       r.review_text, r.rating, r.review_date, r.owner_response,
                       r.language, r.review_hash
                FROM reviews r
                JOIN listing_mode_state s ON s.place_id = r.place_id
                WHERE s.campaign_id = %s
                ORDER BY r.place_id, r.first_seen_at
                """,
                (campaign_id,),
            ).fetchall()

        business_payloads = [dict(row[0] or {}) for row in business_rows if row[0]]
        pd.DataFrame(business_payloads).to_csv(business_path, index=False, encoding="utf-8-sig")

        review_payloads = [
            {
                "place_id": row[0],
                "business_name": row[1],
                "business_address": row[2],
                "reviewer_name": row[3],
                "review_text": row[4],
                "rating": row[5],
                "review_date": row[6],
                "owner_response": row[7],
                "language": row[8],
                "review_hash": row[9],
            }
            for row in review_rows
        ]
        pd.DataFrame(review_payloads).to_csv(reviews_path, index=False, encoding="utf-8-sig")
        return str(business_path), str(reviews_path)


def get_postgres_store(database_url: Optional[str] = None) -> PostgresStore:
    return PostgresStore(database_url=database_url)
