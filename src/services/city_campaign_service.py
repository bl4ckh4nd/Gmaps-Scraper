"""CLI campaign pipeline for multi-city Google Maps scraping."""

from __future__ import annotations

import json
import re
import time
import unicodedata
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Sequence

import pandas as pd

from .category_report_service import CategoryReportService

if TYPE_CHECKING:
    from web.scraper_service import ScraperManager


DEFAULT_CAMPAIGN_CATEGORIES = (
    "Hotels",
    "Restaurants",
    "Cafes",
    "Bakeries",
    "Pharmacies",
)
DEFAULT_BOUNDS_CACHE_PATH = Path(__file__).resolve().parents[2] / "data" / "city_bounds_de.json"
NOMINATIM_ENDPOINT = "https://nominatim.openstreetmap.org/search"
NOMINATIM_USER_AGENT = "Google-Maps-Scrapper/1.0 (city campaign bounds resolver)"


@dataclass
class CampaignJobSpec:
    """A single city/category scrape queued within a campaign."""

    city: str
    category: str
    search_term: str
    bounds: Optional[tuple[float, float, float, float]] = None
    status: str = "pending"
    queue_job_id: Optional[str] = None
    results_file: Optional[str] = None
    reviews_file: Optional[str] = None
    log_file: Optional[str] = None
    error_message: Optional[str] = None

    def __post_init__(self) -> None:
        if self.bounds is not None:
            self.bounds = _coerce_bounds_tuple(self.bounds)


@dataclass
class CityCampaignOptions:
    """Configuration for a queued city/category campaign."""

    cities_file: str
    total_results_per_job: int
    output_dir: Optional[str] = None
    categories: Sequence[str] = DEFAULT_CAMPAIGN_CATEGORIES
    search_template: str = "{category} in {city}"
    grid_size: int = 2
    bounds: Optional[tuple[float, float, float, float]] = None
    bounds_cache_path: Optional[str] = None
    refresh_bounds: bool = False
    bounds_request_delay_seconds: float = 1.1
    scraping_mode: str = "fast"
    review_mode: str = "rolling_365d"
    review_window_days: int = 365
    max_reviews: Optional[int] = None
    headless: bool = True
    config_overrides: Optional[dict] = None
    smoke_test: bool = False
    smoke_cities: int = 2
    smoke_categories: int = 2
    resume: bool = False
    poll_interval_seconds: float = 2.0

    def __post_init__(self) -> None:
        if self.config_overrides is None:
            self.config_overrides = {}
        self.categories = tuple(_normalize_category_name(item) for item in self.categories if item)
        if self.bounds is not None:
            self.bounds = _coerce_bounds_tuple(self.bounds)


@dataclass
class CityCampaignResult:
    """Final artifact paths and counts from a completed campaign."""

    output_dir: str
    manifest_path: str
    business_csv: str
    reviews_csv: str
    summary_csv: Optional[str]
    total_jobs: int
    completed_jobs: int
    failed_jobs: int


class CityCampaignRunner:
    """Run a queued multi-city campaign using the existing job queue service."""

    def __init__(
        self,
        scraper_manager: Optional["ScraperManager"] = None,
        category_report_service: Optional[CategoryReportService] = None,
    ) -> None:
        if scraper_manager is None:
            from web.scraper_service import ScraperManager

            scraper_manager = ScraperManager()
        self.scraper_manager = scraper_manager
        self.category_report_service = category_report_service or CategoryReportService()

    def run(self, options: CityCampaignOptions) -> CityCampaignResult:
        output_dir = self._resolve_output_dir(options)
        jobs_dir = output_dir / "jobs"
        jobs_dir.mkdir(parents=True, exist_ok=True)

        manifest_path = output_dir / "campaign_manifest.json"
        business_csv = output_dir / "campaign_businesses.csv"
        reviews_csv = output_dir / "campaign_reviews.csv"

        manifest = self._load_or_initialize_manifest(
            options,
            manifest_path,
            business_csv,
            reviews_csv,
        )

        jobs = [CampaignJobSpec(**item) for item in manifest["jobs"]]
        total_jobs = len(jobs)

        for index, job in enumerate(jobs, start=1):
            if job.status == "completed":
                continue

            print(f"[{index}/{total_jobs}] Queueing {job.search_term}")
            job.status = "queued"
            self._persist_manifest(manifest_path, manifest, jobs)

            from web.scraper_service import JobConfig

            job_config = JobConfig(
                search_term=job.search_term,
                total_results=options.total_results_per_job,
                bounds=options.bounds or job.bounds,
                grid_size=options.grid_size,
                scraping_mode=options.scraping_mode,
                review_mode=options.review_mode,
                review_window_days=options.review_window_days,
                max_reviews=options.max_reviews,
                headless=options.headless,
                output_dir=str(jobs_dir),
                config_overrides=dict(options.config_overrides or {}),
            )
            queue_job_id = self.scraper_manager.start_job(job_config)
            job.queue_job_id = queue_job_id
            self._persist_manifest(manifest_path, manifest, jobs)

            final_status = self._wait_for_job(queue_job_id, options.poll_interval_seconds)
            job.status = final_status.status
            job.error_message = final_status.error_message
            job.results_file = final_status.results_file
            job.reviews_file = final_status.reviews_file
            job.log_file = final_status.log_file
            self._persist_manifest(manifest_path, manifest, jobs)

            if job.status == "completed":
                self._append_csv(final_status.results_file, business_csv)
                self._append_csv(final_status.reviews_file, reviews_csv)
            else:
                print(f"  -> {job.search_term} ended with status: {job.status}")

        summary_csv = self.category_report_service.build_summary(str(business_csv))
        completed_jobs = sum(1 for job in jobs if job.status == "completed")
        failed_jobs = sum(1 for job in jobs if job.status == "failed")

        manifest["summary_csv"] = summary_csv or ""
        self._persist_manifest(manifest_path, manifest, jobs)

        return CityCampaignResult(
            output_dir=str(output_dir.resolve()),
            manifest_path=str(manifest_path.resolve()),
            business_csv=str(business_csv.resolve()),
            reviews_csv=str(reviews_csv.resolve()),
            summary_csv=summary_csv,
            total_jobs=total_jobs,
            completed_jobs=completed_jobs,
            failed_jobs=failed_jobs,
        )

    def _resolve_output_dir(self, options: CityCampaignOptions) -> Path:
        if options.output_dir:
            output_dir = Path(options.output_dir).expanduser()
            if options.resume and not output_dir.exists():
                raise FileNotFoundError(f"Campaign output directory does not exist: {output_dir}")
            output_dir.mkdir(parents=True, exist_ok=True)
            return output_dir

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return (Path.cwd() / "campaign_runs" / f"cities_campaign_{timestamp}").resolve()

    def _load_or_initialize_manifest(
        self,
        options: CityCampaignOptions,
        manifest_path: Path,
        business_csv: Path,
        reviews_csv: Path,
    ) -> dict:
        if options.resume:
            if not manifest_path.exists():
                raise FileNotFoundError(f"Cannot resume without manifest: {manifest_path}")
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            jobs_payload = manifest.get("jobs", [])
            missing_bounds = [job["city"] for job in jobs_payload if not job.get("bounds")]
            if missing_bounds:
                if options.bounds is not None:
                    for job in jobs_payload:
                        if not job.get("bounds"):
                            job["bounds"] = list(options.bounds)
                else:
                    resolved_bounds = resolve_city_bounds(
                        missing_bounds,
                        cache_path=options.bounds_cache_path,
                        refresh=options.refresh_bounds,
                        request_delay_seconds=options.bounds_request_delay_seconds,
                    )
                    for job in jobs_payload:
                        if not job.get("bounds"):
                            job["bounds"] = list(resolved_bounds[job["city"]])
            manifest["updated_at"] = datetime.now().isoformat()
            return manifest

        if manifest_path.exists():
            raise FileExistsError(
                f"Campaign manifest already exists at {manifest_path}. "
                "Use --campaign-resume or choose a different --campaign-output-dir."
            )

        cities = parse_cities_markdown(options.cities_file)
        if options.bounds is not None:
            city_bounds = {city: options.bounds for city in cities}
        else:
            city_bounds = resolve_city_bounds(
                cities,
                cache_path=options.bounds_cache_path,
                refresh=options.refresh_bounds,
                request_delay_seconds=options.bounds_request_delay_seconds,
            )
        jobs = build_campaign_jobs(
            cities,
            options.categories,
            options.search_template,
            bounds_by_city=city_bounds,
            smoke_test=options.smoke_test,
            smoke_cities=options.smoke_cities,
            smoke_categories=options.smoke_categories,
        )

        manifest = {
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "options": _serialize_options(options),
            "business_csv": str(business_csv.resolve()),
            "reviews_csv": str(reviews_csv.resolve()),
            "summary_csv": "",
            "jobs": [asdict(job) for job in jobs],
        }
        manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
        return manifest

    def _persist_manifest(
        self,
        manifest_path: Path,
        manifest: dict,
        jobs: Sequence[CampaignJobSpec],
    ) -> None:
        manifest["updated_at"] = datetime.now().isoformat()
        manifest["jobs"] = [asdict(job) for job in jobs]
        manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")

    def _wait_for_job(self, job_id: str, poll_interval_seconds: float):
        last_progress = None

        while True:
            job = self.scraper_manager.get_job_status(job_id)
            if job is None:
                raise RuntimeError(f"Queue job disappeared: {job_id}")

            if job.status in {"completed", "failed", "cancelled"}:
                if job.status == "completed":
                    print("  -> completed")
                return job

            percentage = int(job.progress.get("percentage", 0))
            current = job.progress.get("current", 0)
            total = job.progress.get("total", 0)
            progress_key = (job.status, percentage, current, total)
            if progress_key != last_progress:
                print(f"  -> {job.status}: {current}/{total} ({percentage}%)")
                last_progress = progress_key

            time.sleep(poll_interval_seconds)

    def _append_csv(self, source_path: Optional[str], target_path: Path) -> None:
        if not source_path:
            return

        source = Path(source_path)
        if not source.exists():
            return

        frame = pd.read_csv(source)
        if frame.empty:
            return

        frame.to_csv(
            target_path,
            mode="a",
            header=not target_path.exists(),
            index=False,
            encoding="utf-8-sig" if not target_path.exists() else "utf-8",
        )


def parse_cities_markdown(path: str) -> list[str]:
    """Extract ordered city names from the referenced markdown file."""

    source = Path(path).expanduser()
    if not source.exists():
        raise FileNotFoundError(f"Cities file not found: {source}")

    cities: list[str] = []
    pattern = re.compile(r"^\s*(\d+)\s+(.+?)\s+r\/[^\s]+", re.IGNORECASE)
    for line in source.read_text(encoding="utf-8").splitlines():
        match = pattern.match(line.strip())
        if not match:
            continue
        city = match.group(2).strip()
        if city not in cities:
            cities.append(city)

    if not cities:
        raise ValueError(f"No city rows could be parsed from {source}")

    return cities


def load_city_bounds_cache(cache_path: Optional[str] = None) -> dict[str, dict[str, Any]]:
    """Load cached city bounds metadata."""

    path = _resolve_bounds_cache_path(cache_path)
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def save_city_bounds_cache(entries: dict[str, dict[str, Any]], cache_path: Optional[str] = None) -> None:
    """Persist city bounds metadata to disk."""

    path = _resolve_bounds_cache_path(cache_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(entries, indent=2, ensure_ascii=False), encoding="utf-8")


def resolve_city_bounds(
    cities: Sequence[str],
    *,
    cache_path: Optional[str] = None,
    refresh: bool = False,
    request_delay_seconds: float = 1.1,
) -> dict[str, tuple[float, float, float, float]]:
    """Resolve bounds for campaign cities using the cache and Nominatim fallback."""

    cache_entries = load_city_bounds_cache(cache_path)
    resolved: dict[str, tuple[float, float, float, float]] = {}
    updated_cache = False

    for city in cities:
        cache_key = _find_cached_city_key(cache_entries, city)
        if cache_key and not refresh:
            resolved[city] = _coerce_bounds_tuple(cache_entries[cache_key]["bounds"])
            continue

        entry = fetch_city_bounds_from_nominatim(city)
        cache_entries[city] = entry
        resolved[city] = _coerce_bounds_tuple(entry["bounds"])
        updated_cache = True
        if request_delay_seconds > 0:
            time.sleep(request_delay_seconds)

    if updated_cache:
        save_city_bounds_cache(cache_entries, cache_path)

    return resolved


def fetch_city_bounds_from_nominatim(
    city: str,
    *,
    country: str = "Germany",
) -> dict[str, Any]:
    """Resolve a single city bounding box from Nominatim."""

    query = urllib.parse.urlencode(
        {
            "q": f"{city}, {country}",
            "format": "jsonv2",
            "limit": 1,
            "addressdetails": 1,
        }
    )
    request = urllib.request.Request(
        f"{NOMINATIM_ENDPOINT}?{query}",
        headers={"User-Agent": NOMINATIM_USER_AGENT},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        payload = json.load(response)

    if not payload:
        raise ValueError(f"No bounds result returned for city: {city}")

    row = payload[0]
    south, north, west, east = map(float, row["boundingbox"])
    return {
        "display_name": row.get("display_name", ""),
        "osm_id": row.get("osm_id"),
        "osm_type": row.get("osm_type"),
        "lat": float(row["lat"]),
        "lng": float(row["lon"]),
        "resolved_at": datetime.now(timezone.utc).isoformat(),
        "source": "nominatim",
        "bounds": {
            "min_lat": south,
            "min_lng": west,
            "max_lat": north,
            "max_lng": east,
        },
    }


def build_campaign_jobs(
    cities: Sequence[str],
    categories: Sequence[str],
    search_template: str,
    *,
    bounds_by_city: Optional[dict[str, tuple[float, float, float, float]]] = None,
    smoke_test: bool = False,
    smoke_cities: int = 2,
    smoke_categories: int = 2,
) -> list[CampaignJobSpec]:
    """Expand cities and categories into queued campaign jobs."""

    selected_cities = list(cities)
    selected_categories = [_normalize_category_name(item) for item in categories if item]

    if smoke_test:
        selected_cities = selected_cities[:smoke_cities]
        selected_categories = selected_categories[:smoke_categories]

    jobs = []
    for city in selected_cities:
        for category in selected_categories:
            jobs.append(
                CampaignJobSpec(
                    city=city,
                    category=category,
                    search_term=search_template.format(category=category, city=city),
                    bounds=(bounds_by_city or {}).get(city),
                )
            )
    return jobs


def run_city_campaign(options: CityCampaignOptions) -> CityCampaignResult:
    """Convenience wrapper for running a city campaign."""

    runner = CityCampaignRunner()
    return runner.run(options)


def _normalize_category_name(value: str) -> str:
    cleaned = (value or "").strip()
    if not cleaned:
        return ""
    return cleaned[0].upper() + cleaned[1:]


def _normalize_city_key(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", (value or "").strip())
    normalized = "".join(char for char in normalized if not unicodedata.combining(char))
    return normalized.casefold()


def _find_cached_city_key(entries: dict[str, dict[str, Any]], city: str) -> Optional[str]:
    normalized_target = _normalize_city_key(city)
    for cached_city in entries.keys():
        if _normalize_city_key(cached_city) == normalized_target:
            return cached_city
    return None


def _coerce_bounds_tuple(bounds: Any) -> tuple[float, float, float, float]:
    if isinstance(bounds, dict):
        return (
            float(bounds["min_lat"]),
            float(bounds["min_lng"]),
            float(bounds["max_lat"]),
            float(bounds["max_lng"]),
        )
    if isinstance(bounds, (list, tuple)) and len(bounds) == 4:
        return tuple(float(item) for item in bounds)
    raise ValueError(f"Unsupported bounds payload: {bounds!r}")


def _resolve_bounds_cache_path(cache_path: Optional[str]) -> Path:
    if cache_path:
        return Path(cache_path).expanduser()
    return DEFAULT_BOUNDS_CACHE_PATH


def _serialize_options(options: CityCampaignOptions) -> dict:
    payload = asdict(options)
    payload["categories"] = list(options.categories)
    return payload
