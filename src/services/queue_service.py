"""Redis/RQ queue integration for durable scraper campaigns."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Iterable, Optional

from ..utils.exceptions import ScraperException


DEFAULT_REDIS_URL = "redis://localhost:6379/0"
DISCOVERY_QUEUE = "scraper-discovery"
LISTING_QUEUE = "scraper-listings"


@dataclass
class QueueConfig:
    redis_url: str = DEFAULT_REDIS_URL
    discovery_queue: str = DISCOVERY_QUEUE
    listing_queue: str = LISTING_QUEUE
    default_timeout_seconds: int = 3600
    default_retry_attempts: int = 2

    @classmethod
    def from_env(cls) -> "QueueConfig":
        return cls(
            redis_url=os.getenv("REDIS_URL", DEFAULT_REDIS_URL),
            discovery_queue=os.getenv("SCRAPER_DISCOVERY_QUEUE", DISCOVERY_QUEUE),
            listing_queue=os.getenv("SCRAPER_LISTING_QUEUE", LISTING_QUEUE),
            default_timeout_seconds=int(os.getenv("SCRAPER_JOB_TIMEOUT_SECONDS", "3600")),
            default_retry_attempts=int(os.getenv("SCRAPER_JOB_RETRY_ATTEMPTS", "2")),
        )


def _import_queue_dependencies():
    try:
        from redis import Redis
        from rq import Queue, Retry, Worker
    except ImportError as exc:
        raise ScraperException(
            "Redis/RQ queue support requires redis and rq. Install with `pip install -r requirements.txt`."
        ) from exc
    return Redis, Queue, Retry, Worker


def get_redis_connection(config: Optional[QueueConfig] = None):
    Redis, _, _, _ = _import_queue_dependencies()
    config = config or QueueConfig.from_env()
    return Redis.from_url(config.redis_url)


def get_queue(name: str, config: Optional[QueueConfig] = None):
    _, Queue, _, _ = _import_queue_dependencies()
    config = config or QueueConfig.from_env()
    return Queue(name, connection=get_redis_connection(config))


def enqueue_discover_cell(
    campaign_id: str,
    cell_id: str,
    *,
    config_path: str = "config.yaml",
    queue_config: Optional[QueueConfig] = None,
) -> str:
    _, _, Retry, _ = _import_queue_dependencies()
    queue_config = queue_config or QueueConfig.from_env()
    queue = get_queue(queue_config.discovery_queue, queue_config)
    job = queue.enqueue(
        "src.services.queue_tasks.discover_cell_task",
        campaign_id,
        cell_id,
        config_path,
        job_timeout=queue_config.default_timeout_seconds,
        retry=Retry(max=queue_config.default_retry_attempts),
        meta={"campaign_id": campaign_id, "cell_id": cell_id, "task_type": "discover_cell"},
    )
    return job.id


def enqueue_scrape_listing(
    campaign_id: str,
    place_id: str,
    *,
    mode: str,
    config_path: str = "config.yaml",
    queue_config: Optional[QueueConfig] = None,
) -> str:
    _, _, Retry, _ = _import_queue_dependencies()
    queue_config = queue_config or QueueConfig.from_env()
    queue = get_queue(queue_config.listing_queue, queue_config)
    job = queue.enqueue(
        "src.services.queue_tasks.scrape_listing_task",
        campaign_id,
        place_id,
        mode,
        config_path,
        job_timeout=queue_config.default_timeout_seconds,
        retry=Retry(max=queue_config.default_retry_attempts),
        meta={
            "campaign_id": campaign_id,
            "place_id": place_id,
            "mode": mode,
            "task_type": "scrape_listing",
        },
    )
    return job.id


def run_worker(
    queue_names: Optional[Iterable[str]] = None,
    *,
    queue_config: Optional[QueueConfig] = None,
) -> None:
    _, _, _, Worker = _import_queue_dependencies()
    queue_config = queue_config or QueueConfig.from_env()
    names = list(queue_names or [queue_config.discovery_queue, queue_config.listing_queue])
    worker = Worker(names, connection=get_redis_connection(queue_config))
    worker.work()
