"""Shared configuration helpers for the Postgres-first job orchestrator."""

from __future__ import annotations

import os
import socket
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def use_postgres_runner_mode() -> bool:
    """Return whether the Postgres-first scheduler/runner mode is enabled."""

    return _env_flag("SCRAPER_USE_POSTGRES_RUNNER", default=False)


@dataclass(frozen=True)
class OrchestrationConfig:
    """Environment-driven settings for scheduler and runner services."""

    enabled: bool = False
    scheduler_poll_seconds: float = 5.0
    runner_poll_seconds: float = 5.0
    heartbeat_interval_seconds: float = 5.0
    heartbeat_timeout_seconds: int = 120
    max_active_jobs: int = 2
    max_active_browser_sessions: int = 2
    max_job_retries: int = 2
    default_backoff_seconds: int = 60
    runner_id: str = ""
    runner_hostname: str = ""
    runner_max_sessions: int = 1
    runner_headed: bool = True
    runner_display: str = ":99"
    runner_novnc_base_url: Optional[str] = None
    artifact_root: str = "artifacts"
    session_root: str = "sessions"

    @classmethod
    def from_env(cls) -> "OrchestrationConfig":
        hostname = socket.gethostname()
        return cls(
            enabled=use_postgres_runner_mode(),
            scheduler_poll_seconds=float(os.getenv("SCRAPER_SCHEDULER_POLL_SECONDS", "5")),
            runner_poll_seconds=float(os.getenv("SCRAPER_RUNNER_POLL_SECONDS", "5")),
            heartbeat_interval_seconds=float(os.getenv("SCRAPER_HEARTBEAT_INTERVAL_SECONDS", "5")),
            heartbeat_timeout_seconds=int(os.getenv("SCRAPER_HEARTBEAT_TIMEOUT_SECONDS", "120")),
            max_active_jobs=int(os.getenv("MAX_ACTIVE_JOBS", "2")),
            max_active_browser_sessions=int(os.getenv("MAX_ACTIVE_BROWSER_SESSIONS", "2")),
            max_job_retries=int(os.getenv("SCRAPER_MAX_JOB_RETRIES", "2")),
            default_backoff_seconds=int(os.getenv("SCRAPER_DEFAULT_BACKOFF_SECONDS", "60")),
            runner_id=os.getenv("RUNNER_ID", hostname),
            runner_hostname=hostname,
            runner_max_sessions=int(os.getenv("RUNNER_MAX_SESSIONS", "1")),
            runner_headed=_env_flag("RUNNER_HEADED", default=True),
            runner_display=os.getenv("DISPLAY", ":99"),
            runner_novnc_base_url=os.getenv("RUNNER_NOVNC_BASE_URL") or None,
            artifact_root=os.getenv("SCRAPER_ARTIFACT_ROOT", "artifacts"),
            session_root=os.getenv("SCRAPER_SESSION_ROOT", "sessions"),
        )

    def artifact_root_path(self) -> Path:
        return Path(self.artifact_root).expanduser().resolve()

    def session_root_path(self) -> Path:
        return Path(self.session_root).expanduser().resolve()
