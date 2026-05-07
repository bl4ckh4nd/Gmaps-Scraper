"""Dedicated headed Playwright runner for Postgres-orchestrated jobs."""

from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path
from typing import Optional

from ..config import Config, apply_settings_overrides
from ..google_maps_scraper import GoogleMapsScraper
from ..persistence.orchestrator_store import ClaimedJobExecution, OrchestratorStore
from .orchestration import OrchestrationConfig


class RunnerService:
    """Runs one Postgres-claimed scrape job at a time in a dedicated browser session."""

    def __init__(
        self,
        *,
        store: OrchestratorStore | None = None,
        config: OrchestrationConfig | None = None,
    ) -> None:
        self.store = store or OrchestratorStore()
        self.config = config or OrchestrationConfig.from_env()
        self.logger = logging.getLogger(__name__)

    def run_once(self) -> bool:
        self.store.initialize_schema()
        self.store.register_runner_node(
            runner_id=self.config.runner_id,
            hostname=self.config.runner_hostname,
            max_sessions=self.config.runner_max_sessions,
            supports_headed=self.config.runner_headed,
            novnc_base_url=self.config.runner_novnc_base_url,
            version="postgres-runner-v1",
        )
        self.store.heartbeat_runner_node(
            self.config.runner_id,
            status="idle",
            metadata={"display": self.config.runner_display},
        )

        claim = self.store.claim_waiting_job(
            runner_id=self.config.runner_id,
            max_active_browser_sessions=self.config.max_active_browser_sessions,
            headed=self.config.runner_headed,
            display_name=self.config.runner_display,
            novnc_url=self.config.runner_novnc_base_url,
            state_dir=self._session_state_dir(None, None).as_posix(),
            artifact_dir=self._artifact_dir(None, None).as_posix(),
        )
        if claim is None:
            return False

        self._execute_claimed_job(claim)
        return True

    def run_forever(self) -> None:
        self.logger.info("Starting runner loop for %s", self.config.runner_id)
        while True:
            handled = self.run_once()
            delay = (
                self.config.heartbeat_interval_seconds
                if handled
                else self.config.runner_poll_seconds
            )
            time.sleep(delay)

    def _execute_claimed_job(self, claim: ClaimedJobExecution) -> None:
        job = claim.job
        run = claim.run
        session = claim.session
        artifact_dir = self._artifact_dir(job.job_id, run.run_id)
        session_dir = self._session_state_dir(job.job_id, run.run_id)
        artifact_dir.mkdir(parents=True, exist_ok=True)
        session_dir.mkdir(parents=True, exist_ok=True)

        self.store.add_job_event(
            job.job_id,
            job_run_id=run.run_id,
            event_type="runner_starting_job",
            message=f"Runner {self.config.runner_id} is starting the browser session.",
            details={"runner_id": self.config.runner_id},
        )
        self.store.mark_job_run_running(job.job_id, run.run_id, session.session_id)
        self.store.heartbeat_runner_node(
            self.config.runner_id,
            status="busy",
            metadata={"job_id": job.job_id, "run_id": run.run_id},
        )

        paths = self._build_artifact_paths(job.job_id, run.run_id)
        stop_event = threading.Event()
        monitor = threading.Thread(
            target=self._progress_monitor,
            args=(job.job_id, run.run_id, session.session_id, paths["progress_file"], stop_event),
            daemon=True,
        )
        monitor.start()

        try:
            scraper_config = self._build_scraper_config(job, run.run_id, session.session_id, paths)
            scraper = GoogleMapsScraper(
                scraper_config,
                log_level="INFO",
                log_file=str(paths["log_file"]),
                configure_root_logger=False,
            )
            scraper.run(
                search_term=str(job.config_payload.get("search_term") or ""),
                total_results=int(job.config_payload.get("total_results") or 0),
                bounds=self._optional_bounds(job.config_payload.get("bounds")),
                grid_size=job.config_payload.get("grid_size"),
                scraping_mode=job.config_payload.get("scraping_mode") or "fast",
                should_cancel=lambda: self.store.is_job_cancel_requested(job.job_id),
            )
            stop_event.set()
            monitor.join(timeout=self.config.heartbeat_interval_seconds * 2)
            progress = self._load_progress_payload(paths["progress_file"], job)
            self._record_artifacts(job.job_id, run.run_id, paths)
            self.store.complete_job_run(
                job.job_id,
                run.run_id,
                session.session_id,
                progress_payload=progress,
                runner_id=self.config.runner_id,
            )
            self.store.add_job_event(
                job.job_id,
                job_run_id=run.run_id,
                event_type="job_completed",
                message="Runner completed the scraping job.",
            )
        except Exception as exc:
            stop_event.set()
            monitor.join(timeout=self.config.heartbeat_interval_seconds * 2)
            self._record_artifacts(job.job_id, run.run_id, paths)
            category = "cancelled" if self.store.is_job_cancel_requested(job.job_id) else exc.__class__.__name__
            action = "cancel" if category == "cancelled" else "retry"
            self.store.fail_job_run(
                job.job_id,
                run.run_id,
                session.session_id,
                error_message=str(exc),
                failure_category=category,
                recommended_action=action,
                max_job_retries=self.config.max_job_retries,
                backoff_seconds=self.config.default_backoff_seconds,
                runner_id=self.config.runner_id,
            )
            self.logger.exception("Runner failed job %s", job.job_id)

    def _progress_monitor(
        self,
        job_id: str,
        run_id: str,
        session_id: str,
        progress_path: Path,
        stop_event: threading.Event,
    ) -> None:
        while not stop_event.wait(self.config.heartbeat_interval_seconds):
            progress = self._load_progress_payload(progress_path, self.store.get_job(job_id))
            self.store.heartbeat_job_run(
                job_id,
                run_id,
                session_id,
                progress_payload=progress,
                checkpoint_payload=progress,
                runner_id=self.config.runner_id,
            )

    def _build_scraper_config(
        self,
        job,
        run_id: str,
        session_id: str,
        paths: dict[str, Path],
    ) -> Config:
        config_path = job.config_payload.get("config_path") or "config.yaml"
        try:
            config = Config.from_file(str(config_path))
        except Exception:
            config = Config()

        settings = config.settings
        overrides = dict(job.config_payload.get("config_overrides") or {})
        if overrides:
            apply_settings_overrides(settings, overrides)

        settings.browser.headless = False if self.config.runner_headed else bool(
            job.config_payload.get("headless", settings.browser.headless)
        )
        settings.browser.session_state_file = str(paths["session_state"])
        if job.config_payload.get("review_mode"):
            settings.scraping.review_mode = job.config_payload["review_mode"]
        if job.config_payload.get("review_window_days"):
            settings.scraping.review_window_days = int(job.config_payload["review_window_days"])
        if job.config_payload.get("max_reviews") is not None:
            settings.scraping.max_reviews_per_business = int(job.config_payload["max_reviews"])
        settings.files.result_filename = str(paths["result_file"])
        settings.files.reviews_filename = str(paths["reviews_file"])
        settings.files.progress_filename = str(paths["progress_file"])
        return config

    def _record_artifacts(self, job_id: str, run_id: str, paths: dict[str, Path]) -> None:
        for artifact_type, path in paths.items():
            if artifact_type == "session_state":
                mapped_type = "session_state"
            elif artifact_type == "result_file":
                mapped_type = "business_data"
            elif artifact_type == "reviews_file":
                mapped_type = "reviews_data"
            elif artifact_type == "log_file":
                mapped_type = "log_file"
            elif artifact_type == "progress_file":
                mapped_type = "progress_file"
            else:
                mapped_type = artifact_type
            if path.exists():
                self.store.add_job_artifact(
                    job_id,
                    job_run_id=run_id,
                    artifact_type=mapped_type,
                    artifact_path=str(path),
                )

    def _build_artifact_paths(self, job_id: str, run_id: str) -> dict[str, Path]:
        artifact_dir = self._artifact_dir(job_id, run_id)
        session_dir = self._session_state_dir(job_id, run_id)
        return {
            "result_file": artifact_dir / "businesses.csv",
            "reviews_file": artifact_dir / "reviews.csv",
            "progress_file": artifact_dir / "progress.json",
            "log_file": artifact_dir / "scraper.log",
            "session_state": session_dir / "browser_state.json",
        }

    def _artifact_dir(self, job_id: Optional[str], run_id: Optional[str]) -> Path:
        root = self.config.artifact_root_path()
        if not job_id or not run_id:
            return root
        return root / job_id / run_id

    def _session_state_dir(self, job_id: Optional[str], run_id: Optional[str]) -> Path:
        root = self.config.session_root_path()
        if not job_id or not run_id:
            return root
        return root / job_id / run_id

    @staticmethod
    def _optional_bounds(value) -> Optional[tuple[float, float, float, float]]:
        if not value:
            return None
        return tuple(float(item) for item in value)

    @staticmethod
    def _load_progress_payload(progress_path: Path, job) -> dict:
        payload = {
            "current": 0,
            "total": int(job.config_payload.get("total_results") or 0),
            "percentage": 0,
        }
        if not progress_path.exists():
            return payload
        try:
            data = json.loads(progress_path.read_text(encoding="utf-8"))
        except Exception:
            return payload
        total_target = int(data.get("total_target") or payload["total"] or 0)
        current = int(data.get("results_count") or 0)
        percentage = round((current / total_target) * 100, 2) if total_target else 0
        payload.update(
            {
                "current": current,
                "total": total_target,
                "percentage": percentage,
                "cells_completed": len(data.get("completed_cells") or []),
                "cell_distribution": data.get("cell_results") or {},
                "last_updated": data.get("last_updated"),
            }
        )
        return payload
