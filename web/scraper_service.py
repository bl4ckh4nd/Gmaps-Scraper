"""Background scraper service for managing web-initiated scraping jobs."""

import json
import threading
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from queue import Queue, Empty
from dataclasses import dataclass, asdict
import os
import sys
from pathlib import Path

# Add the parent directory to the path so we can import from src
parent_dir = Path(__file__).parent.parent
sys.path.insert(0, str(parent_dir))

from src.google_maps_scraper import GoogleMapsScraper, create_scraper_from_args
from src.config import Config, apply_settings_overrides
from src.navigation import GridNavigator
from src.persistence import OrchestratorStore, PostgresStore
from src.services import OwnerCSVEnricher, OwnerCSVEnrichmentOptions
from src.services.queue_service import enqueue_discover_cell
from src.services.orchestration import use_postgres_runner_mode
from src.utils.exceptions import ScraperException


@dataclass
class JobConfig:
    """Configuration for a scraping or enrichment job."""

    search_term: Optional[str] = None
    total_results: Optional[int] = None
    bounds: Optional[Tuple[float, float, float, float]] = None
    grid_size: int = 2
    scraping_mode: str = 'fast'  # 'fast' (sequential) or 'coverage' (distributed)
    review_mode: str = 'all_available'
    review_window_days: int = 365
    max_reviews: Optional[int] = None
    headless: bool = True
    output_dir: Optional[str] = None
    config_overrides: Dict[str, Any] = None
    job_type: str = 'scrape'
    owner_csv_path: Optional[str] = None
    owner_output_path: Optional[str] = None
    owner_in_place: bool = False
    owner_resume: bool = False
    owner_model: Optional[str] = None
    owner_skip_existing: bool = True

    def __post_init__(self):
        if self.config_overrides is None:
            self.config_overrides = {}


@dataclass
class JobStatus:
    """Status information for a scraping job."""
    
    job_id: str
    status: str  # pending, running, completed, failed, cancelled
    config: JobConfig
    progress: Dict[str, Any]
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    error_message: Optional[str] = None
    results_file: Optional[str] = None
    reviews_file: Optional[str] = None
    log_file: Optional[str] = None
    
    def get_elapsed_time(self) -> str:
        """Get formatted elapsed time."""
        if not self.start_time:
            return "00:00:00"
        
        start = datetime.fromisoformat(self.start_time)
        end = datetime.fromisoformat(self.end_time) if self.end_time else datetime.now()
        
        elapsed = end - start
        hours, remainder = divmod(int(elapsed.total_seconds()), 3600)
        minutes, seconds = divmod(remainder, 60)
        
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    
    def get_estimated_remaining(self) -> Optional[str]:
        """Estimate remaining time based on current progress."""
        if not self.start_time or self.status != 'running':
            return None
        
        progress_pct = self.progress.get('percentage', 0)
        if progress_pct <= 0:
            return None
        
        start = datetime.fromisoformat(self.start_time)
        elapsed = datetime.now() - start
        
        total_estimated = elapsed / (progress_pct / 100)
        remaining = total_estimated - elapsed
        
        if remaining.total_seconds() <= 0:
            return "00:00:00"
        
        hours, remainder = divmod(int(remaining.total_seconds()), 3600)
        minutes, seconds = divmod(remainder, 60)
        
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


class ProgressCallback:
    """Callback class to capture progress from the scraper."""
    
    def __init__(self, job_id: str, scraper_manager: 'ScraperManager'):
        self.job_id = job_id
        self.scraper_manager = scraper_manager
        self.last_update = time.time()
    
    def update_progress(self, current: int, total: int, **kwargs):
        """Update job progress."""
        # Throttle updates to avoid overwhelming the system
        now = time.time()
        if now - self.last_update < 1.0:  # Update at most once per second
            return
        
        self.last_update = now
        
        progress = {
            'current': current,
            'total': total,
            'percentage': (current / total * 100) if total > 0 else 0,
            'cells_completed': kwargs.get('cells_completed', 0),
            'cells_total': kwargs.get('cells_total', 0),
            'cell_distribution': kwargs.get('cell_distribution', {}),
            'last_updated': datetime.now().isoformat()
        }
        
        self.scraper_manager.update_job_progress(self.job_id, progress)


class ScraperManager:
    """Manages multiple scraping jobs and their execution."""
    
    def __init__(self):
        self.jobs: Dict[str, JobStatus] = {}
        self.job_queue = Queue()
        self.active_threads: Dict[str, threading.Thread] = {}
        self.cancel_events: Dict[str, threading.Event] = {}
        self.lock = threading.Lock()
        
        # Start the job processor thread
        self.processor_thread = threading.Thread(target=self._process_jobs, daemon=True)
        self.processor_thread.start()
    
    def start_job(self, job_config: JobConfig) -> str:
        """Start a new scraping job."""
        if use_postgres_runner_mode() and job_config.job_type == "scrape":
            return self._start_orchestrated_scrape_job(job_config)
        if (
            os.getenv("SCRAPER_USE_RQ", "").lower() in {"1", "true", "yes"}
            and job_config.job_type == "scrape"
        ):
            return self._start_durable_scrape_job(job_config)

        job_id = str(uuid.uuid4())
        
        # Create job status
        if job_config.job_type == 'owner_enrichment':
            progress_payload = {
                'processed_rows': 0,
                'total_rows': 0,
                'owners_found': 0,
                'percentage': 0,
            }
        else:
            progress_payload = {
                'current': 0,
                'total': job_config.total_results,
                'percentage': 0,
                'cells_completed': 0,
                'cells_total': 0
            }

        job_status = JobStatus(
            job_id=job_id,
            status='pending',
            config=job_config,
            progress=progress_payload
        )
        
        with self.lock:
            self.jobs[job_id] = job_status
            self.cancel_events[job_id] = threading.Event()
            self.job_queue.put(job_id)
        
        return job_id

    def _start_durable_scrape_job(self, job_config: JobConfig) -> str:
        """Create a Postgres campaign and enqueue grid discovery jobs."""

        config_path = parent_dir / "config.yaml"
        try:
            scraper_config = Config.from_file(str(config_path))
        except Exception:
            scraper_config = Config()

        if job_config.config_overrides:
            apply_settings_overrides(scraper_config.settings, job_config.config_overrides)

        settings = scraper_config.settings
        bounds = job_config.bounds or settings.grid.default_bounds
        review_mode = job_config.review_mode or settings.scraping.review_mode
        review_window_days = job_config.review_window_days or settings.scraping.review_window_days
        scraping_mode = job_config.scraping_mode or settings.scraping.default_mode
        output_dir = Path(job_config.output_dir).expanduser() if job_config.output_dir else Path.cwd()
        output_dir.mkdir(parents=True, exist_ok=True)

        store = PostgresStore()
        store.initialize_schema()
        campaign_id = store.create_campaign(
            search_term=job_config.search_term,
            search_input_term=job_config.search_term,
            total_target=job_config.total_results,
            bounds=bounds,
            grid_size=job_config.grid_size,
            scraping_mode=scraping_mode,
            review_mode=review_mode,
            review_window_days=review_window_days,
            metadata={
                "config_path": str(config_path),
                "output_dir": str(output_dir.resolve()),
                "headless": job_config.headless,
                "max_reviews": job_config.max_reviews,
                "web_job": True,
                "config_overrides": dict(job_config.config_overrides or {}),
            },
        )

        grid = GridNavigator(
            bounds,
            job_config.grid_size,
            settings.grid.default_zoom_level,
        )
        store.create_grid_cells(campaign_id, grid.grid_cells)
        for cell in grid.grid_cells:
            enqueue_discover_cell(
                campaign_id,
                cell.id,
                config_path=str(config_path),
            )

        job_status = JobStatus(
            job_id=campaign_id,
            status='pending',
            config=job_config,
            progress={
                'current': 0,
                'total': job_config.total_results,
                'percentage': 0,
                'cells_completed': 0,
                'cells_total': len(grid.grid_cells),
                'listings_total': 0,
                'listings_completed': 0,
            },
            start_time=datetime.now().isoformat(),
        )

        with self.lock:
            self.jobs[campaign_id] = job_status

        return campaign_id

    def _start_orchestrated_scrape_job(self, job_config: JobConfig) -> str:
        """Queue a Postgres-first scrape job for headed runner execution."""

        config_path = parent_dir / "config.yaml"
        output_dir = Path(job_config.output_dir).expanduser() if job_config.output_dir else Path.cwd()
        output_dir.mkdir(parents=True, exist_ok=True)
        store = OrchestratorStore()
        store.initialize_schema()
        initial_progress = {
            'current': 0,
            'total': job_config.total_results,
            'percentage': 0,
            'cells_completed': 0,
            'cells_total': 0,
        }
        payload = asdict(job_config)
        payload["config_path"] = str(config_path)
        payload["output_dir"] = str(output_dir.resolve())
        job_id = store.queue_job(
            job_type="scrape",
            config_payload=payload,
            progress_payload=initial_progress,
            output_dir=str(output_dir.resolve()),
            created_by="web",
        )
        job_status = JobStatus(
            job_id=job_id,
            status='queued',
            config=job_config,
            progress=initial_progress,
            start_time=datetime.now().isoformat(),
        )
        with self.lock:
            self.jobs[job_id] = job_status
        return job_id
    
    def cancel_job(self, job_id: str) -> bool:
        """Cancel a pending or running job."""
        if use_postgres_runner_mode():
            self._refresh_orchestrated_jobs(limit=200)
        with self.lock:
            if job_id not in self.jobs:
                return False
            
            job = self.jobs[job_id]
            
            if job.status in {'pending', 'queued', 'waiting_for_slot', 'retry_pending', 'backoff'}:
                job.status = 'cancelled'
                job.end_time = datetime.now().isoformat()
                if use_postgres_runner_mode() and job.config.job_type == "scrape":
                    try:
                        OrchestratorStore().request_job_cancel(job_id)
                    except Exception:
                        pass
                    return True
                if os.getenv("SCRAPER_USE_RQ", "").lower() in {"1", "true", "yes"}:
                    try:
                        PostgresStore().mark_campaign_status(job_id, "cancelled")
                    except Exception:
                        pass
                return True
            elif job.status in {'running', 'starting_session'}:
                if job_id in self.cancel_events:
                    self.cancel_events[job_id].set()
                job.status = 'cancelled'
                job.end_time = datetime.now().isoformat()
                if use_postgres_runner_mode() and job.config.job_type == "scrape":
                    try:
                        OrchestratorStore().request_job_cancel(job_id)
                    except Exception:
                        pass
                    return True
                if os.getenv("SCRAPER_USE_RQ", "").lower() in {"1", "true", "yes"}:
                    try:
                        PostgresStore().mark_campaign_status(job_id, "cancelled")
                    except Exception:
                        pass
                # Note: We can't easily stop the scraper thread once started
                # In a production system, we'd need to implement cancellation tokens
                return True
            
            return False
    
    def get_job_status(self, job_id: str) -> Optional[JobStatus]:
        """Get status of a specific job."""
        with self.lock:
            job = self.jobs.get(job_id)
        if job is None and use_postgres_runner_mode():
            self._refresh_orchestrated_jobs(limit=200)
            with self.lock:
                job = self.jobs.get(job_id)
        if job:
            self._sync_orchestrated_job_status(job)
            self._sync_durable_job_status(job)
        return job
    
    def list_jobs(self, limit: Optional[int] = 50) -> List[JobStatus]:
        """List all jobs, most recent first."""
        if use_postgres_runner_mode():
            self._refresh_orchestrated_jobs(limit=limit or 50)
        with self.lock:
            jobs = list(self.jobs.values())
        for job in jobs:
            self._sync_orchestrated_job_status(job)
            self._sync_durable_job_status(job)
        # Sort by start time, most recent first
        jobs.sort(key=lambda j: j.start_time or '0000-00-00', reverse=True)
        if limit is None or limit <= 0:
            return jobs
        return jobs[:limit]
    
    def get_active_jobs(self) -> List[JobStatus]:
        """Get currently running jobs."""
        if use_postgres_runner_mode():
            self._refresh_orchestrated_jobs(limit=200)
        with self.lock:
            return [
                job
                for job in self.jobs.values()
                if job.status in {
                    'pending',
                    'queued',
                    'waiting_for_slot',
                    'starting_session',
                    'running',
                    'backoff',
                    'retry_pending',
                }
            ]
    
    def get_completed_jobs(self) -> List[JobStatus]:
        """Get completed jobs."""
        with self.lock:
            return [job for job in self.jobs.values() if job.status in ['completed', 'failed']]
    
    def update_job_progress(self, job_id: str, progress: Dict[str, Any]):
        """Update progress for a job."""
        with self.lock:
            if job_id in self.jobs:
                self.jobs[job_id].progress.update(progress)
    
    def cleanup_old_jobs(self, older_than_hours: int = 24):
        """Clean up old completed jobs."""
        cutoff = datetime.now() - timedelta(hours=older_than_hours)
        
        with self.lock:
            to_remove = []
            for job_id, job in self.jobs.items():
                if job.status in ['completed', 'failed', 'cancelled'] and job.end_time:
                    end_time = datetime.fromisoformat(job.end_time)
                    if end_time < cutoff:
                        to_remove.append(job_id)
            
            for job_id in to_remove:
                del self.jobs[job_id]
    
    def get_job_results(self, job_id: str) -> Dict[str, Optional[str]]:
        """Get file paths for job results."""
        job = self.get_job_status(job_id)
        if not job or job.status != 'completed':
            return {}

        if use_postgres_runner_mode() and job.config.job_type == "scrape":
            return self._get_orchestrated_job_results(job.job_id)

        self._ensure_durable_exports(job)
        
        return {
            'business_data': job.results_file,
            'reviews_data': job.reviews_file,
            'log_file': job.log_file
        }

    def _refresh_orchestrated_jobs(self, limit: int = 50) -> None:
        try:
            store = OrchestratorStore()
            jobs = store.list_jobs(limit=limit)
        except Exception:
            return

        with self.lock:
            for db_job in jobs:
                existing = self.jobs.get(db_job.job_id)
                payload = self._job_config_from_payload(db_job.config_payload)
                if existing is None:
                    self.jobs[db_job.job_id] = JobStatus(
                        job_id=db_job.job_id,
                        status=db_job.status,
                        config=payload,
                        progress=dict(db_job.progress_payload or {}),
                        start_time=db_job.started_at.isoformat() if db_job.started_at else db_job.created_at.isoformat(),
                        end_time=db_job.completed_at.isoformat() if db_job.completed_at else None,
                        error_message=db_job.last_error_message,
                    )
                else:
                    existing.status = db_job.status
                    existing.progress.update(db_job.progress_payload or {})
                    existing.error_message = db_job.last_error_message
                    if db_job.started_at:
                        existing.start_time = db_job.started_at.isoformat()
                    if db_job.completed_at:
                        existing.end_time = db_job.completed_at.isoformat()

    def _sync_orchestrated_job_status(self, job: JobStatus) -> None:
        if not use_postgres_runner_mode():
            return
        if job.config.job_type != "scrape":
            return
        store = OrchestratorStore()
        try:
            db_job = store.get_job(job.job_id)
        except Exception:
            return

        artifacts = store.get_job_artifacts(job.job_id)
        checkpoint = store.get_job_checkpoint(job.job_id)
        session = store.get_current_browser_session(job.job_id)
        runner = store.get_runner_node(session.runner_id) if session else None
        available_actions = self._get_orchestrated_job_actions(
            db_job.status,
            has_checkpoint=checkpoint is not None,
        )
        with self.lock:
            job.status = db_job.status
            job.progress.update(db_job.progress_payload or {})
            job.error_message = db_job.last_error_message
            job.results_file = artifacts.get("business_data")
            job.reviews_file = artifacts.get("reviews_data")
            job.log_file = artifacts.get("log_file")
            job.available_actions = available_actions
            job.orchestration = {
                "backend": "postgres_runner",
                "current_run_id": db_job.current_run_id,
                "failure_category": db_job.failure_category,
                "failure_reason": db_job.failure_reason,
                "recommended_action": db_job.recommended_action,
                "cancel_requested": db_job.cancel_requested,
                "has_checkpoint": checkpoint is not None,
                "session": {
                    "session_id": session.session_id,
                    "runner_id": session.runner_id,
                    "status": session.status,
                    "display_name": session.display_name,
                    "novnc_url": session.novnc_url,
                    "last_heartbeat_at": (
                        session.last_heartbeat_at.isoformat()
                        if session.last_heartbeat_at
                        else None
                    ),
                } if session else None,
                "runner": runner,
            }
            job.start_time = (
                db_job.started_at.isoformat()
                if db_job.started_at
                else db_job.created_at.isoformat()
            )
            if db_job.completed_at:
                job.end_time = db_job.completed_at.isoformat()

    def _get_orchestrated_job_results(self, job_id: str) -> Dict[str, Optional[str]]:
        artifacts = OrchestratorStore().get_job_artifacts(job_id)
        return {
            "business_data": artifacts.get("business_data"),
            "reviews_data": artifacts.get("reviews_data"),
            "log_file": artifacts.get("log_file"),
        }

    def get_job_operations(self, job_id: str) -> Optional[Dict[str, Any]]:
        job = self.get_job_status(job_id)
        if not job:
            return None
        if not (use_postgres_runner_mode() and job.config.job_type == "scrape"):
            return {
                "backend": "legacy",
                "available_actions": ["cancel"] if job.status in {"pending", "running"} else [],
                "events": [],
                "artifacts": [],
                "checkpoint": None,
                "session": None,
                "runner": None,
                "migration": {
                    "current_mode": "legacy",
                    "recommended_mode": "postgres_runner",
                },
            }

        store = OrchestratorStore()
        try:
            db_job = store.get_job(job_id)
        except Exception:
            return None
        checkpoint = store.get_job_checkpoint(job_id)
        session = store.get_current_browser_session(job_id)
        runner = store.get_runner_node(session.runner_id) if session else None
        return {
            "backend": "postgres_runner",
            "available_actions": self._get_orchestrated_job_actions(
                db_job.status,
                has_checkpoint=checkpoint is not None,
            ),
            "failure_category": db_job.failure_category,
            "failure_reason": db_job.failure_reason,
            "recommended_action": db_job.recommended_action,
            "current_run_id": db_job.current_run_id,
            "checkpoint": checkpoint,
            "session": {
                "session_id": session.session_id,
                "runner_id": session.runner_id,
                "status": session.status,
                "display_name": session.display_name,
                "novnc_url": session.novnc_url,
                "state_dir": session.state_dir,
                "artifact_dir": session.artifact_dir,
                "leased_at": session.leased_at.isoformat() if session.leased_at else None,
                "released_at": session.released_at.isoformat() if session.released_at else None,
                "last_heartbeat_at": (
                    session.last_heartbeat_at.isoformat()
                    if session.last_heartbeat_at
                    else None
                ),
            } if session else None,
            "runner": runner,
            "events": store.list_job_events(job_id, limit=30),
            "artifacts": store.list_job_artifacts(job_id, limit=30),
            "migration": {
                "current_mode": "postgres_runner",
                "legacy_mode_available": os.getenv("SCRAPER_USE_RQ", "").lower() in {"1", "true", "yes"},
            },
        }

    def execute_job_action(self, job_id: str, action: str) -> Dict[str, Any]:
        job = self.get_job_status(job_id)
        if not job:
            raise ValueError("Job not found")

        if action == "cancel":
            if not self.cancel_job(job_id):
                raise ValueError("Job not found or cannot be cancelled")
            return {"job_id": job_id, "status": "cancelled", "action": action}

        if not (use_postgres_runner_mode() and job.config.job_type == "scrape"):
            raise ValueError(f"Action '{action}' is only available in postgres-runner mode")

        store = OrchestratorStore()
        operations = self.get_job_operations(job_id) or {}
        available_actions = set(operations.get("available_actions") or [])
        if action not in available_actions:
            raise ValueError(f"Action '{action}' is not available for this job")

        if action in {"retry", "restart_from_checkpoint"}:
            store.requeue_job(job_id, reset_progress=False, clear_checkpoint=False)
        elif action == "restart_from_scratch":
            store.requeue_job(job_id, reset_progress=True, clear_checkpoint=True)
        else:
            raise ValueError(f"Unsupported action '{action}'")

        self._refresh_orchestrated_jobs(limit=200)
        job = self.get_job_status(job_id)
        return {
            "job_id": job_id,
            "status": job.status if job else "queued",
            "action": action,
        }

    @staticmethod
    def _get_orchestrated_job_actions(status: str, *, has_checkpoint: bool) -> List[str]:
        if status in {"pending", "queued", "waiting_for_slot", "starting_session", "running", "backoff", "retry_pending"}:
            return ["cancel"]
        if status in {"failed", "cancelled"}:
            actions = ["retry", "restart_from_scratch"]
            if has_checkpoint:
                actions.insert(1, "restart_from_checkpoint")
            return actions
        if status == "completed":
            return ["restart_from_scratch"]
        return []

    @staticmethod
    def _job_config_from_payload(payload: Dict[str, Any]) -> JobConfig:
        fields = JobConfig.__dataclass_fields__
        filtered = {key: value for key, value in (payload or {}).items() if key in fields}
        return JobConfig(**filtered)

    def _sync_durable_job_status(self, job: JobStatus) -> None:
        if os.getenv("SCRAPER_USE_RQ", "").lower() not in {"1", "true", "yes"}:
            return
        if job.config.job_type != "scrape":
            return
        if job.job_id in self.active_threads:
            return

        try:
            store = PostgresStore()
            campaign = store.get_campaign(job.job_id)
            progress = store.get_campaign_progress(job.job_id)
        except Exception:
            return

        status = campaign.status
        if status == "completed_with_errors":
            status = "completed"

        with self.lock:
            job.status = status
            job.progress.update({
                'current': progress.get('listings_completed', 0),
                'total': progress.get('listings_total') or job.config.total_results,
                'percentage': progress.get('percentage', 0),
                'cells_completed': progress.get('cells_completed', 0),
                'cells_total': progress.get('cells_total', 0),
                'cells_failed': progress.get('cells_failed', 0),
                'listings_total': progress.get('listings_total', 0),
                'listings_completed': progress.get('listings_completed', 0),
                'listings_failed': progress.get('listings_failed', 0),
            })
            if job.status in {'completed', 'failed', 'cancelled'} and not job.end_time:
                job.end_time = datetime.now().isoformat()

    def _ensure_durable_exports(self, job: JobStatus) -> None:
        if os.getenv("SCRAPER_USE_RQ", "").lower() not in {"1", "true", "yes"}:
            return
        if job.results_file and job.reviews_file:
            return
        if job.config.job_type != "scrape":
            return

        output_dir = Path(job.config.output_dir).expanduser() if job.config.output_dir else Path.cwd()
        output_dir.mkdir(parents=True, exist_ok=True)
        results_file = output_dir / f"result_{job.job_id}.csv"
        reviews_file = output_dir / f"reviews_{job.job_id}.csv"
        store = PostgresStore()
        business_csv, reviews_csv = store.export_campaign_csvs(
            job.job_id,
            business_csv=results_file,
            reviews_csv=reviews_file,
        )
        with self.lock:
            job.results_file = business_csv
            job.reviews_file = reviews_csv
    
    def _process_jobs(self):
        """Background thread that processes jobs from the queue."""
        while True:
            try:
                # Wait for a job (blocking)
                job_id = self.job_queue.get(timeout=1)
                
                with self.lock:
                    if job_id not in self.jobs:
                        continue
                    
                    job = self.jobs[job_id]
                    if job.status != 'pending':
                        continue
                    
                    # Mark job as running
                    job.status = 'running'
                    job.start_time = datetime.now().isoformat()
                
                # Start job execution in a separate thread
                thread = threading.Thread(
                    target=self._execute_job,
                    args=(job_id,),
                    daemon=True
                )
                thread.start()
                
                with self.lock:
                    self.active_threads[job_id] = thread
                
            except Empty:
                # No jobs in queue, continue
                continue
            except Exception as e:
                print(f"Error in job processor: {e}")
                continue
    
    def _execute_job(self, job_id: str):
        """Execute a single scraping job."""
        try:
            with self.lock:
                job = self.jobs[job_id]
                config = job.config

            if config.job_type == 'owner_enrichment':
                self._run_owner_enrichment_job(job_id, config)
                return

            output_dir = Path(config.output_dir).expanduser() if config.output_dir else Path.cwd()
            output_dir.mkdir(parents=True, exist_ok=True)

            # Create unique filenames for this job
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            results_file = output_dir / f"result_{job_id}_{timestamp}.csv"
            reviews_file = output_dir / f"reviews_{job_id}_{timestamp}.csv"
            progress_file = output_dir / f"progress_{job_id}_{timestamp}.json"
            log_file = output_dir / f"scraper_log_{job_id}_{timestamp}.log"
            
            # Create scraper configuration based on shared YAML
            config_path = parent_dir / "config.yaml"
            try:
                scraper_config = Config.from_file(str(config_path))
            except Exception:
                scraper_config = Config()
            
            # Apply overrides
            if config.config_overrides:
                apply_settings_overrides(scraper_config.settings, config.config_overrides)
            
            # Set headless mode
            scraper_config.settings.browser.headless = config.headless
            
            # Set max reviews if specified
            if config.max_reviews:
                scraper_config.settings.scraping.max_reviews_per_business = config.max_reviews
            scraper_config.settings.scraping.review_mode = config.review_mode
            scraper_config.settings.scraping.review_window_days = config.review_window_days
            
            # Set custom filenames
            scraper_config.settings.files.result_filename = results_file
            scraper_config.settings.files.reviews_filename = reviews_file
            scraper_config.settings.files.progress_filename = progress_file
            
            # Create scraper instance with job-specific log file; avoid
            # reconfiguring the root logger inside the web process.
            scraper = GoogleMapsScraper(
                scraper_config,
                log_file=log_file,
                configure_root_logger=False,
            )
            
            # Set up progress monitoring
            progress_callback = ProgressCallback(job_id, self)
            
            # Patch the scraper to report progress
            original_increment = scraper.progress_tracker.increment_results_count
            
            def monitored_increment(increment=1):
                result = original_increment(increment)
                progress = scraper.progress_tracker.get_current_progress()
                if progress:
                    # Get cell distribution stats
                    cell_stats = progress.get_cell_distribution_stats()
                    progress_callback.update_progress(
                        current=progress.results_count,
                        total=progress.total_target,
                        cells_completed=len(progress.completed_cells),
                        cells_total=progress.grid_size * progress.grid_size,
                        cell_distribution=cell_stats
                    )
                return result
            
            scraper.progress_tracker.increment_results_count = monitored_increment

            def should_cancel() -> bool:
                event = self.cancel_events.get(job_id)
                return bool(event and event.is_set())
            
            # Run the scraper
            scraper.run(
                search_term=config.search_term,
                total_results=config.total_results,
                bounds=config.bounds,
                grid_size=config.grid_size,
                scraping_mode=config.scraping_mode,
                should_cancel=should_cancel
            )

            if should_cancel():
                with self.lock:
                    job = self.jobs[job_id]
                    job.status = 'cancelled'
                    job.end_time = datetime.now().isoformat()
                return
            
            # Mark job as completed
            with self.lock:
                job = self.jobs[job_id]
                job.status = 'completed'
                job.end_time = datetime.now().isoformat()
                job.results_file = str(results_file.resolve())
                job.reviews_file = str(reviews_file.resolve())
                job.log_file = str(log_file.resolve())
                
                # Final progress update
                job.progress['percentage'] = 100
                job.progress['current'] = job.progress['total']
            
        except Exception as e:
            # Mark job as failed
            with self.lock:
                job = self.jobs[job_id]
                cancel_event = self.cancel_events.get(job_id)
                if cancel_event and cancel_event.is_set():
                    job.status = 'cancelled'
                    job.error_message = None
                else:
                    job.status = 'failed'
                    job.error_message = str(e)
                job.end_time = datetime.now().isoformat()
        
        finally:
            # Clean up thread reference
            with self.lock:
                if job_id in self.active_threads:
                    del self.active_threads[job_id]
                if job_id in self.cancel_events and self.jobs.get(job_id, None) and self.jobs[job_id].status != 'running':
                    del self.cancel_events[job_id]

    def _run_owner_enrichment_job(self, job_id: str, job_config: JobConfig) -> None:
        if job_config.owner_in_place and job_config.owner_resume:
            raise ValueError("owner_in_place cannot be combined with owner_resume")

        csv_path = Path(job_config.owner_csv_path).expanduser()
        if not csv_path.exists():
            raise FileNotFoundError(f"Owner enrichment CSV not found: {csv_path}")

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        if job_config.owner_output_path:
            output_path = Path(job_config.owner_output_path).expanduser()
        elif job_config.owner_in_place:
            output_path = csv_path
        else:
            output_path = csv_path.with_name(f"{csv_path.stem}_owner_enriched_{timestamp}{csv_path.suffix}")

        config_path = parent_dir / "config.yaml"
        try:
            enrichment_config = Config.from_file(str(config_path))
        except Exception:
            enrichment_config = Config()
        if job_config.config_overrides:
            apply_settings_overrides(enrichment_config.settings, job_config.config_overrides)

        total_rows = max(self._count_rows(csv_path), 0)
        self.update_job_progress(job_id, {
            'total_rows': total_rows,
            'processed_rows': 0,
            'owners_found': 0,
            'percentage': 0,
        })

        enricher = OwnerCSVEnricher(enrichment_config)

        options = OwnerCSVEnrichmentOptions(
            input_path=csv_path,
            output_path=output_path if output_path != csv_path else None,
            in_place=job_config.owner_in_place,
            resume=job_config.owner_resume,
            owner_model=job_config.owner_model,
            skip_existing=job_config.owner_skip_existing,
        )

        def progress(stats: Dict[str, int]) -> None:
            cancel_event = self.cancel_events.get(job_id)
            if cancel_event and cancel_event.is_set():
                raise ScraperException("Owner enrichment job cancelled")
            processed = stats.get('processed_rows', 0)
            owners = stats.get('owners_found', 0)
            percentage = 0
            if total_rows:
                percentage = min(100, (processed / total_rows) * 100)
            self.update_job_progress(job_id, {
                'processed_rows': processed,
                'owners_found': owners,
                'percentage': percentage,
            })

        result = enricher.enrich(options, progress_callback=progress)

        cancel_event = self.cancel_events.get(job_id)
        if cancel_event and cancel_event.is_set():
            with self.lock:
                job = self.jobs[job_id]
                job.status = 'cancelled'
                job.end_time = datetime.now().isoformat()
                job.error_message = None
            return

        with self.lock:
            job = self.jobs[job_id]
            job.status = 'completed'
            job.end_time = datetime.now().isoformat()
            resolved_output = result.output_path if result.output_path else output_path
            job.results_file = str(Path(resolved_output).expanduser().resolve())
            job.error_message = None
            job.progress.update({
                'processed_rows': result.processed_rows,
                'owners_found': result.owners_found,
                'total_rows': result.total_rows,
                'percentage': 100,
            })

    def _count_rows(self, path: Path) -> int:
        with path.open('r', encoding='utf-8', newline='') as fh:
            # subtract header
            return max(sum(1 for _ in fh) - 1, 0)


# Global scraper manager instance
scraper_manager = ScraperManager()
