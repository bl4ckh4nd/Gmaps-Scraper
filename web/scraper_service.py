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
from src.config import Config
from src.utils.exceptions import ScraperException


@dataclass
class JobConfig:
    """Configuration for a scraping job."""
    
    search_term: str
    total_results: int
    bounds: Optional[Tuple[float, float, float, float]] = None
    grid_size: int = 2
    scraping_mode: str = 'fast'  # 'fast' (sequential) or 'coverage' (distributed)
    max_reviews: Optional[int] = None
    headless: bool = True
    config_overrides: Dict[str, Any] = None
    
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
        self.lock = threading.Lock()
        
        # Start the job processor thread
        self.processor_thread = threading.Thread(target=self._process_jobs, daemon=True)
        self.processor_thread.start()
    
    def start_job(self, job_config: JobConfig) -> str:
        """Start a new scraping job."""
        job_id = str(uuid.uuid4())
        
        # Create job status
        job_status = JobStatus(
            job_id=job_id,
            status='pending',
            config=job_config,
            progress={
                'current': 0,
                'total': job_config.total_results,
                'percentage': 0,
                'cells_completed': 0,
                'cells_total': 0
            }
        )
        
        with self.lock:
            self.jobs[job_id] = job_status
            self.job_queue.put(job_id)
        
        return job_id
    
    def cancel_job(self, job_id: str) -> bool:
        """Cancel a pending or running job."""
        with self.lock:
            if job_id not in self.jobs:
                return False
            
            job = self.jobs[job_id]
            
            if job.status == 'pending':
                job.status = 'cancelled'
                job.end_time = datetime.now().isoformat()
                return True
            elif job.status == 'running':
                job.status = 'cancelled'
                job.end_time = datetime.now().isoformat()
                # Note: We can't easily stop the scraper thread once started
                # In a production system, we'd need to implement cancellation tokens
                return True
            
            return False
    
    def get_job_status(self, job_id: str) -> Optional[JobStatus]:
        """Get status of a specific job."""
        with self.lock:
            return self.jobs.get(job_id)
    
    def list_jobs(self, limit: int = 50) -> List[JobStatus]:
        """List all jobs, most recent first."""
        with self.lock:
            jobs = list(self.jobs.values())
            # Sort by start time, most recent first
            jobs.sort(key=lambda j: j.start_time or '0000-00-00', reverse=True)
            return jobs[:limit]
    
    def get_active_jobs(self) -> List[JobStatus]:
        """Get currently running jobs."""
        with self.lock:
            return [job for job in self.jobs.values() if job.status == 'running']
    
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
    
    def get_job_results(self, job_id: str) -> Dict[str, str]:
        """Get file paths for job results."""
        job = self.get_job_status(job_id)
        if not job or job.status != 'completed':
            return {}
        
        return {
            'business_data': job.results_file,
            'reviews_data': job.reviews_file,
            'log_file': job.log_file
        }
    
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
            
            # Create unique filenames for this job
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            results_file = f"result_{job_id}_{timestamp}.csv"
            reviews_file = f"reviews_{job_id}_{timestamp}.csv"
            progress_file = f"progress_{job_id}_{timestamp}.json"
            
            # Create scraper configuration
            scraper_config = Config()
            
            # Apply overrides
            if config.config_overrides:
                for key, value in config.config_overrides.items():
                    if hasattr(scraper_config.settings, key):
                        setattr(scraper_config.settings, key, value)
            
            # Set headless mode
            scraper_config.settings.browser.headless = config.headless
            
            # Set max reviews if specified
            if config.max_reviews:
                scraper_config.settings.scraping.max_reviews_per_business = config.max_reviews
            
            # Set custom filenames
            scraper_config.settings.files.result_filename = results_file
            scraper_config.settings.files.reviews_filename = reviews_file
            scraper_config.settings.files.progress_filename = progress_file
            
            # Create scraper instance
            scraper = GoogleMapsScraper(scraper_config)
            
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
            
            # Run the scraper
            scraper.run(
                search_term=config.search_term,
                total_results=config.total_results,
                bounds=config.bounds,
                grid_size=config.grid_size,
                scraping_mode=config.scraping_mode
            )
            
            # Mark job as completed
            with self.lock:
                job = self.jobs[job_id]
                job.status = 'completed'
                job.end_time = datetime.now().isoformat()
                job.results_file = results_file
                job.reviews_file = reviews_file
                job.log_file = f"scraper_log_{timestamp}.log"
                
                # Final progress update
                job.progress['percentage'] = 100
                job.progress['current'] = job.progress['total']
            
        except Exception as e:
            # Mark job as failed
            with self.lock:
                job = self.jobs[job_id]
                job.status = 'failed'
                job.end_time = datetime.now().isoformat()
                job.error_message = str(e)
        
        finally:
            # Clean up thread reference
            with self.lock:
                if job_id in self.active_threads:
                    del self.active_threads[job_id]


# Global scraper manager instance
scraper_manager = ScraperManager()