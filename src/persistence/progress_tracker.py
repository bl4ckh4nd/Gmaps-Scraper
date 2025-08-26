"""Progress tracking for scraping jobs."""

import json
import os
from dataclasses import dataclass, asdict
from typing import List, Optional, Tuple, Set
from pathlib import Path
import logging

from ..utils.exceptions import PersistenceException


@dataclass
class JobProgress:
    """Data structure for tracking job progress."""
    
    completed_cells: List[str]
    seen_urls: List[str]  # Store as list for JSON serialization
    results_count: int
    search_term: str
    bounds: List[float]
    grid_size: int
    total_target: int
    scraping_mode: str = 'fast'  # Track the scraping mode used
    cell_results: Optional[dict] = None  # Track results per cell {cell_id: count}
    start_time: Optional[str] = None
    last_updated: Optional[str] = None
    
    def __post_init__(self):
        """Initialize timestamps if not provided."""
        import datetime
        current_time = datetime.datetime.now().isoformat()
        
        if self.start_time is None:
            self.start_time = current_time
        
        if self.cell_results is None:
            self.cell_results = {}
        
        self.last_updated = current_time
    
    def get_seen_urls_set(self) -> Set[str]:
        """Get seen URLs as a set for efficient lookup."""
        return set(self.seen_urls)
    
    def add_seen_url(self, url: str) -> None:
        """Add a URL to the seen list."""
        if url not in self.seen_urls:
            self.seen_urls.append(url)
    
    def add_seen_urls(self, urls: List[str]) -> None:
        """Add multiple URLs to the seen list."""
        for url in urls:
            self.add_seen_url(url)
    
    def mark_cell_completed(self, cell_id: str) -> None:
        """Mark a grid cell as completed."""
        if cell_id not in self.completed_cells:
            self.completed_cells.append(cell_id)
    
    def is_cell_completed(self, cell_id: str) -> bool:
        """Check if a grid cell is completed."""
        return cell_id in self.completed_cells
    
    def get_progress_percentage(self) -> float:
        """Calculate progress as percentage of target."""
        if self.total_target <= 0:
            return 0.0
        return min(100.0, (self.results_count / self.total_target) * 100)
    
    def is_same_job(self, search_term: str, bounds: Tuple[float, float, float, float], 
                    grid_size: int) -> bool:
        """Check if this progress matches the given job parameters."""
        return (self.search_term == search_term and 
                self.bounds == list(bounds) and 
                self.grid_size == grid_size)
    
    def add_cell_results(self, cell_id: str, count: int) -> None:
        """Track results collected from specific cell."""
        if cell_id not in self.cell_results:
            self.cell_results[cell_id] = 0
        self.cell_results[cell_id] += count
    
    def get_cell_distribution_stats(self) -> dict:
        """Get statistics about result distribution across cells."""
        if not self.cell_results:
            return {
                'cells_with_results': 0,
                'min_per_cell': 0,
                'max_per_cell': 0,
                'avg_per_cell': 0,
                'total_cells_expected': self.grid_size * self.grid_size,
                'distribution': {}
            }
        
        values = [v for v in self.cell_results.values() if v > 0]
        return {
            'cells_with_results': len(values),
            'min_per_cell': min(values) if values else 0,
            'max_per_cell': max(values) if values else 0,
            'avg_per_cell': sum(values) / len(values) if values else 0,
            'total_cells_expected': self.grid_size * self.grid_size,
            'distribution': self.cell_results
        }


class ProgressTracker:
    """Manages job progress tracking and persistence."""
    
    def __init__(self, filename: str = 'scraper_progress.json'):
        """Initialize progress tracker.
        
        Args:
            filename: Name of the progress file
        """
        self.filename = filename
        self.logger = logging.getLogger(__name__)
        self._current_progress: Optional[JobProgress] = None
    
    def load_progress(self) -> JobProgress:
        """Load progress from file or create new progress.
        
        Returns:
            JobProgress instance
            
        Raises:
            PersistenceException: If loading fails
        """
        if os.path.exists(self.filename):
            try:
                with open(self.filename, 'r') as f:
                    data = json.load(f)
                
                progress = JobProgress(
                    completed_cells=data.get("completed_cells", []),
                    seen_urls=data.get("seen_urls", []),
                    results_count=data.get("results_count", 0),
                    search_term=data.get("search_term", ""),
                    bounds=data.get("bounds", []),
                    grid_size=data.get("grid_size", 0),
                    total_target=data.get("total_target", 0),
                    scraping_mode=data.get("scraping_mode", "fast"),
                    cell_results=data.get("cell_results", {}),
                    start_time=data.get("start_time"),
                    last_updated=data.get("last_updated")
                )
                
                self._current_progress = progress
                self.logger.info(f"Loaded progress: {progress.results_count}/{progress.total_target} results")
                return progress
                
            except Exception as e:
                raise PersistenceException(f"Failed to load progress from {self.filename}: {e}") from e
        
        # Return empty progress if file doesn't exist
        progress = JobProgress(
            completed_cells=[],
            seen_urls=[],
            results_count=0,
            search_term="",
            bounds=[],
            grid_size=0,
            total_target=0
        )
        self._current_progress = progress
        return progress
    
    def save_progress(self, progress: Optional[JobProgress] = None) -> None:
        """Save progress to file.
        
        Args:
            progress: Progress to save, uses current if None
            
        Raises:
            PersistenceException: If saving fails
        """
        progress = progress or self._current_progress
        if progress is None:
            raise PersistenceException("No progress to save")
        
        # Update timestamp
        import datetime
        progress.last_updated = datetime.datetime.now().isoformat()
        
        try:
            with open(self.filename, 'w') as f:
                json.dump(asdict(progress), f, indent=2)
            
            self.logger.debug(f"Saved progress to {self.filename}")
            
        except Exception as e:
            raise PersistenceException(f"Failed to save progress to {self.filename}: {e}") from e
    
    def initialize_job(self, search_term: str, bounds: Tuple[float, float, float, float],
                      grid_size: int, total_target: int, scraping_mode: str = 'fast') -> JobProgress:
        """Initialize progress for a new job or continue existing one.
        
        Args:
            search_term: Search query
            bounds: Geographic bounds
            grid_size: Grid size
            total_target: Target number of results
            
        Returns:
            JobProgress instance for the job
        """
        existing_progress = self.load_progress()
        
        # Check if this is the same job
        if existing_progress.is_same_job(search_term, bounds, grid_size):
            self.logger.info("Continuing existing job")
            # Update target if it changed
            existing_progress.total_target = total_target
            self._current_progress = existing_progress
            return existing_progress
        
        # Create new job progress
        self.logger.info("Starting new job")
        progress = JobProgress(
            completed_cells=[],
            seen_urls=[],
            results_count=0,
            search_term=search_term,
            bounds=list(bounds),
            grid_size=grid_size,
            total_target=total_target,
            scraping_mode=scraping_mode,
            cell_results={}
        )
        
        self._current_progress = progress
        self.save_progress(progress)
        return progress
    
    def update_progress(self, results_count: Optional[int] = None,
                       seen_urls: Optional[List[str]] = None,
                       completed_cells: Optional[List[str]] = None) -> None:
        """Update current progress with new data.
        
        Args:
            results_count: New results count
            seen_urls: URLs to add to seen list
            completed_cells: Cell IDs to mark as completed
        """
        if self._current_progress is None:
            self.logger.warning("No current progress to update")
            return
        
        if results_count is not None:
            self._current_progress.results_count = results_count
        
        if seen_urls:
            self._current_progress.add_seen_urls(seen_urls)
        
        if completed_cells:
            for cell_id in completed_cells:
                self._current_progress.mark_cell_completed(cell_id)
        
        self.save_progress()
    
    def add_seen_url(self, url: str) -> None:
        """Add a single URL to the seen list."""
        if self._current_progress is None:
            return
        
        self._current_progress.add_seen_url(url)
        self.save_progress()
    
    def mark_cell_completed(self, cell_id: str) -> None:
        """Mark a grid cell as completed."""
        if self._current_progress is None:
            return
        
        self._current_progress.mark_cell_completed(cell_id)
        self.save_progress()
    
    def increment_results_count(self, increment: int = 1) -> int:
        """Increment results count and return new count."""
        if self._current_progress is None:
            return 0
        
        self._current_progress.results_count += increment
        self.save_progress()
        return self._current_progress.results_count
    
    def is_job_complete(self) -> bool:
        """Check if the current job is complete."""
        if self._current_progress is None:
            return False
        
        return self._current_progress.results_count >= self._current_progress.total_target
    
    def add_cell_results(self, cell_id: str, count: int) -> None:
        """Add results count for a specific cell."""
        if self._current_progress is None:
            return
        
        self._current_progress.add_cell_results(cell_id, count)
        self.save_progress()
    
    def get_current_progress(self) -> Optional[JobProgress]:
        """Get the current progress."""
        return self._current_progress
    
    def create_named_progress_file(self, name: str) -> str:
        """Create a named progress file for specific searches.
        
        Args:
            name: Name identifier for the progress file
            
        Returns:
            Filename of the created progress file
        """
        # Clean name for filename
        clean_name = "".join(c for c in name if c.isalnum() or c in (' ', '-', '_')).rstrip()
        clean_name = clean_name.replace(' ', '_')
        
        named_filename = f"scraper_progress_{clean_name}.json"
        
        if self._current_progress:
            try:
                with open(named_filename, 'w') as f:
                    json.dump(asdict(self._current_progress), f, indent=2)
                
                self.logger.info(f"Created named progress file: {named_filename}")
                
            except Exception as e:
                self.logger.error(f"Failed to create named progress file: {e}")
        
        return named_filename