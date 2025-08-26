"""Logging configuration for Google Maps scraper."""

import logging
import datetime
from pathlib import Path
from typing import Optional


def setup_logging(log_level: str = 'INFO', 
                  log_file: Optional[str] = None,
                  log_format: Optional[str] = None) -> logging.Logger:
    """Set up logging configuration for the scraper.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional log file path. If None, auto-generates with timestamp
        log_format: Log message format string
        
    Returns:
        Configured root logger
    """
    # Default format
    if log_format is None:
        log_format = '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    
    # Auto-generate log file if not provided
    if log_file is None:
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = f"scraper_log_{timestamp}.log"
    
    # Convert string level to logging constant
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    
    # Clear any existing handlers
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Configure logging with both file and console handlers
    logging.basicConfig(
        level=numeric_level,
        format=log_format,
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger('GoogleMapsScraper')
    logger.info(f"Logging initialized - Level: {log_level}, File: {log_file}")
    
    return logger


def get_component_logger(component_name: str) -> logging.Logger:
    """Get a logger for a specific component.
    
    Args:
        component_name: Name of the component (e.g., 'scraper.business')
        
    Returns:
        Logger instance for the component
    """
    return logging.getLogger(f'GoogleMapsScraper.{component_name}')


def log_execution_time(logger: logging.Logger, operation_name: str):
    """Decorator to log execution time of operations.
    
    Args:
        logger: Logger instance to use
        operation_name: Name of the operation being timed
        
    Returns:
        Decorator function
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            import time
            start_time = time.time()
            
            try:
                result = func(*args, **kwargs)
                execution_time = time.time() - start_time
                logger.info(f"{operation_name} completed in {execution_time:.2f} seconds")
                return result
                
            except Exception as e:
                execution_time = time.time() - start_time
                logger.error(f"{operation_name} failed after {execution_time:.2f} seconds: {e}")
                raise
                
        return wrapper
    return decorator


def log_scraping_progress(logger: logging.Logger, current: int, total: int, 
                         item_type: str = "items"):
    """Log scraping progress in a consistent format.
    
    Args:
        logger: Logger instance
        current: Current count
        total: Total count
        item_type: Type of items being processed
    """
    if total > 0:
        percentage = (current / total) * 100
        logger.info(f"Progress: {current}/{total} {item_type} ({percentage:.1f}%)")
    else:
        logger.info(f"Progress: {current} {item_type}")


class ScraperLoggerAdapter(logging.LoggerAdapter):
    """Logger adapter that adds scraper context to log messages."""
    
    def __init__(self, logger: logging.Logger, search_term: str, 
                 grid_cell: Optional[str] = None):
        """Initialize adapter with context.
        
        Args:
            logger: Base logger
            search_term: Current search term
            grid_cell: Current grid cell identifier
        """
        extra = {
            'search_term': search_term,
            'grid_cell': grid_cell or 'N/A'
        }
        super().__init__(logger, extra)
    
    def process(self, msg, kwargs):
        """Add context to log message."""
        return f"[{self.extra['search_term']}] [{self.extra['grid_cell']}] {msg}", kwargs