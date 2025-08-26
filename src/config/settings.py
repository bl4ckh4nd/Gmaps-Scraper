"""Configuration settings for Google Maps scraper."""

import os
import yaml
from dataclasses import dataclass, field
from typing import Tuple, Optional
from pathlib import Path


@dataclass
class BrowserSettings:
    """Browser configuration settings."""
    executable_path: str = r'C:\Program Files\Google\Chrome\Application\chrome.exe'
    headless: bool = False
    timeout_navigation: int = 60000
    timeout_element: int = 10000
    timeout_short: int = 5000
    timeout_very_short: int = 3000
    handle_cookie_banner: bool = True
    cookie_preference: str = "reject"  # "reject" or "accept"
    enable_debug_mode: bool = True


@dataclass
class ScrapingSettings:
    """Scraping behavior configuration."""
    scroll_interval: int = 1500
    max_scroll_attempts: int = 5
    max_listings_per_cell: int = 120
    max_reviews_per_business: int = 100
    review_batch_size: int = 10
    default_max_reviews: int = 50
    default_mode: str = 'fast'  # Default scraping mode: 'fast' or 'coverage'


@dataclass  
class GridSettings:
    """Geographic grid configuration."""
    default_grid_size: int = 2
    default_zoom_level: int = 12
    default_bounds: Tuple[float, float, float, float] = (43.6, -79.5, 43.9, -79.2)  # Toronto area


@dataclass
class FileSettings:
    """File and output configuration."""
    result_filename: str = 'result.csv'
    reviews_filename: str = 'reviews.csv'
    progress_filename: str = 'scraper_progress.json'
    log_format: str = '%(asctime)s [%(levelname)s] %(message)s'


@dataclass
class ScraperSettings:
    """Complete scraper configuration."""
    browser: BrowserSettings = field(default_factory=BrowserSettings)
    scraping: ScrapingSettings = field(default_factory=ScrapingSettings)
    grid: GridSettings = field(default_factory=GridSettings)
    files: FileSettings = field(default_factory=FileSettings)


class Config:
    """Configuration manager for the scraper."""
    
    def __init__(self, settings: Optional[ScraperSettings] = None):
        self.settings = settings or ScraperSettings()
    
    @classmethod
    def from_file(cls, config_path: str) -> 'Config':
        """Load configuration from YAML file."""
        config_file = Path(config_path)
        
        if not config_file.exists():
            # Create default config file if it doesn't exist
            default_config = cls()
            default_config.save_to_file(config_path)
            return default_config
        
        with open(config_file, 'r') as f:
            config_data = yaml.safe_load(f)
        
        settings = ScraperSettings()
        
        # Load browser settings
        if 'browser' in config_data:
            browser_data = config_data['browser']
            settings.browser = BrowserSettings(
                executable_path=browser_data.get('executable_path', settings.browser.executable_path),
                headless=browser_data.get('headless', settings.browser.headless),
                timeout_navigation=browser_data.get('timeout_navigation', settings.browser.timeout_navigation),
                timeout_element=browser_data.get('timeout_element', settings.browser.timeout_element),
                timeout_short=browser_data.get('timeout_short', settings.browser.timeout_short),
                timeout_very_short=browser_data.get('timeout_very_short', settings.browser.timeout_very_short),
                handle_cookie_banner=browser_data.get('handle_cookie_banner', settings.browser.handle_cookie_banner),
                cookie_preference=browser_data.get('cookie_preference', settings.browser.cookie_preference),
                enable_debug_mode=browser_data.get('enable_debug_mode', settings.browser.enable_debug_mode)
            )
        
        # Load scraping settings
        if 'scraping' in config_data:
            scraping_data = config_data['scraping']
            settings.scraping = ScrapingSettings(
                scroll_interval=scraping_data.get('scroll_interval', settings.scraping.scroll_interval),
                max_scroll_attempts=scraping_data.get('max_scroll_attempts', settings.scraping.max_scroll_attempts),
                max_listings_per_cell=scraping_data.get('max_listings_per_cell', settings.scraping.max_listings_per_cell),
                max_reviews_per_business=scraping_data.get('max_reviews_per_business', settings.scraping.max_reviews_per_business),
                review_batch_size=scraping_data.get('review_batch_size', settings.scraping.review_batch_size),
                default_max_reviews=scraping_data.get('default_max_reviews', settings.scraping.default_max_reviews),
                default_mode=scraping_data.get('default_mode', settings.scraping.default_mode)
            )
        
        # Load grid settings
        if 'grid' in config_data:
            grid_data = config_data['grid']
            settings.grid = GridSettings(
                default_grid_size=grid_data.get('default_grid_size', settings.grid.default_grid_size),
                default_zoom_level=grid_data.get('default_zoom_level', settings.grid.default_zoom_level),
                default_bounds=tuple(grid_data.get('default_bounds', settings.grid.default_bounds))
            )
        
        # Load file settings
        if 'files' in config_data:
            files_data = config_data['files']
            settings.files = FileSettings(
                result_filename=files_data.get('result_filename', settings.files.result_filename),
                reviews_filename=files_data.get('reviews_filename', settings.files.reviews_filename),
                progress_filename=files_data.get('progress_filename', settings.files.progress_filename),
                log_format=files_data.get('log_format', settings.files.log_format)
            )
        
        return cls(settings)
    
    @classmethod
    def from_env(cls) -> 'Config':
        """Load configuration from environment variables."""
        settings = ScraperSettings()
        
        # Browser settings from env
        if os.getenv('CHROME_PATH'):
            settings.browser.executable_path = os.getenv('CHROME_PATH')
        if os.getenv('HEADLESS'):
            settings.browser.headless = os.getenv('HEADLESS').lower() == 'true'
        
        # Scraping settings from env
        if os.getenv('MAX_LISTINGS_PER_CELL'):
            settings.scraping.max_listings_per_cell = int(os.getenv('MAX_LISTINGS_PER_CELL'))
        if os.getenv('MAX_REVIEWS_PER_BUSINESS'):
            settings.scraping.max_reviews_per_business = int(os.getenv('MAX_REVIEWS_PER_BUSINESS'))
        
        return cls(settings)
    
    def save_to_file(self, config_path: str) -> None:
        """Save current configuration to YAML file."""
        config_data = {
            'browser': {
                'executable_path': self.settings.browser.executable_path,
                'headless': self.settings.browser.headless,
                'timeout_navigation': self.settings.browser.timeout_navigation,
                'timeout_element': self.settings.browser.timeout_element,
                'timeout_short': self.settings.browser.timeout_short,
                'timeout_very_short': self.settings.browser.timeout_very_short,
                'handle_cookie_banner': self.settings.browser.handle_cookie_banner,
                'cookie_preference': self.settings.browser.cookie_preference,
                'enable_debug_mode': self.settings.browser.enable_debug_mode
            },
            'scraping': {
                'scroll_interval': self.settings.scraping.scroll_interval,
                'max_scroll_attempts': self.settings.scraping.max_scroll_attempts,
                'max_listings_per_cell': self.settings.scraping.max_listings_per_cell,
                'max_reviews_per_business': self.settings.scraping.max_reviews_per_business,
                'review_batch_size': self.settings.scraping.review_batch_size,
                'default_max_reviews': self.settings.scraping.default_max_reviews
            },
            'grid': {
                'default_grid_size': self.settings.grid.default_grid_size,
                'default_zoom_level': self.settings.grid.default_zoom_level,
                'default_bounds': list(self.settings.grid.default_bounds)
            },
            'files': {
                'result_filename': self.settings.files.result_filename,
                'reviews_filename': self.settings.files.reviews_filename,
                'progress_filename': self.settings.files.progress_filename,
                'log_format': self.settings.files.log_format
            }
        }
        
        with open(config_path, 'w') as f:
            yaml.dump(config_data, f, default_flow_style=False, indent=2)