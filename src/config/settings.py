"""Configuration settings for Google Maps scraper."""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Tuple

import yaml


EXTRACTION_GROUPS = (
    "contact_fields",
    "business_details",
    "review_summary",
    "review_rows",
    "review_analytics",
    "deleted_review_signals",
    "website_modernity",
)


@dataclass
class BrowserSettings:
    """Browser configuration settings."""
    executable_path: str = ""
    headless: bool = False
    session_state_file: str = ""
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
    review_mode: str = 'all_available'  # 'all_available' or 'rolling_365d'
    review_window_days: int = 365
    review_sort_order: str = 'newest'


@dataclass
class ExtractionSettings:
    """Per-run extraction groups that control optional scraper work."""

    contact_fields: bool = True
    business_details: bool = True
    review_summary: bool = True
    review_rows: bool = True
    review_analytics: bool = True
    deleted_review_signals: bool = True
    website_modernity: bool = False

    def enabled_groups(self) -> tuple[str, ...]:
        return tuple(
            group for group in EXTRACTION_GROUPS if bool(getattr(self, group, False))
        )


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
class OwnerEnrichmentSettings:
    """Configuration for adaptive owner enrichment workflow."""

    enabled: bool = False
    max_depth: int = 2
    max_pages: int = 4
    request_timeout_ms: int = 20000
    query_terms: Tuple[str, ...] = (
        "impressum",
        "imprint",
        "owner",
        "contact",
        "about us",
        "ueber uns",
        "geschaeftsfuehrer",
    )
    confidence_threshold: float = 0.75
    saturation_threshold: float = 0.6
    crawler_engine: str = "adaptive"
    crawler_browser_channel: Optional[str] = None
    openrouter_api_key_env: str = "OPENROUTER_API_KEY"
    openrouter_default_model: str = "google/gemini-2.0-flash-exp:free"
    # Compatibility flag; explicit model overrides are still honored.
    allow_free_models_only: bool = True
    max_llm_retries: int = 2
    llm_response_format: str = "json_object"
    log_prompts: bool = False


@dataclass
class ScraperSettings:
    """Complete scraper configuration."""
    browser: BrowserSettings = field(default_factory=BrowserSettings)
    scraping: ScrapingSettings = field(default_factory=ScrapingSettings)
    extraction: ExtractionSettings = field(default_factory=ExtractionSettings)
    grid: GridSettings = field(default_factory=GridSettings)
    files: FileSettings = field(default_factory=FileSettings)
    owner_enrichment: OwnerEnrichmentSettings = field(default_factory=OwnerEnrichmentSettings)


def parse_extraction_group_csv(raw_value: Optional[str]) -> tuple[str, ...]:
    """Parse a comma-separated extraction group string."""

    if raw_value is None:
        return ()

    return tuple(item.strip() for item in str(raw_value).split(",") if item.strip())


def validate_extraction_groups(groups: Iterable[str]) -> tuple[str, ...]:
    """Validate extraction group names and return them in a stable order."""

    requested = {str(group).strip() for group in groups if str(group).strip()}
    invalid = sorted(requested.difference(EXTRACTION_GROUPS))
    if invalid:
        raise ValueError(
            "Unknown extraction group(s): "
            + ", ".join(invalid)
            + f". Supported groups: {', '.join(EXTRACTION_GROUPS)}"
        )

    return tuple(group for group in EXTRACTION_GROUPS if group in requested)


def apply_extraction_overrides(
    extraction: ExtractionSettings,
    *,
    enabled_groups: Optional[Iterable[str]] = None,
    disabled_groups: Optional[Iterable[str]] = None,
    field_overrides: Optional[Mapping[str, Any]] = None,
) -> None:
    """Apply extraction selection overrides to an ExtractionSettings object."""

    if enabled_groups is not None:
        normalized_enabled = validate_extraction_groups(enabled_groups)
        for group in EXTRACTION_GROUPS:
            setattr(extraction, group, group in normalized_enabled)

    if disabled_groups:
        normalized_disabled = validate_extraction_groups(disabled_groups)
        for group in normalized_disabled:
            setattr(extraction, group, False)

    if not field_overrides:
        return

    override_groups = {
        key: value for key, value in field_overrides.items()
        if key not in {"enabled_groups", "disabled_groups"}
    }
    if override_groups:
        validate_extraction_groups(override_groups.keys())
        for group, value in override_groups.items():
            if not isinstance(value, bool):
                raise ValueError(
                    f"Extraction override '{group}' must be a boolean, got {type(value).__name__}"
                )
            setattr(extraction, group, value)

    override_enabled = field_overrides.get("enabled_groups")
    if override_enabled is not None:
        if not isinstance(override_enabled, (list, tuple)):
            raise ValueError("extraction.enabled_groups must be a list of group names")
        apply_extraction_overrides(extraction, enabled_groups=override_enabled)

    override_disabled = field_overrides.get("disabled_groups")
    if override_disabled is not None:
        if not isinstance(override_disabled, (list, tuple)):
            raise ValueError("extraction.disabled_groups must be a list of group names")
        apply_extraction_overrides(extraction, disabled_groups=override_disabled)


def apply_settings_overrides(settings: ScraperSettings, overrides: Mapping[str, Any]) -> None:
    """Apply nested runtime overrides onto ScraperSettings."""

    for key, value in overrides.items():
        if key == "owner_enrichment" and isinstance(value, Mapping):
            owner_settings = settings.owner_enrichment
            for sub_key, sub_value in value.items():
                if hasattr(owner_settings, sub_key):
                    setattr(owner_settings, sub_key, sub_value)
            continue

        if key == "extraction" and isinstance(value, Mapping):
            apply_extraction_overrides(settings.extraction, field_overrides=value)
            continue

        if hasattr(settings, key):
            setattr(settings, key, value)


def apply_argument_overrides(settings: ScraperSettings, args: Any) -> None:
    """Apply CLI-style argument overrides onto ScraperSettings."""

    if getattr(args, "headless", None) is not None:
        settings.browser.headless = args.headless
    if getattr(args, "browser_state_file", None):
        settings.browser.session_state_file = args.browser_state_file

    if getattr(args, "max_reviews", None):
        settings.scraping.max_reviews_per_business = args.max_reviews
    if getattr(args, "review_mode", None):
        settings.scraping.review_mode = args.review_mode
    if getattr(args, "review_window_days", None):
        settings.scraping.review_window_days = args.review_window_days

    enabled_groups = (
        parse_extraction_group_csv(getattr(args, "extract", None))
        if getattr(args, "extract", None) is not None
        else None
    )
    disabled_groups = parse_extraction_group_csv(getattr(args, "skip_extract", None))
    apply_extraction_overrides(
        settings.extraction,
        enabled_groups=enabled_groups,
        disabled_groups=disabled_groups,
    )

    if getattr(args, "owner_enrichment", False):
        settings.owner_enrichment.enabled = True

    if getattr(args, "owner_model", None):
        settings.owner_enrichment.openrouter_default_model = args.owner_model

    if getattr(args, "owner_max_pages", None):
        settings.owner_enrichment.max_pages = args.owner_max_pages


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
            config_data = yaml.safe_load(f) or {}
        
        settings = ScraperSettings()
        
        # Load browser settings
        if 'browser' in config_data:
            browser_data = config_data['browser']
            settings.browser = BrowserSettings(
                executable_path=browser_data.get('executable_path', settings.browser.executable_path),
                headless=browser_data.get('headless', settings.browser.headless),
                session_state_file=browser_data.get('session_state_file', settings.browser.session_state_file),
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
                default_mode=scraping_data.get('default_mode', settings.scraping.default_mode),
                review_mode=scraping_data.get('review_mode', settings.scraping.review_mode),
                review_window_days=scraping_data.get('review_window_days', settings.scraping.review_window_days),
                review_sort_order=scraping_data.get('review_sort_order', settings.scraping.review_sort_order),
            )

        if 'extraction' in config_data:
            extraction_data = config_data['extraction'] or {}
            settings.extraction = ExtractionSettings(
                contact_fields=extraction_data.get('contact_fields', settings.extraction.contact_fields),
                business_details=extraction_data.get('business_details', settings.extraction.business_details),
                review_summary=extraction_data.get('review_summary', settings.extraction.review_summary),
                review_rows=extraction_data.get('review_rows', settings.extraction.review_rows),
                review_analytics=extraction_data.get('review_analytics', settings.extraction.review_analytics),
                deleted_review_signals=extraction_data.get('deleted_review_signals', settings.extraction.deleted_review_signals),
                website_modernity=extraction_data.get('website_modernity', settings.extraction.website_modernity),
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

        if 'owner_enrichment' in config_data:
            owner_data = config_data['owner_enrichment'] or {}
            settings.owner_enrichment = OwnerEnrichmentSettings(
                enabled=owner_data.get('enabled', settings.owner_enrichment.enabled),
                max_depth=owner_data.get('max_depth', settings.owner_enrichment.max_depth),
                max_pages=owner_data.get('max_pages', settings.owner_enrichment.max_pages),
                request_timeout_ms=owner_data.get('request_timeout_ms', settings.owner_enrichment.request_timeout_ms),
                query_terms=tuple(owner_data.get('query_terms', settings.owner_enrichment.query_terms)),
                confidence_threshold=owner_data.get('confidence_threshold', settings.owner_enrichment.confidence_threshold),
                saturation_threshold=owner_data.get('saturation_threshold', settings.owner_enrichment.saturation_threshold),
                crawler_engine=owner_data.get('crawler_engine', settings.owner_enrichment.crawler_engine),
                crawler_browser_channel=owner_data.get('crawler_browser_channel', settings.owner_enrichment.crawler_browser_channel),
                openrouter_api_key_env=owner_data.get('openrouter_api_key_env', settings.owner_enrichment.openrouter_api_key_env),
                openrouter_default_model=owner_data.get('openrouter_default_model', settings.owner_enrichment.openrouter_default_model),
                allow_free_models_only=owner_data.get('allow_free_models_only', settings.owner_enrichment.allow_free_models_only),
                max_llm_retries=owner_data.get('max_llm_retries', settings.owner_enrichment.max_llm_retries),
                llm_response_format=owner_data.get('llm_response_format', settings.owner_enrichment.llm_response_format),
                log_prompts=owner_data.get('log_prompts', settings.owner_enrichment.log_prompts),
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
        if os.getenv('BROWSER_STATE_FILE'):
            settings.browser.session_state_file = os.getenv('BROWSER_STATE_FILE')
        
        # Scraping settings from env
        if os.getenv('MAX_LISTINGS_PER_CELL'):
            settings.scraping.max_listings_per_cell = int(os.getenv('MAX_LISTINGS_PER_CELL'))
        if os.getenv('MAX_REVIEWS_PER_BUSINESS'):
            settings.scraping.max_reviews_per_business = int(os.getenv('MAX_REVIEWS_PER_BUSINESS'))
        if os.getenv('REVIEW_MODE'):
            settings.scraping.review_mode = os.getenv('REVIEW_MODE')
        if os.getenv('REVIEW_WINDOW_DAYS'):
            settings.scraping.review_window_days = int(os.getenv('REVIEW_WINDOW_DAYS'))

        extraction_env = os.getenv('EXTRACT_GROUPS')
        if extraction_env:
            apply_extraction_overrides(
                settings.extraction,
                enabled_groups=parse_extraction_group_csv(extraction_env),
            )
        skip_extraction_env = os.getenv('SKIP_EXTRACT_GROUPS')
        if skip_extraction_env:
            apply_extraction_overrides(
                settings.extraction,
                disabled_groups=parse_extraction_group_csv(skip_extraction_env),
            )
        
        # Owner enrichment overrides
        if os.getenv('OWNER_ENRICHMENT_ENABLED'):
            settings.owner_enrichment.enabled = os.getenv('OWNER_ENRICHMENT_ENABLED').lower() == 'true'
        if os.getenv('OWNER_ENRICHMENT_MAX_PAGES'):
            settings.owner_enrichment.max_pages = int(os.getenv('OWNER_ENRICHMENT_MAX_PAGES'))
        if os.getenv('OWNER_ENRICHMENT_MAX_DEPTH'):
            settings.owner_enrichment.max_depth = int(os.getenv('OWNER_ENRICHMENT_MAX_DEPTH'))
        if os.getenv('OWNER_ENRICHMENT_CONFIDENCE_THRESHOLD'):
            settings.owner_enrichment.confidence_threshold = float(os.getenv('OWNER_ENRICHMENT_CONFIDENCE_THRESHOLD'))
        if os.getenv('OPENROUTER_DEFAULT_MODEL'):
            settings.owner_enrichment.openrouter_default_model = os.getenv('OPENROUTER_DEFAULT_MODEL')
        if os.getenv('OWNER_ENRICHMENT_ALLOW_FREE_ONLY'):
            settings.owner_enrichment.allow_free_models_only = os.getenv('OWNER_ENRICHMENT_ALLOW_FREE_ONLY').lower() == 'true'

        return cls(settings)
    
    def save_to_file(self, config_path: str) -> None:
        """Save current configuration to YAML file."""
        config_data = {
            'browser': {
                'executable_path': self.settings.browser.executable_path,
                'headless': self.settings.browser.headless,
                'session_state_file': self.settings.browser.session_state_file,
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
                'default_max_reviews': self.settings.scraping.default_max_reviews,
                'default_mode': self.settings.scraping.default_mode,
                'review_mode': self.settings.scraping.review_mode,
                'review_window_days': self.settings.scraping.review_window_days,
                'review_sort_order': self.settings.scraping.review_sort_order,
            },
            'extraction': {
                'contact_fields': self.settings.extraction.contact_fields,
                'business_details': self.settings.extraction.business_details,
                'review_summary': self.settings.extraction.review_summary,
                'review_rows': self.settings.extraction.review_rows,
                'review_analytics': self.settings.extraction.review_analytics,
                'deleted_review_signals': self.settings.extraction.deleted_review_signals,
                'website_modernity': self.settings.extraction.website_modernity,
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
            },
            'owner_enrichment': {
                'enabled': self.settings.owner_enrichment.enabled,
                'max_depth': self.settings.owner_enrichment.max_depth,
                'max_pages': self.settings.owner_enrichment.max_pages,
                'request_timeout_ms': self.settings.owner_enrichment.request_timeout_ms,
                'query_terms': list(self.settings.owner_enrichment.query_terms),
                'confidence_threshold': self.settings.owner_enrichment.confidence_threshold,
                'saturation_threshold': self.settings.owner_enrichment.saturation_threshold,
                'crawler_engine': self.settings.owner_enrichment.crawler_engine,
                'crawler_browser_channel': self.settings.owner_enrichment.crawler_browser_channel,
                'openrouter_api_key_env': self.settings.owner_enrichment.openrouter_api_key_env,
                'openrouter_default_model': self.settings.owner_enrichment.openrouter_default_model,
                'allow_free_models_only': self.settings.owner_enrichment.allow_free_models_only,
                'max_llm_retries': self.settings.owner_enrichment.max_llm_retries,
                'llm_response_format': self.settings.owner_enrichment.llm_response_format,
                'log_prompts': self.settings.owner_enrichment.log_prompts,
            }
        }

        with open(config_path, 'w') as f:
            yaml.dump(config_data, f, default_flow_style=False, indent=2)
