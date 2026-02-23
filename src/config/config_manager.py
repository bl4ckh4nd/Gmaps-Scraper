"""Configuration manager implementing hybrid configuration strategy.

Precedence (highest to lowest):
1. Environment Variables (runtime overrides)
2. Database Settings (user-configured via UI)
3. config.yaml (advanced settings)
4. Dataclass Defaults (fallback)
"""

import os
import logging
from pathlib import Path
from typing import Any, Optional

from .settings import Config, ScraperSettings, BrowserSettings, OwnerEnrichmentSettings
from .db_repository import SettingsRepository, DatabaseError
from ..utils.encryption import SecureStorage, EncryptionError


logger = logging.getLogger(__name__)


class ConfigurationError(Exception):
    """Raised when configuration operations fail."""
    pass


class ConfigurationManager:
    """Unified configuration manager implementing hybrid strategy."""

    def __init__(
        self,
        yaml_path: Path,
        db_path: Path,
        encryption_key_path: Optional[Path] = None
    ):
        """Initialize configuration manager.

        Args:
            yaml_path: Path to config.yaml file
            db_path: Path to SQLite database
            encryption_key_path: Optional path to encryption key file
        """
        self.yaml_path = yaml_path
        self.db_path = db_path

        # Initialize database repository
        try:
            self.db_repo = SettingsRepository(db_path)
        except DatabaseError as e:
            logger.error(f"Failed to initialize database: {e}")
            raise ConfigurationError(f"Database initialization failed: {e}")

        # Initialize encryption
        try:
            self.secure_storage = SecureStorage(encryption_key_path)
        except EncryptionError as e:
            logger.error(f"Failed to initialize encryption: {e}")
            raise ConfigurationError(f"Encryption initialization failed: {e}")

        self._cached_config: Optional[Config] = None

    def get_effective_config(self) -> Config:
        """Get effective configuration merging all sources.

        Returns:
            Config instance with merged settings

        Raises:
            ConfigurationError: If configuration cannot be loaded
        """
        # Start with YAML configuration (or defaults)
        base_config = self._load_yaml_config()

        # Apply database overrides
        try:
            db_config = self._apply_database_overrides(base_config)
        except DatabaseError as e:
            logger.warning(f"Database unavailable, using YAML config: {e}")
            db_config = base_config

        # Apply environment variable overrides
        final_config = self._apply_env_overrides(db_config)

        # Cache for future use
        self._cached_config = final_config

        return final_config

    def invalidate_cache(self) -> None:
        """Invalidate cached configuration.

        Call this after updating settings to force reload.
        """
        self._cached_config = None
        logger.debug("Configuration cache invalidated")

    def save_chrome_path(self, chrome_path: str, validated: bool = False) -> None:
        """Save Chrome path to database.

        Args:
            chrome_path: Path to Chrome executable
            validated: Whether path has been validated

        Raises:
            ConfigurationError: If save fails
        """
        try:
            from datetime import datetime

            settings = {
                'chrome_path': chrome_path,
                'chrome_validated': validated
            }

            if validated:
                settings['chrome_last_validated'] = datetime.now()

            self.db_repo.update_system_settings(settings)
            self.invalidate_cache()
            logger.info(f"Saved Chrome path to database: {chrome_path}")

        except DatabaseError as e:
            logger.error(f"Failed to save Chrome path: {e}")
            raise ConfigurationError(f"Could not save Chrome path: {e}")

    def save_api_key(
        self,
        api_key: str,
        model: Optional[str] = None,
        validated: bool = False
    ) -> None:
        """Save OpenRouter API key to database (encrypted).

        Args:
            api_key: OpenRouter API key
            model: Optional model ID
            validated: Whether key has been validated

        Raises:
            ConfigurationError: If save fails
        """
        try:
            from datetime import datetime

            # Encrypt API key
            encrypted_key = self.secure_storage.encrypt(api_key)

            settings = {
                'openrouter_api_key': encrypted_key,
                'openrouter_validated': validated
            }

            if model:
                settings['openrouter_model'] = model

            if validated:
                settings['openrouter_last_validated'] = datetime.now()

            self.db_repo.update_system_settings(settings)
            self.invalidate_cache()
            logger.info("Saved API key to database (encrypted)")

        except (DatabaseError, EncryptionError) as e:
            logger.error(f"Failed to save API key: {e}")
            raise ConfigurationError(f"Could not save API key: {e}")

    def get_decrypted_api_key(self) -> Optional[str]:
        """Get decrypted API key from database.

        Returns:
            Decrypted API key or None if not set

        Raises:
            ConfigurationError: If decryption fails
        """
        try:
            system_settings = self.db_repo.get_system_settings()

            if not system_settings.openrouter_api_key:
                return None

            decrypted_key = self.secure_storage.decrypt(system_settings.openrouter_api_key)
            return decrypted_key

        except (DatabaseError, EncryptionError) as e:
            logger.error(f"Failed to decrypt API key: {e}")
            raise ConfigurationError(f"Could not decrypt API key: {e}")

    def save_owner_enrichment_enabled(self, enabled: bool) -> None:
        """Save owner enrichment enabled flag.

        Args:
            enabled: Whether owner enrichment is enabled

        Raises:
            ConfigurationError: If save fails
        """
        try:
            self.db_repo.update_system_settings({
                'owner_enrichment_enabled': enabled
            })
            self.invalidate_cache()
            logger.info(f"Set owner enrichment enabled: {enabled}")

        except DatabaseError as e:
            logger.error(f"Failed to save owner enrichment setting: {e}")
            raise ConfigurationError(f"Could not save setting: {e}")

    def mark_onboarding_completed(self) -> None:
        """Mark onboarding as completed.

        Raises:
            ConfigurationError: If update fails
        """
        try:
            self.db_repo.update_system_settings({
                'onboarding_completed': True
            })
            logger.info("Onboarding marked as completed")

        except DatabaseError as e:
            logger.error(f"Failed to mark onboarding completed: {e}")
            raise ConfigurationError(f"Could not mark onboarding completed: {e}")

    def is_onboarding_completed(self) -> bool:
        """Check if onboarding has been completed.

        Returns:
            True if onboarding completed, False otherwise
        """
        try:
            system_settings = self.db_repo.get_system_settings()
            return system_settings.onboarding_completed
        except DatabaseError as e:
            logger.warning(f"Could not check onboarding status: {e}")
            return False  # Assume not completed on error

    def _load_yaml_config(self) -> Config:
        """Load configuration from YAML file.

        Returns:
            Config instance

        Raises:
            ConfigurationError: If YAML cannot be loaded
        """
        try:
            if self.yaml_path.exists():
                config = Config.from_file(str(self.yaml_path))
                logger.debug(f"Loaded config from {self.yaml_path}")
            else:
                # Create default config
                config = Config()
                config.save_to_file(str(self.yaml_path))
                logger.info(f"Created default config at {self.yaml_path}")

            return config

        except Exception as e:
            logger.error(f"Failed to load YAML config: {e}")
            # Return defaults rather than failing completely
            logger.warning("Using default configuration")
            return Config()

    def _apply_database_overrides(self, base_config: Config) -> Config:
        """Apply database settings overrides to base configuration.

        Args:
            base_config: Base configuration from YAML

        Returns:
            Config with database overrides applied

        Raises:
            DatabaseError: If database access fails
        """
        system_settings = self.db_repo.get_system_settings()
        user_prefs = self.db_repo.get_user_preferences()

        # Create a copy of settings
        settings = base_config.settings

        # Override browser settings
        if system_settings.chrome_path:
            settings.browser.executable_path = system_settings.chrome_path

        if user_prefs.default_headless is not None:
            settings.browser.headless = user_prefs.default_headless

        # Override owner enrichment settings
        if system_settings.owner_enrichment_enabled is not None:
            settings.owner_enrichment.enabled = system_settings.owner_enrichment_enabled

        if system_settings.openrouter_api_key:
            # Decrypt API key and set in environment for runtime use
            try:
                decrypted_key = self.secure_storage.decrypt(system_settings.openrouter_api_key)
                os.environ[settings.owner_enrichment.openrouter_api_key_env] = decrypted_key
            except EncryptionError as e:
                logger.warning(f"Could not decrypt API key: {e}")

        if system_settings.openrouter_model:
            settings.owner_enrichment.openrouter_default_model = system_settings.openrouter_model

        # Override scraping preferences
        if user_prefs.default_scraping_mode:
            settings.scraping.default_mode = user_prefs.default_scraping_mode

        # Override grid settings
        if user_prefs.default_grid_size:
            settings.grid.default_grid_size = user_prefs.default_grid_size

        return Config(settings)

    def _apply_env_overrides(self, base_config: Config) -> Config:
        """Apply environment variable overrides to configuration.

        Args:
            base_config: Base configuration with DB overrides

        Returns:
            Config with environment overrides applied
        """
        settings = base_config.settings

        # Browser settings
        if os.getenv('CHROME_PATH'):
            settings.browser.executable_path = os.getenv('CHROME_PATH')

        if os.getenv('HEADLESS'):
            settings.browser.headless = os.getenv('HEADLESS').lower() == 'true'

        # Scraping settings
        if os.getenv('MAX_LISTINGS_PER_CELL'):
            try:
                settings.scraping.max_listings_per_cell = int(os.getenv('MAX_LISTINGS_PER_CELL'))
            except ValueError:
                logger.warning("Invalid MAX_LISTINGS_PER_CELL environment variable")

        if os.getenv('MAX_REVIEWS_PER_BUSINESS'):
            try:
                settings.scraping.max_reviews_per_business = int(os.getenv('MAX_REVIEWS_PER_BUSINESS'))
            except ValueError:
                logger.warning("Invalid MAX_REVIEWS_PER_BUSINESS environment variable")

        # Owner enrichment settings
        if os.getenv('OWNER_ENRICHMENT_ENABLED'):
            settings.owner_enrichment.enabled = os.getenv('OWNER_ENRICHMENT_ENABLED').lower() == 'true'

        if os.getenv('OPENROUTER_DEFAULT_MODEL'):
            settings.owner_enrichment.openrouter_default_model = os.getenv('OPENROUTER_DEFAULT_MODEL')

        # Note: OPENROUTER_API_KEY is handled directly by the owner enrichment service

        logger.debug("Applied environment variable overrides")
        return Config(settings)

    def get_system_settings_dict(self) -> dict:
        """Get current system settings as dictionary.

        Returns:
            Dictionary of system settings

        Raises:
            ConfigurationError: If settings cannot be retrieved
        """
        try:
            system_settings = self.db_repo.get_system_settings()
            return system_settings.to_dict()
        except DatabaseError as e:
            logger.error(f"Failed to get system settings: {e}")
            raise ConfigurationError(f"Could not get system settings: {e}")

    def get_user_preferences_dict(self) -> dict:
        """Get current user preferences as dictionary.

        Returns:
            Dictionary of user preferences

        Raises:
            ConfigurationError: If preferences cannot be retrieved
        """
        try:
            user_prefs = self.db_repo.get_user_preferences()
            return user_prefs.to_dict()
        except DatabaseError as e:
            logger.error(f"Failed to get user preferences: {e}")
            raise ConfigurationError(f"Could not get user preferences: {e}")
