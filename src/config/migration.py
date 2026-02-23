"""Migration script for existing installations to database-based configuration."""

import os
import logging
import shutil
from pathlib import Path
from datetime import datetime
from typing import Optional

from .settings import Config
from .config_manager import ConfigurationManager
from .db_repository import DatabaseError
from ..services.system_validation import SystemValidationService


logger = logging.getLogger(__name__)


class MigrationError(Exception):
    """Raised when migration fails."""
    pass


class ConfigMigration:
    """Handles migration from YAML-only to database-backed configuration."""

    def __init__(self, yaml_path: Path, db_path: Path):
        """Initialize migration handler.

        Args:
            yaml_path: Path to config.yaml
            db_path: Path to database file
        """
        self.yaml_path = yaml_path
        self.db_path = db_path
        self.validator = SystemValidationService()

    def needs_migration(self) -> bool:
        """Check if migration is needed.

        Migration is needed if:
        - config.yaml exists
        - Database doesn't exist OR onboarding not completed

        Returns:
            True if migration needed, False otherwise
        """
        yaml_exists = self.yaml_path.exists()
        db_exists = self.db_path.exists()

        if not yaml_exists:
            # No config file, fresh installation - no migration needed
            return False

        if not db_exists:
            # YAML exists but no database - migration needed
            logger.info("Migration needed: config.yaml exists but no database")
            return True

        # Database exists, check if onboarding completed
        try:
            config_manager = ConfigurationManager(self.yaml_path, self.db_path)
            if not config_manager.is_onboarding_completed():
                logger.info("Migration needed: onboarding not completed")
                return True
        except Exception as e:
            logger.warning(f"Could not check onboarding status: {e}")
            # Assume migration needed if we can't check
            return True

        logger.debug("No migration needed")
        return False

    def migrate(self) -> bool:
        """Perform migration from YAML to database.

        Process:
        1. Initialize database schema
        2. Load config.yaml
        3. Import Chrome path
        4. Import owner enrichment settings
        5. Check for environment API key
        6. Validate imported settings
        7. Mark onboarding as completed
        8. Create backup of config.yaml

        Returns:
            True if migration successful, False otherwise
        """
        logger.info("Starting configuration migration...")

        try:
            # Create backup of config.yaml
            self._backup_config()

            # Initialize configuration manager
            # (this will create database schema automatically)
            config_manager = ConfigurationManager(self.yaml_path, self.db_path)

            # Load YAML configuration
            yaml_config = self._load_yaml_config()
            if not yaml_config:
                logger.warning("Could not load config.yaml, using defaults")
                yaml_config = Config()

            # Migrate Chrome path
            self._migrate_chrome_path(config_manager, yaml_config)

            # Migrate owner enrichment settings
            self._migrate_owner_enrichment(config_manager, yaml_config)

            # Check for API key in environment
            self._check_env_api_key(config_manager, yaml_config)

            # Mark onboarding as completed (skip wizard for existing users)
            config_manager.mark_onboarding_completed()

            logger.info("Migration completed successfully")
            logger.info("Existing installation detected - onboarding wizard will be skipped")
            return True

        except Exception as e:
            logger.error(f"Migration failed: {e}")
            logger.error("You may need to configure settings manually through the web interface")
            return False

    def _backup_config(self) -> None:
        """Create a backup of config.yaml.

        Raises:
            MigrationError: If backup fails
        """
        if not self.yaml_path.exists():
            return

        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_path = self.yaml_path.parent / f"config.yaml.backup_{timestamp}"

            shutil.copy2(self.yaml_path, backup_path)
            logger.info(f"Created config backup: {backup_path}")

        except Exception as e:
            logger.warning(f"Could not create config backup: {e}")
            # Don't fail migration if backup fails

    def _load_yaml_config(self) -> Optional[Config]:
        """Load configuration from YAML.

        Returns:
            Config instance or None if failed
        """
        try:
            if self.yaml_path.exists():
                config = Config.from_file(str(self.yaml_path))
                logger.debug("Loaded config.yaml for migration")
                return config
        except Exception as e:
            logger.warning(f"Could not load config.yaml: {e}")

        return None

    def _migrate_chrome_path(
        self,
        config_manager: ConfigurationManager,
        yaml_config: Config
    ) -> None:
        """Migrate Chrome path from YAML to database.

        Args:
            config_manager: Configuration manager instance
            yaml_config: Config loaded from YAML
        """
        chrome_path = yaml_config.settings.browser.executable_path

        if not chrome_path:
            logger.warning("No Chrome path in config.yaml")
            return

        logger.info(f"Migrating Chrome path: {chrome_path}")

        # Validate Chrome path
        validation = self.validator.validate_chrome(chrome_path)

        if validation.is_valid:
            logger.info(f"Chrome path validated: {validation.message}")
            config_manager.save_chrome_path(chrome_path, validated=True)
        else:
            logger.warning(f"Chrome path validation failed: {validation.error}")
            logger.warning("Saving path anyway - it may need to be updated")
            config_manager.save_chrome_path(chrome_path, validated=False)

    def _migrate_owner_enrichment(
        self,
        config_manager: ConfigurationManager,
        yaml_config: Config
    ) -> None:
        """Migrate owner enrichment settings from YAML to database.

        Args:
            config_manager: Configuration manager instance
            yaml_config: Config loaded from YAML
        """
        owner_settings = yaml_config.settings.owner_enrichment

        # Migrate enabled flag
        if owner_settings.enabled:
            logger.info("Migrating owner enrichment enabled flag")
            config_manager.save_owner_enrichment_enabled(True)

        # Migrate default model
        if owner_settings.openrouter_default_model:
            logger.info(f"Default model: {owner_settings.openrouter_default_model}")
            # Model will be saved with API key if present

    def _check_env_api_key(
        self,
        config_manager: ConfigurationManager,
        yaml_config: Config
    ) -> None:
        """Check for OpenRouter API key in environment and import if present.

        Args:
            config_manager: Configuration manager instance
            yaml_config: Config loaded from YAML
        """
        owner_settings = yaml_config.settings.owner_enrichment
        api_key_env = owner_settings.openrouter_api_key_env

        api_key = os.getenv(api_key_env)

        if not api_key:
            logger.debug(f"No API key found in environment variable {api_key_env}")
            return

        logger.info(f"Found API key in environment variable {api_key_env}")

        # Validate API key
        validation = self.validator.validate_openrouter_api_key(
            api_key,
            model=owner_settings.openrouter_default_model
        )

        if validation.is_valid:
            logger.info(f"API key validated: {validation.message}")
            config_manager.save_api_key(
                api_key,
                model=owner_settings.openrouter_default_model,
                validated=True
            )
        else:
            logger.warning(f"API key validation failed: {validation.error}")
            logger.warning("Saving key anyway - it may need to be updated")
            config_manager.save_api_key(
                api_key,
                model=owner_settings.openrouter_default_model,
                validated=False
            )


def run_migration_if_needed(yaml_path: Path, db_path: Path) -> bool:
    """Run migration if needed (convenience function).

    Args:
        yaml_path: Path to config.yaml
        db_path: Path to database file

    Returns:
        True if migration was run (or not needed), False if migration failed
    """
    migration = ConfigMigration(yaml_path, db_path)

    if not migration.needs_migration():
        logger.debug("Migration not needed")
        return True

    logger.info("=== Running configuration migration ===")
    success = migration.migrate()

    if success:
        logger.info("=== Migration completed successfully ===")
    else:
        logger.error("=== Migration failed - manual configuration may be required ===")

    return success
