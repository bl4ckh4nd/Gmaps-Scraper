"""Database repository for system settings using SQLite."""

import sqlite3
import logging
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List

from ..models.system_settings import SystemSettings, UserPreferences


logger = logging.getLogger(__name__)


class DatabaseError(Exception):
    """Raised when database operations fail."""
    pass


class SettingsRepository:
    """Repository for managing system settings in SQLite database."""

    # Whitelist of allowed settings keys for update operations
    SYSTEM_SETTINGS_WHITELIST = {
        'onboarding_completed', 'chrome_path', 'chrome_validated',
        'chrome_last_validated', 'openrouter_api_key', 'openrouter_validated',
        'openrouter_last_validated', 'openrouter_model', 'owner_enrichment_enabled'
    }

    USER_PREFERENCES_WHITELIST = {
        'default_headless', 'default_grid_size', 'default_scraping_mode', 'theme'
    }

    def __init__(self, db_path: Path):
        """Initialize repository.

        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self._ensure_directory()
        self._ensure_schema()

    def _ensure_directory(self) -> None:
        """Ensure database directory exists."""
        try:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logger.error(f"Failed to create database directory: {e}")
            raise DatabaseError(f"Could not create database directory: {e}")

    @contextmanager
    def _get_connection(self):
        """Get database connection with automatic commit/rollback.

        Yields:
            sqlite3.Connection: Database connection

        Raises:
            DatabaseError: If connection fails
        """
        conn = None
        try:
            conn = sqlite3.connect(str(self.db_path))
            conn.row_factory = sqlite3.Row  # Enable column access by name
            yield conn
            conn.commit()
        except sqlite3.Error as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise DatabaseError(f"Database operation failed: {e}")
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Unexpected error in database operation: {e}")
            raise DatabaseError(f"Unexpected database error: {e}")
        finally:
            if conn:
                conn.close()

    def _ensure_schema(self) -> None:
        """Create database schema if it doesn't exist."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # System settings table (singleton)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS system_settings (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    onboarding_completed BOOLEAN NOT NULL DEFAULT 0,
                    chrome_path TEXT,
                    chrome_validated BOOLEAN DEFAULT 0,
                    chrome_last_validated TIMESTAMP,
                    openrouter_api_key TEXT,
                    openrouter_validated BOOLEAN DEFAULT 0,
                    openrouter_last_validated TIMESTAMP,
                    openrouter_model TEXT,
                    owner_enrichment_enabled BOOLEAN DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # User preferences table (singleton)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_preferences (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    default_headless BOOLEAN DEFAULT 1,
                    default_grid_size INTEGER DEFAULT 2,
                    default_scraping_mode TEXT DEFAULT 'fast',
                    theme TEXT DEFAULT 'light',
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Settings history table (audit trail)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS settings_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    setting_key TEXT NOT NULL,
                    old_value TEXT,
                    new_value TEXT,
                    changed_by TEXT DEFAULT 'user',
                    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create indices for history lookups
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_settings_history_key
                ON settings_history(setting_key)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_settings_history_timestamp
                ON settings_history(changed_at)
            """)

            # Initialize default rows if they don't exist
            cursor.execute("SELECT COUNT(*) FROM system_settings WHERE id = 1")
            if cursor.fetchone()[0] == 0:
                cursor.execute("""
                    INSERT INTO system_settings (id, onboarding_completed)
                    VALUES (1, 0)
                """)
                logger.info("Initialized system_settings table with default row")

            cursor.execute("SELECT COUNT(*) FROM user_preferences WHERE id = 1")
            if cursor.fetchone()[0] == 0:
                cursor.execute("""
                    INSERT INTO user_preferences (id)
                    VALUES (1)
                """)
                logger.info("Initialized user_preferences table with default row")

    def get_system_settings(self) -> SystemSettings:
        """Get system settings.

        Returns:
            SystemSettings instance

        Raises:
            DatabaseError: If retrieval fails
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM system_settings WHERE id = 1")
            row = cursor.fetchone()

            if not row:
                # Should not happen due to schema initialization, but handle gracefully
                logger.warning("System settings row missing, returning defaults")
                return SystemSettings()

            # Convert row to dict
            data = dict(row)

            # Parse datetime fields
            for field in ['chrome_last_validated', 'openrouter_last_validated', 'created_at', 'updated_at']:
                if data.get(field):
                    try:
                        data[field] = datetime.fromisoformat(data[field])
                    except (ValueError, TypeError):
                        data[field] = None

            return SystemSettings.from_dict(data)

    def update_system_settings(self, settings: Dict[str, Any]) -> None:
        """Update system settings.

        Args:
            settings: Dictionary of settings to update (keys must be in whitelist)

        Raises:
            DatabaseError: If update fails
            ValueError: If invalid setting key provided
        """
        # Validate keys against whitelist
        invalid_keys = set(settings.keys()) - self.SYSTEM_SETTINGS_WHITELIST
        if invalid_keys:
            raise ValueError(f"Invalid setting keys: {invalid_keys}")

        if not settings:
            return

        # Get current values for history
        current_settings = self.get_system_settings()

        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Build UPDATE query dynamically from whitelisted keys
            set_clauses = []
            values = []

            for key, value in settings.items():
                set_clauses.append(f"{key} = ?")
                # Convert datetime objects to ISO format
                if isinstance(value, datetime):
                    value = value.isoformat()
                values.append(value)

            # Always update updated_at timestamp
            set_clauses.append("updated_at = CURRENT_TIMESTAMP")

            query = f"""
                UPDATE system_settings
                SET {', '.join(set_clauses)}
                WHERE id = 1
            """

            cursor.execute(query, values)

            # Record history for each changed setting
            for key, new_value in settings.items():
                old_value = getattr(current_settings, key, None)
                if old_value != new_value:
                    self._record_history(
                        cursor,
                        f"system_settings.{key}",
                        str(old_value) if old_value is not None else None,
                        str(new_value) if new_value is not None else None
                    )

            logger.info(f"Updated system settings: {list(settings.keys())}")

    def get_user_preferences(self) -> UserPreferences:
        """Get user preferences.

        Returns:
            UserPreferences instance

        Raises:
            DatabaseError: If retrieval fails
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM user_preferences WHERE id = 1")
            row = cursor.fetchone()

            if not row:
                logger.warning("User preferences row missing, returning defaults")
                return UserPreferences()

            data = dict(row)

            # Parse updated_at
            if data.get('updated_at'):
                try:
                    data['updated_at'] = datetime.fromisoformat(data['updated_at'])
                except (ValueError, TypeError):
                    data['updated_at'] = None

            return UserPreferences.from_dict(data)

    def update_user_preferences(self, prefs: Dict[str, Any]) -> None:
        """Update user preferences.

        Args:
            prefs: Dictionary of preferences to update (keys must be in whitelist)

        Raises:
            DatabaseError: If update fails
            ValueError: If invalid preference key provided
        """
        # Validate keys against whitelist
        invalid_keys = set(prefs.keys()) - self.USER_PREFERENCES_WHITELIST
        if invalid_keys:
            raise ValueError(f"Invalid preference keys: {invalid_keys}")

        if not prefs:
            return

        # Get current values for history
        current_prefs = self.get_user_preferences()

        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Build UPDATE query
            set_clauses = []
            values = []

            for key, value in prefs.items():
                set_clauses.append(f"{key} = ?")
                values.append(value)

            set_clauses.append("updated_at = CURRENT_TIMESTAMP")

            query = f"""
                UPDATE user_preferences
                SET {', '.join(set_clauses)}
                WHERE id = 1
            """

            cursor.execute(query, values)

            # Record history
            for key, new_value in prefs.items():
                old_value = getattr(current_prefs, key, None)
                if old_value != new_value:
                    self._record_history(
                        cursor,
                        f"user_preferences.{key}",
                        str(old_value) if old_value is not None else None,
                        str(new_value) if new_value is not None else None
                    )

            logger.info(f"Updated user preferences: {list(prefs.keys())}")

    def _record_history(
        self,
        cursor: sqlite3.Cursor,
        setting_key: str,
        old_value: Optional[str],
        new_value: Optional[str]
    ) -> None:
        """Record a setting change in history.

        Args:
            cursor: Database cursor (within existing transaction)
            setting_key: Full key of setting (e.g., 'system_settings.chrome_path')
            old_value: Previous value (as string)
            new_value: New value (as string)
        """
        cursor.execute("""
            INSERT INTO settings_history (setting_key, old_value, new_value)
            VALUES (?, ?, ?)
        """, (setting_key, old_value, new_value))

    def get_history(
        self,
        setting_key: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get settings change history.

        Args:
            setting_key: Filter by specific setting key (optional)
            limit: Maximum number of records to return

        Returns:
            List of history records (most recent first)

        Raises:
            DatabaseError: If retrieval fails
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()

            if setting_key:
                cursor.execute("""
                    SELECT * FROM settings_history
                    WHERE setting_key = ?
                    ORDER BY changed_at DESC
                    LIMIT ?
                """, (setting_key, limit))
            else:
                cursor.execute("""
                    SELECT * FROM settings_history
                    ORDER BY changed_at DESC
                    LIMIT ?
                """, (limit,))

            rows = cursor.fetchall()
            return [dict(row) for row in rows]

    def clear_history(self, older_than_days: Optional[int] = None) -> int:
        """Clear settings history.

        Args:
            older_than_days: Only clear records older than this many days (optional)

        Returns:
            Number of records deleted

        Raises:
            DatabaseError: If deletion fails
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()

            if older_than_days:
                cursor.execute("""
                    DELETE FROM settings_history
                    WHERE changed_at < datetime('now', '-' || ? || ' days')
                """, (older_than_days,))
            else:
                cursor.execute("DELETE FROM settings_history")

            deleted_count = cursor.rowcount
            logger.info(f"Cleared {deleted_count} history records")
            return deleted_count
