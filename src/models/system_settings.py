"""Database models for system settings and configuration."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any


@dataclass
class SystemSettings:
    """System-level settings stored in database."""

    id: int = 1  # Singleton - always 1
    onboarding_completed: bool = False
    chrome_path: Optional[str] = None
    chrome_validated: bool = False
    chrome_last_validated: Optional[datetime] = None
    openrouter_api_key: Optional[str] = None  # Stored encrypted
    openrouter_validated: bool = False
    openrouter_last_validated: Optional[datetime] = None
    openrouter_model: Optional[str] = None
    owner_enrichment_enabled: bool = False
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'id': self.id,
            'onboarding_completed': self.onboarding_completed,
            'chrome_path': self.chrome_path,
            'chrome_validated': self.chrome_validated,
            'chrome_last_validated': self.chrome_last_validated.isoformat() if self.chrome_last_validated else None,
            'openrouter_api_key': self.openrouter_api_key,
            'openrouter_validated': self.openrouter_validated,
            'openrouter_last_validated': self.openrouter_last_validated.isoformat() if self.openrouter_last_validated else None,
            'openrouter_model': self.openrouter_model,
            'owner_enrichment_enabled': self.owner_enrichment_enabled,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SystemSettings':
        """Create from dictionary."""
        # Parse datetime fields
        for field_name in ['chrome_last_validated', 'openrouter_last_validated', 'created_at', 'updated_at']:
            if field_name in data and data[field_name] and isinstance(data[field_name], str):
                data[field_name] = datetime.fromisoformat(data[field_name])

        return cls(**{k: v for k, v in data.items() if k in cls.__annotations__})


@dataclass
class UserPreferences:
    """User preferences for UI defaults."""

    id: int = 1  # Singleton - always 1
    default_headless: bool = True
    default_grid_size: int = 2
    default_scraping_mode: str = 'fast'
    theme: str = 'light'
    updated_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'id': self.id,
            'default_headless': self.default_headless,
            'default_grid_size': self.default_grid_size,
            'default_scraping_mode': self.default_scraping_mode,
            'theme': self.theme,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'UserPreferences':
        """Create from dictionary."""
        if 'updated_at' in data and data['updated_at'] and isinstance(data['updated_at'], str):
            data['updated_at'] = datetime.fromisoformat(data['updated_at'])

        return cls(**{k: v for k, v in data.items() if k in cls.__annotations__})


@dataclass
class ValidationResult:
    """Result of a validation operation."""

    is_valid: bool
    message: str
    details: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'is_valid': self.is_valid,
            'message': self.message,
            'details': self.details,
            'error': self.error,
        }


@dataclass
class BrowserCandidate:
    """A detected browser installation candidate."""

    path: str
    version: Optional[str] = None
    is_valid: bool = False
    detection_method: str = 'unknown'
    validation_error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'path': self.path,
            'version': self.version,
            'is_valid': self.is_valid,
            'detection_method': self.detection_method,
            'validation_error': self.validation_error,
        }
