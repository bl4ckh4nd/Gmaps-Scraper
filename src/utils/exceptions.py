"""Custom exceptions for Google Maps scraper."""


class ScraperException(Exception):
    """Base exception for scraper errors."""
    pass


class NavigationException(ScraperException):
    """Exception raised when navigation fails."""
    pass


class ExtractionException(ScraperException):
    """Exception raised when data extraction fails."""
    pass


class PersistenceException(ScraperException):
    """Exception raised when data persistence fails."""
    pass


class ConfigurationException(ScraperException):
    """Exception raised when configuration is invalid."""
    pass


class BrowserException(ScraperException):
    """Exception raised when browser operations fail."""
    pass