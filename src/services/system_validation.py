"""System validation service for pre-flight checks and settings validation."""

import os
import sys
import logging
import subprocess
import re
from pathlib import Path
from typing import Dict, Optional

try:
    import httpx
except ImportError:
    httpx = None

from ..models.system_settings import ValidationResult


logger = logging.getLogger(__name__)


class ValidationError(Exception):
    """Raised when validation fails."""
    pass


class SystemValidationService:
    """Service for validating system configuration and settings."""

    # Minimum versions
    MIN_CHROME_VERSION = 90
    MAX_PYTHON_VERSION = (3, 10)  # Must be < 3.10 for Playwright

    def __init__(self):
        """Initialize validation service."""
        pass

    def validate_chrome(self, path: str) -> ValidationResult:
        """Validate Chrome browser executable.

        Args:
            path: Path to Chrome executable

        Returns:
            ValidationResult with validation status and details
        """
        if not path or not path.strip():
            return ValidationResult(
                is_valid=False,
                message="Chrome path is empty",
                error="Path cannot be empty"
            )

        path = path.strip()

        # Check if path exists
        if not os.path.exists(path):
            return ValidationResult(
                is_valid=False,
                message="Chrome executable not found",
                error=f"File does not exist: {path}"
            )

        # Check if it's a file
        if not os.path.isfile(path):
            return ValidationResult(
                is_valid=False,
                message="Path is not a file",
                error=f"Expected a file, got: {path}"
            )

        # Try to run --version command
        try:
            result = subprocess.run(
                [path, '--version'],
                capture_output=True,
                text=True,
                timeout=10
            )

            if result.returncode != 0:
                return ValidationResult(
                    is_valid=False,
                    message="Chrome executable failed to run",
                    error=f"Exit code: {result.returncode}, stderr: {result.stderr}"
                )

            version_output = result.stdout.strip()

            # Extract version number
            match = re.search(r'(\d+\.[\d.]+)', version_output)
            if not match:
                return ValidationResult(
                    is_valid=False,
                    message="Could not parse Chrome version",
                    error=f"Unexpected version output: {version_output}"
                )

            version_str = match.group(1)
            major_version = int(version_str.split('.')[0])

            # Check minimum version
            if major_version < self.MIN_CHROME_VERSION:
                return ValidationResult(
                    is_valid=False,
                    message=f"Chrome version too old: {version_str}",
                    error=f"Minimum version required: {self.MIN_CHROME_VERSION}",
                    details={'version': version_str, 'minimum': self.MIN_CHROME_VERSION}
                )

            return ValidationResult(
                is_valid=True,
                message=f"Chrome validated successfully",
                details={
                    'version': version_str,
                    'full_output': version_output,
                    'path': path
                }
            )

        except subprocess.TimeoutExpired:
            return ValidationResult(
                is_valid=False,
                message="Chrome version check timed out",
                error="Executable took too long to respond (>10s)"
            )
        except FileNotFoundError:
            return ValidationResult(
                is_valid=False,
                message="Chrome executable not found",
                error=f"Cannot execute file: {path}"
            )
        except Exception as e:
            logger.error(f"Chrome validation failed: {e}")
            return ValidationResult(
                is_valid=False,
                message="Chrome validation failed",
                error=str(e)
            )

    def validate_openrouter_api_key(
        self,
        api_key: str,
        model: Optional[str] = None
    ) -> ValidationResult:
        """Validate OpenRouter API key by making a test request.

        Args:
            api_key: OpenRouter API key to validate
            model: Optional model ID to check availability

        Returns:
            ValidationResult with validation status and details
        """
        if not api_key or not api_key.strip():
            return ValidationResult(
                is_valid=False,
                message="API key is empty",
                error="API key cannot be empty"
            )

        api_key = api_key.strip()

        # Basic format validation
        if not api_key.startswith('sk-or-v1-'):
            return ValidationResult(
                is_valid=False,
                message="Invalid API key format",
                error="OpenRouter API keys should start with 'sk-or-v1-'"
            )

        if len(api_key) < 50:
            return ValidationResult(
                is_valid=False,
                message="API key too short",
                error="API key appears to be incomplete"
            )

        # Check if httpx is available
        if httpx is None:
            return ValidationResult(
                is_valid=False,
                message="httpx library not installed",
                error="Cannot validate API key without httpx library. Install with: pip install httpx"
            )

        # Make test API request to list models
        try:
            with httpx.Client(timeout=30.0) as client:
                response = client.get(
                    'https://openrouter.ai/api/v1/models',
                    headers={
                        'Authorization': f'Bearer {api_key}',
                        'HTTP-Referer': 'https://github.com/google-maps-scraper',
                        'X-Title': 'Google Maps Scraper'
                    }
                )

                # Check response status
                if response.status_code == 401:
                    return ValidationResult(
                        is_valid=False,
                        message="API key authentication failed",
                        error="Invalid API key or unauthorized"
                    )
                elif response.status_code == 403:
                    return ValidationResult(
                        is_valid=False,
                        message="API key access forbidden",
                        error="API key does not have required permissions"
                    )
                elif response.status_code != 200:
                    return ValidationResult(
                        is_valid=False,
                        message=f"API request failed with status {response.status_code}",
                        error=response.text[:200]
                    )

                # Parse response
                data = response.json()

                if 'data' not in data:
                    return ValidationResult(
                        is_valid=False,
                        message="Unexpected API response format",
                        error="Response missing 'data' field"
                    )

                models = data['data']
                total_models = len(models)

                # Count free models
                free_models = [m for m in models if 'free' in m.get('id', '').lower() or
                              m.get('pricing', {}).get('prompt', '0') == '0']
                free_count = len(free_models)

                # If specific model requested, check if it's available
                model_available = True
                if model:
                    model_ids = [m.get('id') for m in models]
                    model_available = model in model_ids

                if model and not model_available:
                    return ValidationResult(
                        is_valid=False,
                        message=f"Model '{model}' not available",
                        error=f"The specified model is not available with this API key",
                        details={
                            'total_models': total_models,
                            'free_models': free_count,
                            'requested_model': model
                        }
                    )

                return ValidationResult(
                    is_valid=True,
                    message="API key validated successfully",
                    details={
                        'total_models': total_models,
                        'free_models': free_count,
                        'tested_model': model if model else None,
                        'model_available': model_available if model else None
                    }
                )

        except httpx.TimeoutException:
            return ValidationResult(
                is_valid=False,
                message="API validation timed out",
                error="OpenRouter API did not respond within 30 seconds"
            )
        except httpx.RequestError as e:
            return ValidationResult(
                is_valid=False,
                message="Network error during API validation",
                error=f"Could not connect to OpenRouter API: {str(e)}"
            )
        except Exception as e:
            logger.error(f"API key validation failed: {e}")
            return ValidationResult(
                is_valid=False,
                message="API key validation failed",
                error=str(e)
            )

    def run_system_checks(self, chrome_path: Optional[str] = None) -> Dict[str, ValidationResult]:
        """Run comprehensive system checks.

        Args:
            chrome_path: Optional Chrome path to validate

        Returns:
            Dictionary of check name to ValidationResult
        """
        checks = {}

        # Python version check
        checks['python_version'] = self._check_python_version()

        # Playwright check
        checks['playwright'] = self._check_playwright()

        # Crawl4AI check (optional)
        checks['crawl4ai'] = self._check_crawl4ai()

        # Chrome check (if path provided)
        if chrome_path:
            checks['chrome'] = self.validate_chrome(chrome_path)

        return checks

    def _check_python_version(self) -> ValidationResult:
        """Check Python version compatibility.

        Returns:
            ValidationResult for Python version check
        """
        current_version = sys.version_info[:2]
        version_str = f"{current_version[0]}.{current_version[1]}"

        if current_version >= self.MAX_PYTHON_VERSION:
            return ValidationResult(
                is_valid=False,
                message=f"Python version {version_str} not supported",
                error=f"Playwright requires Python < {self.MAX_PYTHON_VERSION[0]}.{self.MAX_PYTHON_VERSION[1]}",
                details={
                    'current': version_str,
                    'maximum': f"{self.MAX_PYTHON_VERSION[0]}.{self.MAX_PYTHON_VERSION[1]}"
                }
            )

        return ValidationResult(
            is_valid=True,
            message=f"Python {version_str}",
            details={'version': version_str, 'supported': True}
        )

    def _check_playwright(self) -> ValidationResult:
        """Check if Playwright is installed.

        Returns:
            ValidationResult for Playwright check
        """
        try:
            import playwright
            version = getattr(playwright, '__version__', 'unknown')

            return ValidationResult(
                is_valid=True,
                message="Playwright installed",
                details={'version': version}
            )
        except ImportError:
            return ValidationResult(
                is_valid=False,
                message="Playwright not installed",
                error="Install with: pip install playwright && playwright install chromium"
            )

    def _check_crawl4ai(self) -> ValidationResult:
        """Check if Crawl4AI is installed (optional).

        Returns:
            ValidationResult for Crawl4AI check
        """
        try:
            import crawl4ai
            version = getattr(crawl4ai, '__version__', 'unknown')

            return ValidationResult(
                is_valid=True,
                message="Crawl4AI installed (optional for owner enrichment)",
                details={'version': version, 'optional': True}
            )
        except ImportError:
            return ValidationResult(
                is_valid=True,  # Still valid since it's optional
                message="Crawl4AI not installed (optional)",
                details={
                    'optional': True,
                    'install_cmd': 'pip install "crawl4ai @ git+https://github.com/unclecode/crawl4ai.git" && crawl4ai install browser'
                }
            )
