"""Browser detection service for auto-detecting Chrome/Chromium installations."""

import os
import platform
import subprocess
import logging
import re
from pathlib import Path
from typing import List, Optional

from ..models.system_settings import BrowserCandidate


logger = logging.getLogger(__name__)


class BrowserDetectionError(Exception):
    """Raised when browser detection fails."""
    pass


class BrowserDetector:
    """Detects Chrome/Chromium browser installations across platforms."""

    # Minimum Chrome version required
    MIN_CHROME_VERSION = 90

    def __init__(self):
        """Initialize browser detector."""
        self.system = platform.system().lower()

    def detect_browsers(self) -> List[BrowserCandidate]:
        """Detect all Chrome/Chromium browser installations.

        Returns:
            List of BrowserCandidate objects, sorted by reliability

        Raises:
            BrowserDetectionError: If detection process fails
        """
        candidates = []

        try:
            # Check environment variables first (highest priority)
            env_candidates = self._detect_from_environment()
            candidates.extend(env_candidates)

            # Platform-specific detection
            if self.system == 'windows':
                candidates.extend(self._detect_windows())
            elif self.system == 'darwin':  # macOS
                candidates.extend(self._detect_macos())
            elif self.system == 'linux':
                candidates.extend(self._detect_linux())
            else:
                logger.warning(f"Unsupported platform: {self.system}")

            # Validate all candidates
            for candidate in candidates:
                self._validate_candidate(candidate)

            # Sort by reliability: valid first, then by detection method priority
            candidates.sort(
                key=lambda c: (
                    not c.is_valid,  # Valid first
                    self._get_method_priority(c.detection_method)  # Then by method
                )
            )

            logger.info(f"Detected {len(candidates)} browser candidates, "
                       f"{sum(1 for c in candidates if c.is_valid)} valid")

            return candidates

        except Exception as e:
            logger.error(f"Browser detection failed: {e}")
            raise BrowserDetectionError(f"Failed to detect browsers: {e}")

    def get_best_candidate(self) -> Optional[BrowserCandidate]:
        """Get the best browser candidate.

        Returns:
            Best valid BrowserCandidate or None if none found
        """
        candidates = self.detect_browsers()
        valid_candidates = [c for c in candidates if c.is_valid]

        if valid_candidates:
            return valid_candidates[0]

        return None

    def _detect_from_environment(self) -> List[BrowserCandidate]:
        """Detect browser from environment variables.

        Returns:
            List of BrowserCandidate objects
        """
        candidates = []

        for env_var in ['CHROME_PATH', 'CHROME_BIN', 'CHROMIUM_PATH']:
            path = os.getenv(env_var)
            if path and os.path.exists(path):
                candidates.append(BrowserCandidate(
                    path=path,
                    detection_method='environment_variable'
                ))
                logger.debug(f"Found browser in {env_var}: {path}")

        return candidates

    def _detect_windows(self) -> List[BrowserCandidate]:
        """Detect browsers on Windows.

        Returns:
            List of BrowserCandidate objects
        """
        candidates = []

        # Common Windows paths
        common_paths = [
            Path(r'C:\Program Files\Google\Chrome\Application\chrome.exe'),
            Path(r'C:\Program Files (x86)\Google\Chrome\Application\chrome.exe'),
            Path(os.path.expandvars(r'%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe')),
            Path(os.path.expandvars(r'%PROGRAMFILES%\Google\Chrome\Application\chrome.exe')),
            Path(os.path.expandvars(r'%PROGRAMFILES(X86)%\Google\Chrome\Application\chrome.exe')),
        ]

        for path in common_paths:
            if path.exists():
                candidates.append(BrowserCandidate(
                    path=str(path),
                    detection_method='common_path'
                ))
                logger.debug(f"Found browser at common path: {path}")

        # Try Windows Registry
        try:
            import winreg
            registry_paths = [
                (winreg.HKEY_LOCAL_MACHINE, r'SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\chrome.exe'),
                (winreg.HKEY_CURRENT_USER, r'SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\chrome.exe'),
            ]

            for hive, subkey in registry_paths:
                try:
                    with winreg.OpenKey(hive, subkey) as key:
                        path, _ = winreg.QueryValueEx(key, '')
                        if os.path.exists(path):
                            candidates.append(BrowserCandidate(
                                path=path,
                                detection_method='windows_registry'
                            ))
                            logger.debug(f"Found browser in registry: {path}")
                except FileNotFoundError:
                    continue
                except Exception as e:
                    logger.debug(f"Registry lookup failed for {subkey}: {e}")

        except ImportError:
            logger.debug("winreg module not available (not on Windows)")
        except Exception as e:
            logger.debug(f"Registry detection failed: {e}")

        return candidates

    def _detect_macos(self) -> List[BrowserCandidate]:
        """Detect browsers on macOS.

        Returns:
            List of BrowserCandidate objects
        """
        candidates = []

        # Common macOS paths
        common_paths = [
            Path('/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'),
            Path(os.path.expanduser('~/Applications/Google Chrome.app/Contents/MacOS/Google Chrome')),
            Path('/Applications/Chromium.app/Contents/MacOS/Chromium'),
        ]

        for path in common_paths:
            if path.exists():
                candidates.append(BrowserCandidate(
                    path=str(path),
                    detection_method='common_path'
                ))
                logger.debug(f"Found browser at: {path}")

        return candidates

    def _detect_linux(self) -> List[BrowserCandidate]:
        """Detect browsers on Linux.

        Returns:
            List of BrowserCandidate objects
        """
        candidates = []

        # Try 'which' command
        for binary_name in ['google-chrome', 'google-chrome-stable', 'chromium', 'chromium-browser']:
            try:
                result = subprocess.run(
                    ['which', binary_name],
                    capture_output=True,
                    text=True,
                    timeout=5
                )

                if result.returncode == 0:
                    path = result.stdout.strip()
                    if path and os.path.exists(path):
                        candidates.append(BrowserCandidate(
                            path=path,
                            detection_method='which_command'
                        ))
                        logger.debug(f"Found browser via which: {path}")

            except Exception as e:
                logger.debug(f"'which {binary_name}' failed: {e}")

        # Common Linux paths
        common_paths = [
            Path('/usr/bin/google-chrome'),
            Path('/usr/bin/google-chrome-stable'),
            Path('/usr/bin/chromium'),
            Path('/usr/bin/chromium-browser'),
            Path('/snap/bin/chromium'),
        ]

        for path in common_paths:
            if path.exists():
                # Avoid duplicates from 'which' results
                if not any(c.path == str(path) for c in candidates):
                    candidates.append(BrowserCandidate(
                        path=str(path),
                        detection_method='common_path'
                    ))
                    logger.debug(f"Found browser at: {path}")

        return candidates

    def _validate_candidate(self, candidate: BrowserCandidate) -> None:
        """Validate a browser candidate and populate version info.

        Args:
            candidate: BrowserCandidate to validate (modified in place)
        """
        try:
            # Check if file exists
            if not os.path.exists(candidate.path):
                candidate.is_valid = False
                candidate.validation_error = "File does not exist"
                return

            # Check if it's a file (not a directory)
            if not os.path.isfile(candidate.path):
                candidate.is_valid = False
                candidate.validation_error = "Path is not a file"
                return

            # Try to get version
            version = self._get_browser_version(candidate.path)
            if version:
                candidate.version = version

                # Check minimum version
                version_num = self._parse_version_number(version)
                if version_num and version_num >= self.MIN_CHROME_VERSION:
                    candidate.is_valid = True
                else:
                    candidate.is_valid = False
                    candidate.validation_error = f"Version {version} < minimum {self.MIN_CHROME_VERSION}"
            else:
                candidate.is_valid = False
                candidate.validation_error = "Could not determine version"

        except Exception as e:
            candidate.is_valid = False
            candidate.validation_error = str(e)
            logger.debug(f"Validation failed for {candidate.path}: {e}")

    def _get_browser_version(self, path: str) -> Optional[str]:
        """Get browser version by running --version command.

        Args:
            path: Path to browser executable

        Returns:
            Version string or None if failed
        """
        try:
            result = subprocess.run(
                [path, '--version'],
                capture_output=True,
                text=True,
                timeout=10
            )

            if result.returncode == 0:
                output = result.stdout.strip()
                # Extract version number (e.g., "Google Chrome 120.0.6099.109" -> "120.0.6099.109")
                match = re.search(r'(\d+\.[\d.]+)', output)
                if match:
                    return match.group(1)

        except subprocess.TimeoutExpired:
            logger.debug(f"Version check timed out for {path}")
        except Exception as e:
            logger.debug(f"Version check failed for {path}: {e}")

        return None

    def _parse_version_number(self, version: str) -> Optional[int]:
        """Parse major version number from version string.

        Args:
            version: Version string (e.g., "120.0.6099.109")

        Returns:
            Major version number or None if parsing failed
        """
        try:
            # Extract first number before the dot
            match = re.match(r'(\d+)', version)
            if match:
                return int(match.group(1))
        except Exception as e:
            logger.debug(f"Failed to parse version {version}: {e}")

        return None

    def _get_method_priority(self, method: str) -> int:
        """Get priority for detection method (lower is better).

        Args:
            method: Detection method name

        Returns:
            Priority number (0 = highest priority)
        """
        priority_map = {
            'environment_variable': 0,  # Explicit user configuration
            'windows_registry': 1,      # Official installation
            'which_command': 2,         # System PATH
            'common_path': 3,           # Standard locations
            'unknown': 99
        }

        return priority_map.get(method, 99)
