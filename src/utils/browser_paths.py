"""Locate a Chrome/Chromium executable across supported operating systems."""

from __future__ import annotations

import os
import platform
import shutil
import sys
from pathlib import Path
from typing import Optional


def _candidate_paths() -> list[Path]:
    system = platform.system().lower()
    paths: list[Path] = []

    if system == "windows":
        program_files = os.environ.get("PROGRAMFILES", r"C:\\Program Files")
        program_files_x86 = os.environ.get("PROGRAMFILES(X86)", r"C:\\Program Files (x86)")
        local_app_data = os.environ.get("LOCALAPPDATA")
        for base in filter(None, {program_files, program_files_x86}):
            paths.append(Path(base) / "Google" / "Chrome" / "Application" / "chrome.exe")
            paths.append(Path(base) / "Chromium" / "Application" / "chrome.exe")
        if local_app_data:
            paths.append(Path(local_app_data) / "Google" / "Chrome" / "Application" / "chrome.exe")
            paths.append(Path(local_app_data) / "Chromium" / "Application" / "chrome.exe")

    elif system == "darwin":
        paths.append(Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"))
        paths.append(Path("/Applications/Chromium.app/Contents/MacOS/Chromium"))
        home = Path.home()
        paths.append(home / "Applications" / "Google Chrome.app" / "Contents" / "MacOS" / "Google Chrome")
        paths.append(home / "Applications" / "Chromium.app" / "Contents" / "MacOS" / "Chromium")

    else:  # Linux and other Unix-like systems
        common_linux = [
            "/usr/bin/google-chrome",
            "/usr/bin/google-chrome-stable",
            "/usr/bin/chromium",
            "/usr/bin/chromium-browser",
            "/snap/bin/chromium",
        ]
        paths.extend(Path(p) for p in common_linux)

    return paths


def resolve_chrome_binary(explicit_path: Optional[str] = None) -> Optional[str]:
    """Return a usable Chrome/Chromium executable or ``None``.

    Order of resolution:
      1. Explicit path provided via config/env.
      2. System ``chrome``/``chromium`` discovered by ``shutil.which``.
      3. Known per-OS installation paths.

    If no binary is found, callers can fall back to Playwright's embedded Chromium
    by omitting ``executable_path`` when launching.
    """

    if explicit_path:
        expanded = Path(explicit_path).expanduser()
        if expanded.is_file():
            return str(expanded)

    for candidate in ("google-chrome", "chromium", "chromium-browser", "chrome"):
        located = shutil.which(candidate)
        if located:
            return located

    for candidate_path in _candidate_paths():
        if candidate_path.is_file():
            return str(candidate_path)

    return None
