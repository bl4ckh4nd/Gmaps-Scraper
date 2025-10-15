"""Lightweight dotenv loader for the scraper stack."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Iterable, Tuple


def load_env_file(path: Path) -> Dict[str, str]:
    """Parse key=value lines from a .env file without altering os.environ."""
    if not path.exists():
        return {}

    values: Dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue

        values[key] = value.strip()
    return values


def merge_env_values(values: Dict[str, str], *, override: bool = True) -> None:
    """Place values into os.environ without writing to disk."""
    for key, value in values.items():
        if not override and key in os.environ:
            continue
        os.environ[key] = value


def load_dotenv(path: Path, *, override: bool = False) -> Dict[str, str]:
    """Load key/value pairs from the provided .env path into the process environment."""
    values = load_env_file(path)
    if values:
        merge_env_values(values, override=override)
    return values


def upsert_env_file(path: Path, updates: Dict[str, str], *, remove_keys: Iterable[str] = ()) -> None:
    """Write updated key/value pairs back to the .env file."""
    existing = load_env_file(path)

    # Apply removals
    for key in remove_keys:
        existing.pop(key, None)

    # Apply updates
    for key, value in updates.items():
        existing[key] = value

    lines = [f"{key}={existing[key]}" for key in sorted(existing.keys())]
    if not lines:
        if path.exists():
            path.unlink()
        return

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
