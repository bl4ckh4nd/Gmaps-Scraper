"""Rich CLI progress output for the Google Maps scraper.

Prints structured, human-readable progress to stdout while the log file
continues to receive structured logger output unchanged.
"""

from __future__ import annotations

import sys
import time
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ..models.business import Business

_WIDTH = 72


# ── formatting helpers ────────────────────────────────────────────────────────

def _bar(done: int, total: int, width: int = 28) -> str:
    if total <= 0:
        return "░" * width
    filled = min(width, int(width * done / total))
    return "▓" * filled + "░" * (width - filled)


def _fmt_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m:02d}m"
    if m:
        return f"{m}m {s:02d}s"
    return f"{s}s"


def _fmt_eta(elapsed: float, done: int, total: int) -> str:
    if done <= 0 or elapsed <= 1.0:
        return "calculating…"
    remaining = total - done
    if remaining <= 0:
        return "done"
    eta_secs = elapsed * remaining / done
    return f"~{_fmt_duration(eta_secs)}"


def _rule(char: str = "═", width: int = _WIDTH) -> str:
    return char * width


# ── main class ────────────────────────────────────────────────────────────────

class CliProgressPrinter:
    """Writes structured real-time progress to stdout (or any file-like object)."""

    def __init__(self, file=None) -> None:
        self._file = file or sys.stdout
        self._run_start: float = 0.0

    # ── internal ──────────────────────────────────────────────────────────────

    def _out(self, text: str = "") -> None:
        print(text, file=self._file, flush=True)

    @property
    def _elapsed(self) -> float:
        return time.monotonic() - self._run_start

    # ── public API ────────────────────────────────────────────────────────────

    def print_run_header(
        self,
        search_term: str,
        grid_size: int,
        total_cells: int,
        area_km2: float,
        total_target: int,
        scraping_mode: str,
        bounds: tuple,
        result_file: str,
    ) -> None:
        self._run_start = time.monotonic()
        min_lat, min_lng, max_lat, max_lng = bounds
        self._out()
        self._out(_rule("═"))
        self._out(f"  Google Maps Scraper  ·  {search_term}")
        self._out(
            f"  Grid: {grid_size}×{grid_size}  ·  {total_cells} cells"
            f"  ·  {area_km2:.1f} km²  ·  Target: {total_target}  ·  Mode: {scraping_mode}"
        )
        self._out(
            f"  Bounds: {min_lat:.4f}°N–{max_lat:.4f}°N,"
            f" {min_lng:.4f}°E–{max_lng:.4f}°E"
        )
        self._out(f"  Output : {result_file}")
        self._out(_rule("═"))
        self._out()

    def print_cell_header(
        self,
        cell_idx: int,
        total_cells: int,
        cell_id: str,
        cell_target: int,
        cell_min_lat: Optional[float] = None,
        cell_max_lat: Optional[float] = None,
        cell_min_lng: Optional[float] = None,
        cell_max_lng: Optional[float] = None,
    ) -> None:
        label = f" Cell {cell_idx}/{total_cells}  [{cell_id}] "
        bar_len = max(0, _WIDTH - len(label) - 2)
        self._out(f"┌─{label}{'─' * bar_len}┐")
        if all(v is not None for v in (cell_min_lat, cell_max_lat, cell_min_lng, cell_max_lng)):
            self._out(
                f"│  Bounds: {cell_min_lat:.4f}°N–{cell_max_lat:.4f}°N,"
                f" {cell_min_lng:.4f}°E–{cell_max_lng:.4f}°E"
            )
        self._out(f"│  Target: {cell_target} results")
        self._out(f"└{'─' * (_WIDTH - 2)}┘")
        self._out()

    def print_listing_result(
        self,
        global_done: int,
        total_target: int,
        status: str,
        business: Optional["Business"],
        review_count: int,
        listing_elapsed: float,
        url: str = "",
    ) -> None:
        """Print one listing outcome line.

        status: "new" | "duplicate" | "failed"
        """
        idx_str = f"[{global_done:4d}/{total_target}]"
        eta = _fmt_eta(self._elapsed, global_done, total_target)

        if status == "new" and business is not None:
            name = business.name or "(no name)"
            rating_str = (
                f"★ {business.review_average:.1f}" if business.review_average else "★ —  "
            )
            rev_count = business.review_count or 0
            address = (business.address or "").strip()

            deleted_part = ""
            if business.deleted_review_notice:
                mn = business.deleted_review_count_min
                mx = business.deleted_review_count_max
                if mn is not None and mx is not None and mn != mx:
                    del_str = f"{mn}–{mx}"
                elif mn is not None:
                    del_str = str(mn)
                else:
                    del_str = "?"
                rate_pct = business.deleted_review_rate_mid_pct
                rate_str = f" (↓{rate_pct:.1f}%)" if rate_pct else ""
                deleted_part = f"  ·  🗑 {del_str} deleted{rate_str}"

            self._out(f"  {idx_str}  ✔  {name}")
            self._out(
                f"               {rating_str}  ·  {rev_count} reviews"
                f"  ·  {review_count} new reviews{deleted_part}"
            )
            if address:
                self._out(f"               📍 {address}")
            self._out(
                f"               ⏱  {_fmt_duration(listing_elapsed)}"
                f"  ·  ETA {eta}"
            )

        elif status == "duplicate" and business is not None:
            name = business.name or "(no name)"
            self._out(
                f"  {idx_str}  ⟳  {name}"
                f"  (duplicate – skipped)"
            )

        else:
            short_url = (url[:52] + "…") if len(url) > 53 else url
            self._out(f"  {idx_str}  ✗  extraction failed  —  {short_url}")

        self._out()

    def print_cell_summary(
        self,
        cell_idx: int,
        total_cells: int,
        cell_id: str,
        new_count: int,
        dupe_count: int,
        error_count: int,
        cell_elapsed: float,
        global_done: int,
        total_target: int,
    ) -> None:
        eta = _fmt_eta(self._elapsed, global_done, total_target)
        bar = _bar(global_done, total_target)
        pct = 100.0 * global_done / total_target if total_target else 0.0

        label = f" Cell {cell_idx}/{total_cells} [{cell_id}] done "
        bar_len = max(0, _WIDTH - len(label) - 2)
        self._out(f"└─{label}{'─' * bar_len}┘")
        self._out(
            f"  new: {new_count}  ·  dupes: {dupe_count}"
            f"  ·  errors: {error_count}  ·  elapsed: {_fmt_duration(cell_elapsed)}"
        )
        self._out()
        self._out(
            f"  {bar}  {global_done}/{total_target}  ({pct:.1f}%)"
            f"  ·  ETA {eta}"
        )
        self._out()

    def print_run_summary(
        self,
        total_businesses: int,
        total_reviews: int,
        result_file: str,
        reviews_file: str,
        failed: bool = False,
        failure_reason: str = "",
    ) -> None:
        elapsed = self._elapsed
        self._out()
        self._out(_rule("═"))
        if failed:
            self._out(f"  ⚠  Scraping stopped early")
            if failure_reason:
                self._out(f"     {failure_reason}")
        else:
            self._out("  ✔  Scraping completed successfully!")
        self._out(f"  Businesses : {total_businesses}")
        self._out(f"  Reviews    : {total_reviews}")
        self._out(f"  Elapsed    : {_fmt_duration(elapsed)}")
        self._out(f"  Results    : {result_file}")
        self._out(f"  Reviews    : {reviews_file}")
        self._out(_rule("═"))
        self._out()
