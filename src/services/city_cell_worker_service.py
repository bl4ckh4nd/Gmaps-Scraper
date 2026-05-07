"""Process-based city cell worker orchestration for CLI city mode."""

from __future__ import annotations

import json
import math
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from ..navigation.grid_navigator import GridNavigator
from ..persistence.review_hash_index import ReviewHashIndex
from ..utils.exceptions import ScraperException
from .category_report_service import CategoryReportService


@dataclass
class CityCellJobSpec:
    """One grid cell assigned to a dedicated worker process."""

    cell_id: str
    status: str = "pending"
    attempts: int = 0
    result_file: Optional[str] = None
    reviews_file: Optional[str] = None
    progress_file: Optional[str] = None
    log_file: Optional[str] = None
    error_message: Optional[str] = None
    pid: Optional[int] = None


@dataclass
class CityCellWorkerOptions:
    """Configuration for a parallel city cell worker run."""

    city: str
    query: str
    display_search_term: str
    search_input_term: str
    bounds: tuple[float, float, float, float]
    total_results: int
    grid_size: int
    zoom_level: int
    config_path: str
    scraping_mode: str = "coverage"
    review_mode: str = "rolling_365d"
    review_window_days: int = 365
    max_reviews: Optional[int] = None
    headless: bool = True
    cell_workers: int = 1
    output_dir: Optional[str] = None
    final_business_csv: Optional[str] = None
    final_reviews_csv: Optional[str] = None
    log_level: str = "INFO"
    extract: Optional[str] = None
    skip_extract: Optional[str] = None


@dataclass
class CityCellWorkerResult:
    """Final artifact paths and counts from a parallel city cell run."""

    output_dir: str
    manifest_path: str
    business_csv: str
    reviews_csv: str
    summary_csv: Optional[str]
    total_cells: int
    completed_cells: int
    failed_cells: int


class CityCellWorkerRunner:
    """Run dedicated subprocess workers for city grid cells."""

    def __init__(self, *, category_report_service: Optional[CategoryReportService] = None) -> None:
        self.category_report_service = category_report_service or CategoryReportService()

    def run(self, options: CityCellWorkerOptions) -> CityCellWorkerResult:
        if options.cell_workers <= 0:
            raise ValueError("cell_workers must be positive")
        if options.scraping_mode != "coverage":
            raise ValueError("Parallel city cell workers currently require coverage mode")

        output_dir = Path(options.output_dir or Path.cwd()).expanduser().resolve()
        output_dir.mkdir(parents=True, exist_ok=True)
        final_business_csv = Path(
            options.final_business_csv or output_dir / "city_cell_businesses.csv"
        ).expanduser().resolve()
        final_reviews_csv = Path(
            options.final_reviews_csv or output_dir / "city_cell_reviews.csv"
        ).expanduser().resolve()
        cells_root = output_dir / "cells"
        manifest_path = output_dir / "city_cell_manifest.json"
        cells_root.mkdir(parents=True, exist_ok=True)

        manifest = self._load_or_initialize_manifest(
            options=options,
            manifest_path=manifest_path,
            final_business_csv=final_business_csv,
            final_reviews_csv=final_reviews_csv,
        )
        jobs = [CityCellJobSpec(**job) for job in manifest["jobs"]]
        self._rebuild_merged_outputs(jobs, final_business_csv, final_reviews_csv)
        self._persist_manifest(
            manifest_path,
            manifest,
            jobs,
            final_business_csv,
            final_reviews_csv,
        )

        total_cells = len(jobs)
        cell_target = math.ceil(options.total_results / total_cells)
        active_workers: dict[str, tuple[subprocess.Popen[Any], Any]] = {}

        while True:
            running_jobs = [job for job in jobs if job.status == "running"]
            if running_jobs and not active_workers:
                for job in running_jobs:
                    job.status = "pending"
                    job.pid = None

            while len(active_workers) < options.cell_workers:
                next_job = next(
                    (
                        job
                        for job in jobs
                        if job.status == "pending"
                        or (job.status == "failed" and job.attempts < 2)
                    ),
                    None,
                )
                if next_job is None:
                    break
                self._start_worker(options, next_job, cell_target, cells_root, active_workers)
                self._persist_manifest(
                    manifest_path,
                    manifest,
                    jobs,
                    final_business_csv,
                    final_reviews_csv,
                )

            completed_this_round = []
            for cell_id, (process, log_handle) in list(active_workers.items()):
                return_code = process.poll()
                if return_code is None:
                    continue
                log_handle.close()
                job = next(job for job in jobs if job.cell_id == cell_id)
                job.pid = None
                if return_code == 0:
                    job.status = "completed"
                    job.error_message = None
                    completed_this_round.append(job)
                else:
                    job.status = "failed"
                    job.error_message = self._tail_text(Path(job.log_file or ""), 20)
                del active_workers[cell_id]

            if completed_this_round:
                self._rebuild_merged_outputs(jobs, final_business_csv, final_reviews_csv)
                self._persist_manifest(
                    manifest_path,
                    manifest,
                    jobs,
                    final_business_csv,
                    final_reviews_csv,
                )

            completed_count = sum(1 for job in jobs if job.status == "completed")
            failed_count = sum(1 for job in jobs if job.status == "failed" and job.attempts >= 2)
            remaining = [job for job in jobs if job.status not in {"completed"}]
            if completed_count == total_cells:
                summary_csv = self.category_report_service.build_summary(str(final_business_csv))
                self._persist_manifest(
                    manifest_path,
                    manifest,
                    jobs,
                    final_business_csv,
                    final_reviews_csv,
                    summary_csv=summary_csv,
                )
                return CityCellWorkerResult(
                    output_dir=str(output_dir),
                    manifest_path=str(manifest_path),
                    business_csv=str(final_business_csv),
                    reviews_csv=str(final_reviews_csv),
                    summary_csv=summary_csv,
                    total_cells=total_cells,
                    completed_cells=completed_count,
                    failed_cells=failed_count,
                )

            if not active_workers and not any(job.status == "pending" for job in jobs):
                self._persist_manifest(
                    manifest_path,
                    manifest,
                    jobs,
                    final_business_csv,
                    final_reviews_csv,
                )
                raise ScraperException(
                    f"Parallel city run stopped with failed cells: "
                    f"{', '.join(job.cell_id for job in jobs if job.status == 'failed')}"
                )

            time.sleep(1.0)

    def _start_worker(
        self,
        options: CityCellWorkerOptions,
        job: CityCellJobSpec,
        cell_target: int,
        cells_root: Path,
        active_workers: dict[str, tuple[subprocess.Popen[Any], Any]],
    ) -> None:
        worker_dir = cells_root / job.cell_id
        worker_dir.mkdir(parents=True, exist_ok=True)

        job.result_file = str((worker_dir / "businesses.csv").resolve())
        job.reviews_file = str((worker_dir / "reviews.csv").resolve())
        job.progress_file = str((worker_dir / "progress.json").resolve())
        job.log_file = str((worker_dir / "console.log").resolve())
        job.attempts += 1
        job.status = "running"
        job.error_message = None

        command = self._build_worker_command(options, job, cell_target)
        log_handle = Path(job.log_file).open("a", encoding="utf-8")
        process = subprocess.Popen(
            command,
            cwd=str(Path(__file__).resolve().parents[2]),
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            text=True,
        )
        job.pid = process.pid
        active_workers[job.cell_id] = (process, log_handle)

    @staticmethod
    def _build_worker_command(
        options: CityCellWorkerOptions,
        job: CityCellJobSpec,
        cell_target: int,
    ) -> list[str]:
        command = [
            sys.executable,
            "main_new.py",
            "--config",
            options.config_path,
            "-s",
            options.display_search_term,
            "--search-input",
            options.search_input_term,
            "-t",
            str(cell_target),
            "-b",
            ",".join(str(value) for value in options.bounds),
            "-g",
            str(options.grid_size),
            "--scraping-mode",
            "fast",
            "--review-mode",
            options.review_mode,
            "--review-window-days",
            str(options.review_window_days),
            "--cell-id",
            job.cell_id,
            "--result-file",
            job.result_file or "",
            "--reviews-file",
            job.reviews_file or "",
            "--progress-file",
            job.progress_file or "",
            "--browser-state-file",
            str(Path(job.progress_file or "").resolve().with_name("browser_state.json")),
            "--log-file",
            job.log_file or "",
            "--log-level",
            options.log_level,
        ]
        if options.max_reviews is not None:
            command.extend(["--max-reviews", str(options.max_reviews)])
        if options.extract:
            command.extend(["--extract", options.extract])
        if options.skip_extract:
            command.extend(["--skip-extract", options.skip_extract])
        command.append("--headless" if options.headless else "--no-headless")
        return command

    def _load_or_initialize_manifest(
        self,
        *,
        options: CityCellWorkerOptions,
        manifest_path: Path,
        final_business_csv: Path,
        final_reviews_csv: Path,
    ) -> dict[str, Any]:
        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            existing_options = manifest.get("options", {})
            comparable = {
                "city": options.city,
                "query": options.query,
                "display_search_term": options.display_search_term,
                "bounds": list(options.bounds),
                "total_results": options.total_results,
                "grid_size": options.grid_size,
                "scraping_mode": options.scraping_mode,
                "review_mode": options.review_mode,
                "review_window_days": options.review_window_days,
                "extract": options.extract,
                "skip_extract": options.skip_extract,
            }
            if existing_options != comparable:
                raise FileExistsError(
                    f"Existing city cell manifest does not match the current run: {manifest_path}"
                )
            return manifest

        navigator = GridNavigator(options.bounds, options.grid_size, options.zoom_level)
        jobs = [CityCellJobSpec(cell_id=cell.id) for cell in navigator.grid_cells]
        manifest = {
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "options": {
                "city": options.city,
                "query": options.query,
                "display_search_term": options.display_search_term,
                "bounds": list(options.bounds),
                "total_results": options.total_results,
                "grid_size": options.grid_size,
                "scraping_mode": options.scraping_mode,
                "review_mode": options.review_mode,
                "review_window_days": options.review_window_days,
            },
            "final_business_csv": str(final_business_csv),
            "final_reviews_csv": str(final_reviews_csv),
            "summary_csv": "",
            "jobs": [asdict(job) for job in jobs],
        }
        return manifest

    def _persist_manifest(
        self,
        manifest_path: Path,
        manifest: dict[str, Any],
        jobs: list[CityCellJobSpec],
        final_business_csv: Path,
        final_reviews_csv: Path,
        *,
        summary_csv: Optional[str] = None,
    ) -> None:
        manifest["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        manifest["jobs"] = [asdict(job) for job in jobs]
        manifest["final_business_csv"] = str(final_business_csv)
        manifest["final_reviews_csv"] = str(final_reviews_csv)
        if summary_csv is not None:
            manifest["summary_csv"] = summary_csv
        manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")

    def _rebuild_merged_outputs(
        self,
        jobs: list[CityCellJobSpec],
        final_business_csv: Path,
        final_reviews_csv: Path,
    ) -> None:
        business_frames = []
        review_frames = []

        for job in jobs:
            if job.status != "completed":
                continue
            if job.result_file and Path(job.result_file).exists():
                business_frames.append(pd.read_csv(job.result_file).fillna(""))
            if job.reviews_file and Path(job.reviews_file).exists():
                review_frames.append(pd.read_csv(job.reviews_file).fillna(""))

        final_business_csv.parent.mkdir(parents=True, exist_ok=True)
        final_reviews_csv.parent.mkdir(parents=True, exist_ok=True)

        business_frame = _merge_business_frames(business_frames)
        review_frame = _merge_review_frames(review_frames)

        business_frame.to_csv(final_business_csv, index=False, encoding="utf-8-sig")
        review_frame.to_csv(final_reviews_csv, index=False, encoding="utf-8-sig")

        hash_index_path = final_reviews_csv.with_name(
            f"{final_reviews_csv.stem}_hash_index.sqlite"
        )
        if hash_index_path.exists():
            hash_index_path.unlink()
        ReviewHashIndex(str(final_reviews_csv))

    @staticmethod
    def _tail_text(path: Path, lines: int) -> str:
        if not path.exists():
            return ""
        content = path.read_text(encoding="utf-8", errors="replace").splitlines()
        return "\n".join(content[-lines:])


def _merge_business_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
    if not frames:
        return pd.DataFrame()

    merged = pd.concat(frames, ignore_index=True).fillna("")
    if "Place ID" in merged.columns:
        merged = merged.drop_duplicates(subset=["Place ID"], keep="first")
    elif {"Names", "Address"}.issubset(set(merged.columns)):
        merged = merged.drop_duplicates(subset=["Names", "Address"], keep="first")
    return merged


def _merge_review_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
    if not frames:
        return pd.DataFrame()

    merged = pd.concat(frames, ignore_index=True).fillna("")
    if "review_hash" in merged.columns:
        merged = merged.drop_duplicates(subset=["review_hash"], keep="first")
    elif {"place_id", "reviewer_name", "review_text"}.issubset(set(merged.columns)):
        merged = merged.drop_duplicates(
            subset=["place_id", "reviewer_name", "review_text"],
            keep="first",
        )
    return merged


def run_city_cell_workers(options: CityCellWorkerOptions) -> CityCellWorkerResult:
    """Convenience wrapper for running bounded parallel city cell workers."""

    runner = CityCellWorkerRunner()
    return runner.run(options)
