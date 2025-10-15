"""Owner enrichment utility for existing CSV outputs."""

from __future__ import annotations

import csv
import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterable, Optional

from ..config import Config, OwnerEnrichmentSettings
from ..models import Business
from ..utils import OwnerEnrichmentService


OWNER_COLUMNS = [
    "Owner Name",
    "Owner Status",
    "Owner Confidence",
    "Owner Source URL",
    "Owner Last Checked",
    "Owner LLM Model",
    "Owner Reason",
]


@dataclass
class OwnerCSVEnrichmentOptions:
    """High-level options when enriching an existing CSV."""

    input_path: Path
    output_path: Optional[Path] = None
    in_place: bool = False
    resume: bool = False
    backup: bool = True
    owner_model: Optional[str] = None
    skip_existing: bool = True
    state_path: Optional[Path] = None


@dataclass
class OwnerCSVEnrichmentResult:
    """Summary of an enrichment run."""

    total_rows: int = 0
    processed_rows: int = 0
    owners_found: int = 0
    skipped_existing: int = 0
    failed_rows: int = 0
    output_path: Optional[Path] = None


class OwnerCSVEnricher:
    """Retrofit owner details into existing business CSV files."""

    def __init__(
        self,
        config: Config,
        *,
        owner_service_factory: Optional[Callable[[OwnerEnrichmentSettings], OwnerEnrichmentService]] = None,
    ) -> None:
        self.config = config
        self.owner_service_factory = owner_service_factory or (
            lambda settings: OwnerEnrichmentService(settings)
        )
        self._owner_service: Optional[OwnerEnrichmentService] = None

    def enrich(
        self,
        options: OwnerCSVEnrichmentOptions,
        *,
        progress_callback: Optional[Callable[[Dict[str, int]], None]] = None,
    ) -> OwnerCSVEnrichmentResult:
        """Enrich the given CSV with owner data."""

        input_path = options.input_path.resolve()
        if not input_path.exists():
            raise FileNotFoundError(f"Input CSV not found: {input_path}")

        output_path = self._determine_output_path(input_path, options)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        state_path = options.state_path or output_path.with_suffix(output_path.suffix + ".state.json")

        resume_state = self._load_resume_state(state_path) if options.resume else None
        processed_signatures = resume_state.get("processed", set()) if resume_state else set()

        # Apply owner model override if provided
        if options.owner_model:
            self.config.settings.owner_enrichment.openrouter_default_model = options.owner_model

        # Ensure enrichment is enabled for this run (without mutating persisted config)
        owner_settings = self.config.settings.owner_enrichment
        owner_settings.enabled = True

        owner_service = self._get_owner_service()

        # Determine whether we write header (new run) or append (resume)
        is_resume_append = options.resume and output_path.exists()

        tmp_output_path: Optional[Path] = None
        if options.in_place and not is_resume_append:
            tmp_output_path = output_path.with_suffix(output_path.suffix + ".tmp")
            if tmp_output_path.exists():
                tmp_output_path.unlink()
            writer_path = tmp_output_path
        else:
            writer_path = output_path

        result = OwnerCSVEnrichmentResult(output_path=output_path)

        # If resume with existing output, seed processed signatures from that file
        if is_resume_append:
            processed_signatures.update(self._load_signatures_from_csv(output_path))

        # Prepare writer
        header = list(self._business_header())
        write_header = not is_resume_append

        with input_path.open("r", encoding="utf-8", newline="") as src:
            reader = csv.DictReader(src)
            if reader.fieldnames is None:
                raise ValueError("Input CSV has no header row")

            # Normalise header to include owner columns
            for column in OWNER_COLUMNS:
                if column not in reader.fieldnames:
                    reader.fieldnames.append(column)

            # Ensure we start fresh when not resuming
            if not is_resume_append and writer_path.exists():
                writer_path.unlink()

            with writer_path.open("a", encoding="utf-8", newline="") as dst:
                writer = csv.DictWriter(dst, fieldnames=header, extrasaction="ignore")
                if write_header:
                    writer.writeheader()

                for row in reader:
                    result.total_rows += 1
                    signature = self._row_signature(row)

                    if signature in processed_signatures:
                        result.skipped_existing += 1
                        continue

                    try:
                        enriched_row, owner_found = self._enrich_row(row, owner_service, options.skip_existing)
                    except Exception as exc:  # pragma: no cover - defensive path
                        enriched_row = row.copy()
                        enriched_row["Owner Status"] = "error"
                        enriched_row["Owner Reason"] = str(exc)
                        owner_found = False
                        result.failed_rows += 1

                    writer.writerow(enriched_row)
                    processed_signatures.add(signature)
                    result.processed_rows += 1
                    if owner_found:
                        result.owners_found += 1
                    elif enriched_row.get("Owner Status") == "skip_existing":
                        result.skipped_existing += 1

                    if progress_callback:
                        progress_callback(
                            {
                                "total_rows": result.total_rows,
                                "processed_rows": result.processed_rows,
                                "owners_found": result.owners_found,
                            }
                        )

                    if options.resume:
                        self._save_resume_state(state_path, processed_signatures)

        # Finalise output when running in-place
        if tmp_output_path and tmp_output_path.exists():
            if options.backup:
                backup_path = output_path.with_suffix(output_path.suffix + ".bak")
                if not backup_path.exists():
                    shutil.copy2(output_path, backup_path)
            shutil.move(str(tmp_output_path), str(output_path))

        if options.resume and state_path.exists():
            state_path.unlink()

        return result

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _get_owner_service(self) -> OwnerEnrichmentService:
        if not self._owner_service:
            settings_copy = self.config.settings.owner_enrichment
            self._owner_service = self.owner_service_factory(settings_copy)
        return self._owner_service

    def _determine_output_path(
        self,
        input_path: Path,
        options: OwnerCSVEnrichmentOptions,
    ) -> Path:
        if options.output_path:
            return options.output_path.resolve()
        if options.in_place:
            return input_path.resolve()
        return input_path.with_name(f"{input_path.stem}_owner_enriched{input_path.suffix}")

    def _business_header(self) -> Iterable[str]:
        dummy_business = Business(place_id="", name="")
        return dummy_business.to_dict().keys()

    def _row_signature(self, row: Dict[str, str]) -> str:
        key_parts = [
            row.get("Place ID", ""),
            row.get("Names", ""),
            row.get("Address", ""),
            row.get("Website", ""),
        ]
        return "|".join(key_parts)

    def _enrich_row(
        self,
        row: Dict[str, str],
        owner_service: OwnerEnrichmentService,
        skip_existing: bool,
    ) -> tuple[Dict[str, str], bool]:
        """Return updated row plus flag whether an owner was found."""

        existing_owner = row.get("Owner Name")
        existing_status = row.get("Owner Status")

        if skip_existing and existing_owner:
            row.setdefault("Owner Status", "skip_existing")
            return row, False

        business = Business.from_dict(row)
        details = owner_service.enrich_business(business)
        business.owner_details = details
        # Ensure row reflects new details
        enriched = business.to_dict()
        return enriched, bool(details.owner_name)

    def _load_signatures_from_csv(self, path: Path) -> set[str]:
        signatures: set[str] = set()
        with path.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                signatures.add(self._row_signature(row))
        return signatures

    def _load_resume_state(self, state_path: Path) -> Optional[Dict[str, set[str]]]:
        if not state_path.exists():
            return None
        with state_path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        processed = set(data.get("processed", []))
        return {"processed": processed}

    def _save_resume_state(self, state_path: Path, processed: set[str]) -> None:
        state_path.write_text(json.dumps({"processed": list(processed)}), encoding="utf-8")
