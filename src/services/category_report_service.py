"""Aggregate category-oriented analytics from business CSV output."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pandas as pd


class CategoryReportService:
    """Build per-query/per-place-type analytics summaries from business output."""

    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        self.logger = logger or logging.getLogger(__name__)

    def build_summary(self, business_csv_path: str, output_path: Optional[str] = None) -> Optional[str]:
        source_path = Path(business_csv_path)
        if not source_path.exists():
            return None

        df = pd.read_csv(source_path).fillna("")
        if df.empty:
            return None

        df["Search Query"] = df.get("Search Query", "").replace("", "(unspecified)")
        df["Type"] = df.get("Type", "").replace("", "(unknown)")
        df["Review Window Coverage Status"] = df.get(
            "Review Window Coverage Status",
            "",
        ).replace("", "not_requested")

        numeric_columns = [
            "Reviews Last 365d Min",
            "Reviews Last 365d Max",
            "Reviews Last 365d Mid",
            "Deleted Review Rate Min (%)",
            "Deleted Review Rate Max (%)",
            "Deleted Review Rate Mid (%)",
        ]
        for column in numeric_columns:
            df[column] = pd.to_numeric(df.get(column, 0), errors="coerce").fillna(0)

        grouped = df.groupby(["Search Query", "Type"], dropna=False)
        summary = grouped.agg(
            business_count=("Place ID", "count"),
            avg_reviews_last_365d_min=("Reviews Last 365d Min", "mean"),
            avg_reviews_last_365d_mid=("Reviews Last 365d Mid", "mean"),
            avg_reviews_last_365d_max=("Reviews Last 365d Max", "mean"),
            median_reviews_last_365d_mid=("Reviews Last 365d Mid", "median"),
            avg_deleted_rate_min_pct=("Deleted Review Rate Min (%)", "mean"),
            avg_deleted_rate_mid_pct=("Deleted Review Rate Mid (%)", "mean"),
            avg_deleted_rate_max_pct=("Deleted Review Rate Max (%)", "mean"),
            median_deleted_rate_mid_pct=("Deleted Review Rate Mid (%)", "median"),
        ).reset_index()

        coverage_counts = (
            df.groupby(["Search Query", "Type", "Review Window Coverage Status"])
            .size()
            .unstack(fill_value=0)
            .reset_index()
        )
        summary = summary.merge(coverage_counts, on=["Search Query", "Type"], how="left")
        summary = summary.fillna(0)

        output = Path(output_path) if output_path else source_path.with_name(
            f"{source_path.stem}_category_summary{source_path.suffix}"
        )
        summary.to_csv(output, index=False)
        self.logger.info("Wrote category analytics summary to %s", output)
        return str(output.resolve())
