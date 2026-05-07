"""Review analysis utilities for calculating business metrics."""

from __future__ import annotations

from datetime import datetime, timedelta
import re
from typing import Any, Dict, List, Optional, Tuple

from ..models.review import Review

ABSOLUTE_DATE_FORMATS = ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y", "%m/%d/%Y")


def parse_review_date(text: str, reference_date: Optional[datetime] = None) -> datetime:
    """
    Convert relative review dates (German/English) into actual datetimes.

    Falls back to the reference date when the string is not machine-readable.
    """
    reference_date = reference_date or datetime.now()
    parsed = _parse_review_date_exact(text, reference_date)
    return parsed or reference_date


def inspect_review_date(
    text: str,
    reference_date: Optional[datetime] = None,
    window_days: int = 365,
) -> Dict[str, Any]:
    """Classify a Google Maps review date for rolling-window analytics."""

    reference_date = reference_date or datetime.now()
    cutoff_date = reference_date - timedelta(days=window_days)
    cleaned = _normalize_review_date_text(text)

    payload: Dict[str, Any] = {
        "raw_text": text or "",
        "cleaned_text": cleaned,
        "parsed_date": None,
        "bucket": "unknown",
        "definitely_within_window": False,
        "definitely_outside_window": False,
        "ambiguous_one_year_bucket": False,
        "older_than_one_year_bucket": False,
        "is_unknown": not bool(cleaned),
    }

    if not cleaned:
        return payload

    if cleaned in {"heute", "today"}:
        payload.update(
            bucket="day",
            parsed_date=reference_date,
            definitely_within_window=True,
            is_unknown=False,
        )
        return payload

    if cleaned in {"gestern", "yesterday", "vor einem tag", "vor 1 tag", "a day ago", "1 day ago"}:
        parsed_date = reference_date - timedelta(days=1)
        payload.update(
            bucket="day",
            parsed_date=parsed_date,
            definitely_within_window=True,
            is_unknown=False,
        )
        return payload

    if cleaned in {"vor einer woche", "vor 1 woche", "a week ago", "1 week ago"}:
        parsed_date = reference_date - timedelta(weeks=1)
        payload.update(
            bucket="week",
            parsed_date=parsed_date,
            definitely_within_window=True,
            is_unknown=False,
        )
        return payload

    if cleaned in {"vor einem monat", "vor 1 monat", "a month ago", "1 month ago"}:
        parsed_date = reference_date - timedelta(days=30)
        payload.update(
            bucket="month",
            parsed_date=parsed_date,
            definitely_within_window=True,
            is_unknown=False,
        )
        return payload

    if cleaned in {"vor einem jahr", "vor 1 jahr", "a year ago", "1 year ago"}:
        payload.update(
            bucket="one_year",
            parsed_date=reference_date - timedelta(days=365),
            ambiguous_one_year_bucket=True,
            is_unknown=False,
        )
        return payload

    german_match = re.match(
        r"vor (\d+) (tag|tagen|woche|wochen|monat|monaten|jahr|jahren)",
        cleaned,
    )
    if german_match:
        quantity = int(german_match.group(1))
        unit = german_match.group(2)
        return _build_relative_payload(quantity, unit, reference_date, cutoff_date, payload)

    english_match = re.match(
        r"(\d+) (day|days|week|weeks|month|months|year|years) ago",
        cleaned,
    )
    if english_match:
        quantity = int(english_match.group(1))
        unit = english_match.group(2)
        return _build_relative_payload(quantity, unit, reference_date, cutoff_date, payload)

    parsed_date = _parse_absolute_date(cleaned)
    if parsed_date:
        payload["parsed_date"] = parsed_date
        payload["bucket"] = "absolute"
        payload["definitely_within_window"] = parsed_date >= cutoff_date
        payload["definitely_outside_window"] = parsed_date < cutoff_date
        payload["older_than_one_year_bucket"] = parsed_date < (cutoff_date - timedelta(days=31))
        payload["is_unknown"] = False
        return payload

    return payload


def calculate_reply_rates(reviews: List[Review]) -> Tuple[float, float]:
    """Calculate reply rates for good and bad reviews."""
    if not reviews:
        return 0.0, 0.0

    good_reviews = [r for r in reviews if r.rating >= 4]
    bad_reviews = [r for r in reviews if r.rating <= 2]

    good_reply_rate = 0.0
    bad_reply_rate = 0.0

    if good_reviews:
        good_replies = sum(1 for r in good_reviews if r.owner_response and r.owner_response.strip())
        good_reply_rate = (good_replies / len(good_reviews)) * 100

    if bad_reviews:
        bad_replies = sum(1 for r in bad_reviews if r.owner_response and r.owner_response.strip())
        bad_reply_rate = (bad_replies / len(bad_reviews)) * 100

    return good_reply_rate, bad_reply_rate


def calculate_avg_time_between_reviews(reviews: List[Review], months: int = 12) -> Optional[float]:
    """Calculate average time between reviews in the last N months."""
    if len(reviews) < 2:
        return None

    reference_date = datetime.now()
    review_dates = []

    for review in reviews:
        if hasattr(review, 'parsed_date') and review.parsed_date:
            review_dates.append(review.parsed_date)
        elif review.review_date:
            parsed_date = _parse_review_date_exact(review.review_date, reference_date)
            if parsed_date:
                review_dates.append(parsed_date)

    if len(review_dates) < 2:
        return None

    cutoff_date = reference_date - timedelta(days=months * 30)
    recent_dates = [date for date in review_dates if date > cutoff_date]

    if len(recent_dates) < 2:
        return None

    recent_dates.sort()
    time_diffs = []

    for i in range(1, len(recent_dates)):
        diff = recent_dates[i] - recent_dates[i - 1]
        time_diffs.append(diff.days)

    if not time_diffs:
        return None

    return sum(time_diffs) / len(time_diffs)


def calculate_recent_review_window(
    reviews: List[Review],
    *,
    window_days: int = 365,
    reference_date: Optional[datetime] = None,
    collection_metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Calculate min/max recent visible-review counts for the rolling window."""

    reference_date = reference_date or datetime.now()
    definite_recent = 0
    ambiguous_one_year = 0

    for review in reviews:
        classification = inspect_review_date(review.review_date, reference_date, window_days)
        review.window_bucket = classification["bucket"]
        review.parsed_date = classification["parsed_date"]

        if classification["definitely_within_window"]:
            definite_recent += 1
        elif classification["ambiguous_one_year_bucket"]:
            ambiguous_one_year += 1

    min_count = definite_recent
    max_count = definite_recent + ambiguous_one_year
    mid_count = round((min_count + max_count) / 2, 1)

    metadata = collection_metadata or {}
    coverage_status = metadata.get("coverage_status")
    if not coverage_status:
        if not reviews:
            coverage_status = "not_requested"
        else:
            coverage_status = "estimated" if ambiguous_one_year else "exact"

    return {
        "reviews_last_365d_min": min_count,
        "reviews_last_365d_max": max_count,
        "reviews_last_365d_mid": mid_count,
        "ambiguous_one_year_reviews": ambiguous_one_year,
        "review_window_coverage_status": coverage_status,
        "review_window_cutoff_observed": metadata.get("oldest_review_date_text", ""),
    }


def calculate_deleted_review_rates(
    visible_recent_min: int,
    visible_recent_max: int,
    deleted_review_min: Optional[int],
    deleted_review_max: Optional[int],
) -> Dict[str, float]:
    """Calculate deleted-review percentages against total received reviews."""

    deleted_min = max(0, deleted_review_min or 0)
    deleted_max = max(deleted_min, deleted_review_max or deleted_min)
    deleted_mid = (deleted_min + deleted_max) / 2
    visible_mid = (visible_recent_min + visible_recent_max) / 2

    min_rate = _safe_deleted_rate(deleted_min, visible_recent_max)
    max_rate = _safe_deleted_rate(deleted_max, visible_recent_min)
    mid_rate = _safe_deleted_rate(deleted_mid, visible_mid)

    return {
        "deleted_review_rate_min_pct": round(min_rate, 2),
        "deleted_review_rate_max_pct": round(max_rate, 2),
        "deleted_review_rate_mid_pct": round(mid_rate, 2),
    }


def analyze_reviews(
    reviews: List[Review],
    *,
    collection_metadata: Optional[Dict[str, Any]] = None,
    deleted_review_bounds: Optional[Dict[str, Optional[int]]] = None,
    review_window_days: int = 365,
) -> Dict[str, Any]:
    """Analyze reviews and return comprehensive metrics."""

    if not reviews:
        base_window = calculate_recent_review_window(
            [],
            window_days=review_window_days,
            collection_metadata=collection_metadata,
        )
        deleted_rates = calculate_deleted_review_rates(
            base_window["reviews_last_365d_min"],
            base_window["reviews_last_365d_max"],
            (deleted_review_bounds or {}).get("min"),
            (deleted_review_bounds or {}).get("max"),
        )
        return {
            'reply_rate_good': 0.0,
            'reply_rate_bad': 0.0,
            'avg_time_between_reviews': None,
            'total_reviews': 0,
            'good_reviews': 0,
            'bad_reviews': 0,
            'neutral_reviews': 0,
            **base_window,
            **deleted_rates,
        }

    reference_date = datetime.now()
    for review in reviews:
        if not hasattr(review, 'parsed_date') or not review.parsed_date:
            review.parsed_date = parse_review_date(review.review_date or "", reference_date)

    reply_rate_good, reply_rate_bad = calculate_reply_rates(reviews)
    avg_time = calculate_avg_time_between_reviews(reviews)
    good_reviews = len([r for r in reviews if r.rating >= 4])
    bad_reviews = len([r for r in reviews if r.rating <= 2])
    neutral_reviews = len([r for r in reviews if 2 < r.rating < 4])

    recent_window = calculate_recent_review_window(
        reviews,
        window_days=review_window_days,
        reference_date=reference_date,
        collection_metadata=collection_metadata,
    )
    deleted_rates = calculate_deleted_review_rates(
        recent_window["reviews_last_365d_min"],
        recent_window["reviews_last_365d_max"],
        (deleted_review_bounds or {}).get("min"),
        (deleted_review_bounds or {}).get("max"),
    )

    return {
        'reply_rate_good': round(reply_rate_good, 1),
        'reply_rate_bad': round(reply_rate_bad, 1),
        'avg_time_between_reviews': round(avg_time, 1) if avg_time else None,
        'total_reviews': len(reviews),
        'good_reviews': good_reviews,
        'bad_reviews': bad_reviews,
        'neutral_reviews': neutral_reviews,
        **recent_window,
        **deleted_rates,
    }


def get_review_summary_stats(reviews: List[Review]) -> Dict[str, Any]:
    """Get summary statistics for reviews."""
    if not reviews:
        return {
            'has_reviews': False,
            'recent_activity': False,
            'responsive_to_complaints': False,
            'consistent_quality': None,
        }

    analysis = analyze_reviews(reviews)
    recent_cutoff = datetime.now() - timedelta(days=90)
    recent_reviews = []

    for review in reviews:
        review_date = getattr(review, 'parsed_date', None)
        if review_date and review_date >= recent_cutoff:
            recent_reviews.append(review)

    return {
        'has_reviews': len(reviews) > 0,
        'recent_activity': len(recent_reviews) > 0,
        'responsive_to_complaints': analysis['reply_rate_bad'] > 50,
        'consistent_quality': analysis['avg_time_between_reviews'] is not None and analysis['avg_time_between_reviews'] < 30,
        'total_recent_reviews': len(recent_reviews),
    }


def _build_relative_payload(
    quantity: int,
    unit: str,
    reference_date: datetime,
    cutoff_date: datetime,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    normalized_unit = unit.lower()

    if "tag" in normalized_unit or "day" in normalized_unit:
        parsed_date = reference_date - timedelta(days=quantity)
        payload.update(
            bucket="day",
            parsed_date=parsed_date,
            definitely_within_window=True,
            is_unknown=False,
        )
        return payload

    if "woche" in normalized_unit or "week" in normalized_unit:
        parsed_date = reference_date - timedelta(weeks=quantity)
        payload.update(
            bucket="week",
            parsed_date=parsed_date,
            definitely_within_window=True,
            is_unknown=False,
        )
        return payload

    if "monat" in normalized_unit or "month" in normalized_unit:
        parsed_date = reference_date - timedelta(days=quantity * 30)
        payload["parsed_date"] = parsed_date
        payload["is_unknown"] = False
        if quantity < 12:
            payload.update(bucket="month", definitely_within_window=True)
        elif quantity == 12:
            payload.update(bucket="one_year", ambiguous_one_year_bucket=True)
        else:
            payload.update(
                bucket="month",
                definitely_outside_window=True,
                older_than_one_year_bucket=True,
            )
        return payload

    if "jahr" in normalized_unit or "year" in normalized_unit:
        parsed_date = reference_date - timedelta(days=quantity * 365)
        payload["parsed_date"] = parsed_date
        payload["is_unknown"] = False
        if quantity == 1:
            payload.update(bucket="one_year", ambiguous_one_year_bucket=True)
        else:
            payload.update(
                bucket="multi_year",
                definitely_outside_window=True,
                older_than_one_year_bucket=True,
            )
        return payload

    payload["definitely_outside_window"] = False
    payload["older_than_one_year_bucket"] = False
    payload["is_unknown"] = True
    return payload


def _parse_review_date_exact(text: str, reference_date: datetime) -> Optional[datetime]:
    cleaned = _normalize_review_date_text(text)
    if not cleaned or cleaned in {"vor einem jahr", "vor 1 jahr", "a year ago", "1 year ago"}:
        return None

    if cleaned in {"heute", "today"}:
        return reference_date
    if cleaned in {"gestern", "yesterday", "vor einem tag", "vor 1 tag", "a day ago", "1 day ago"}:
        return reference_date - timedelta(days=1)
    if cleaned in {"vor einer woche", "vor 1 woche", "a week ago", "1 week ago"}:
        return reference_date - timedelta(weeks=1)
    if cleaned in {"vor einem monat", "vor 1 monat", "a month ago", "1 month ago"}:
        return reference_date - timedelta(days=30)

    german_match = re.match(r"vor (\d+) (tag|tagen|woche|wochen|monat|monaten)", cleaned)
    if german_match:
        quantity = int(german_match.group(1))
        unit = german_match.group(2)
        if "tag" in unit:
            return reference_date - timedelta(days=quantity)
        if "woche" in unit:
            return reference_date - timedelta(weeks=quantity)
        if "monat" in unit:
            return reference_date - timedelta(days=quantity * 30)

    english_match = re.match(r"(\d+) (day|days|week|weeks|month|months) ago", cleaned)
    if english_match:
        quantity = int(english_match.group(1))
        unit = english_match.group(2)
        if "day" in unit:
            return reference_date - timedelta(days=quantity)
        if "week" in unit:
            return reference_date - timedelta(weeks=quantity)
        if "month" in unit:
            return reference_date - timedelta(days=quantity * 30)

    return _parse_absolute_date(cleaned)


def _parse_absolute_date(text: str) -> Optional[datetime]:
    for date_format in ABSOLUTE_DATE_FORMATS:
        try:
            return datetime.strptime(text, date_format)
        except ValueError:
            continue
    return None


def _normalize_review_date_text(text: str) -> str:
    cleaned = (text or "").strip().lower()
    for prefix in ("bearbeitet:", "edited:"):
        if cleaned.startswith(prefix):
            cleaned = cleaned.replace(prefix, "", 1).strip()
    return cleaned


def _safe_deleted_rate(deleted_count: float, visible_count: float) -> float:
    total_received = visible_count + deleted_count
    if total_received <= 0:
        return 0.0
    return (deleted_count / total_received) * 100
