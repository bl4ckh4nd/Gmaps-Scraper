"""Deterministic homepage checks for website quality/modernity signals."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Optional

import httpx


STALE_OR_BROKEN_PATTERNS = (
    r"under construction",
    r"coming soon",
    r"domain (?:for sale|parking)",
    r"account suspended",
    r"index of /",
    r"403 forbidden",
    r"404 not found",
    r"page not found",
)


@dataclass
class WebsiteQualityAssessment:
    status: str = ""
    modernity_score: Optional[int] = None
    reason: str = ""
    uses_https: Optional[bool] = None
    mobile_friendly_hint: Optional[bool] = None
    structured_data_hint: Optional[bool] = None
    stale_or_broken_hint: Optional[bool] = None


def assess_website_quality(url: Optional[str], timeout_seconds: float = 10.0) -> WebsiteQualityAssessment:
    """Fetch a business homepage and derive simple website quality hints."""

    if not url:
        return WebsiteQualityAssessment(status="missing", reason="website_missing")

    try:
        with httpx.Client(
            follow_redirects=True,
            timeout=timeout_seconds,
            headers={"User-Agent": "Google-Maps-Scrapper/1.0"},
        ) as client:
            response = client.get(url)
    except httpx.TimeoutException:
        return WebsiteQualityAssessment(status="timeout", reason="request_timeout")
    except httpx.HTTPError as exc:
        return WebsiteQualityAssessment(status="unreachable", reason=exc.__class__.__name__.lower())

    final_url = str(response.url)
    uses_https = final_url.startswith("https://")
    content_type = (response.headers.get("content-type") or "").lower()
    html = response.text if "html" in content_type or "<html" in response.text[:500].lower() else ""
    normalized_html = html.lower()

    stale_or_broken_hint = bool(
        normalized_html
        and any(re.search(pattern, normalized_html) for pattern in STALE_OR_BROKEN_PATTERNS)
    )
    mobile_friendly_hint = bool(
        normalized_html
        and (
            '<meta name="viewport"' in normalized_html
            or "@media" in normalized_html
            or "width=device-width" in normalized_html
        )
    )
    structured_data_hint = bool(
        normalized_html
        and (
            "application/ld+json" in normalized_html
            or "schema.org" in normalized_html
            or "itemtype=" in normalized_html
        )
    )

    if response.status_code >= 500:
        status = "broken"
    elif response.status_code >= 400:
        status = "unreachable"
    elif not html:
        status = "reachable"
    else:
        status = "broken" if stale_or_broken_hint else "reachable"

    score = 0
    if status == "reachable":
        score += 30
    if uses_https:
        score += 25
    if mobile_friendly_hint:
        score += 25
    if structured_data_hint:
        score += 20
    if stale_or_broken_hint:
        score -= 30
    score = max(0, min(100, score))

    reasons = [status]
    reasons.append("https" if uses_https else "no_https")
    reasons.append("mobile_hint" if mobile_friendly_hint else "no_mobile_hint")
    reasons.append("structured_data" if structured_data_hint else "no_structured_data")
    if stale_or_broken_hint:
        reasons.append("stale_or_broken")

    return WebsiteQualityAssessment(
        status=status,
        modernity_score=score,
        reason=", ".join(reasons),
        uses_https=uses_https,
        mobile_friendly_hint=mobile_friendly_hint if html else None,
        structured_data_hint=structured_data_hint if html else None,
        stale_or_broken_hint=stale_or_broken_hint if html else None,
    )
