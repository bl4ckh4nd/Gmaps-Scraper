"""Text filtering helpers for owner enrichment."""

from __future__ import annotations

import re
from typing import Iterable

from ..models import OwnerDocument

KEYWORDS = (
    "owner",
    "inhaber",
    "inhaberin",
    "geschaeftsfuehrer",
    "gesellschafter",
    "ceo",
    "founder",
    "managing director",
)


def extract_owner_snippets(
    documents: Iterable[OwnerDocument],
    *,
    max_chars: int = 4000,
) -> str:
    snippet, _ = extract_owner_snippets_with_sources(documents, max_chars=max_chars)
    return snippet


def extract_owner_snippets_with_sources(
    documents: Iterable[OwnerDocument],
    *,
    max_chars: int = 4000,
) -> tuple[str, list[OwnerDocument]]:
    """Reduce crawled documents to the most relevant owner-related snippets."""

    snippets: list[str] = []
    evidence_documents: list[OwnerDocument] = []
    for document in documents:
        content = (document.content or "").strip()
        if not content:
            continue
        lines = [line.strip() for line in content.splitlines() if line.strip()]
        relevant_lines = [
            line for line in lines if any(keyword in line.lower() for keyword in KEYWORDS)
        ]
        sample = "\n".join(relevant_lines[:10]) if relevant_lines else "\n".join(lines[:5])
        if sample:
            header = document.title or document.url
            snippets.append(f"Source: {header}\n{sample}")
            evidence_documents.append(document)
        combined = "\n\n".join(snippets)
        if len(combined) >= max_chars:
            break

    combined = "\n\n".join(snippets)[:max_chars]
    return normalize_whitespace(combined), evidence_documents


def normalize_whitespace(text: str) -> str:
    """Collapse repeated whitespace for cleaner prompts."""

    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()
