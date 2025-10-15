"""Data models supporting owner enrichment workflows."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, List, Optional


@dataclass
class OwnerDocument:
    """Document retrieved during adaptive crawling that may contain owner data."""

    url: str
    title: Optional[str] = None
    content: str = ""
    confidence: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "url": self.url,
            "title": self.title,
            "content": self.content,
            "confidence": self.confidence,
            "metadata": self.metadata,
        }


@dataclass
class OwnerCrawlResult:
    """Outcome of an adaptive crawl for owner-related content."""

    status: str
    documents: List[OwnerDocument] = field(default_factory=list)
    crawl_metadata: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "documents": [doc.to_dict() for doc in self.documents],
            "crawl_metadata": self.crawl_metadata,
            "error": self.error,
        }


@dataclass
class OwnerDetails:
    """Normalized owner information extracted via LLM."""

    owner_name: Optional[str] = None
    status: str = "not_requested"
    confidence: Optional[float] = None
    source_url: Optional[str] = None
    last_checked: Optional[datetime] = None
    llm_model: Optional[str] = None
    reason: Optional[str] = None
    debug_payload: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "owner_name": self.owner_name,
            "status": self.status,
            "confidence": self.confidence,
            "source_url": self.source_url,
            "last_checked": self.last_checked.isoformat() if self.last_checked else None,
            "llm_model": self.llm_model,
            "reason": self.reason,
            "debug_payload": self.debug_payload,
        }

    @classmethod
    def from_response(
        cls,
        owner_name: Optional[str],
        *,
        status: str,
        confidence: Optional[float] = None,
        source_url: Optional[str] = None,
        llm_model: Optional[str] = None,
        reason: Optional[str] = None,
        debug_payload: Optional[Dict[str, Any]] = None,
    ) -> "OwnerDetails":
        return cls(
            owner_name=owner_name,
            status=status,
            confidence=confidence,
            source_url=source_url,
            last_checked=datetime.utcnow(),
            llm_model=llm_model,
            reason=reason,
            debug_payload=debug_payload or {},
        )
