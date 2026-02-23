"""Adaptive owner enrichment via local Crawl4AI pipelines."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Iterable, Optional, Sequence

try:
    # Crawl4AI export layout changed after v0.6, so we probe both new and old paths.
    from crawl4ai import AdaptiveCrawler  # type: ignore[attr-defined]
except ImportError:  # pragma: no cover - fallback for older releases
    try:
        from crawl4ai.crawler import AdaptiveCrawler  # type: ignore[misc]
    except ImportError:  # pragma: no cover - handled at runtime for helpful error message
        AdaptiveCrawler = None  # type: ignore[misc]

from ..config import OwnerEnrichmentSettings
from ..models import OwnerCrawlResult, OwnerDocument

logger = logging.getLogger(__name__)


class AdaptiveOwnerEnricherError(Exception):
    """Raised when the adaptive owner enrichment flow fails."""


class AdaptiveOwnerEnricher:
    """Wrapper around the local Crawl4AI AdaptiveCrawler."""

    def __init__(
        self,
        settings: OwnerEnrichmentSettings,
        *,
        crawler_cls: Optional[type] = None,
    ) -> None:
        self.settings = settings
        if (settings.crawler_engine or "adaptive").lower() != "adaptive":
            raise AdaptiveOwnerEnricherError(
                f"Unsupported crawler engine '{settings.crawler_engine}'. Only 'adaptive' is available in local mode."
            )
        self._crawler_cls = crawler_cls or AdaptiveCrawler
        if self._crawler_cls is None:
            raise AdaptiveOwnerEnricherError(
                "Crawl4AI package not installed. Install with `pip install crawl4ai` and ensure "
                "Playwright browsers are available (run `crawl4ai install browser`)."
            )

    def crawl_owner_content_sync(self, website_url: Optional[str]) -> OwnerCrawlResult:
        """Synchronous helper that runs the adaptive crawl."""

        def _run_in_new_loop() -> OwnerCrawlResult:
            new_loop = asyncio.new_event_loop()
            try:
                try:
                    asyncio.set_event_loop(new_loop)
                    return new_loop.run_until_complete(self.crawl_owner_content(website_url))
                finally:
                    asyncio.set_event_loop(None)
            finally:
                new_loop.close()

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No running loop in this thread; prefer asyncio.run but guard against
            # "loop is running" edge cases (Playwright sync API).
            try:
                return asyncio.run(self.crawl_owner_content(website_url))
            except RuntimeError as exc:
                if "loop is running" in str(exc).lower():
                    return _run_in_new_loop()
                raise

        if loop.is_running():  # e.g. when invoked from a Playwright-managed loop
            return _run_in_new_loop()

        return loop.run_until_complete(self.crawl_owner_content(website_url))

    async def crawl_owner_content(self, website_url: Optional[str]) -> OwnerCrawlResult:
        if not website_url:
            return OwnerCrawlResult(status="no_website", documents=[], crawl_metadata={})

        crawler = self._create_crawler()
        try:
            crawl_response = await self._run_crawler(crawler, website_url)
        except Exception as exc:  # pragma: no cover - surfaced to callers
            logger.exception("Adaptive crawl failed for %s", website_url)
            raise AdaptiveOwnerEnricherError(f"Adaptive crawl failed: {exc}") from exc
        finally:
            self._dispose_crawler(crawler)

        documents = self._extract_documents(crawl_response)
        status = "documents_found" if documents else "no_documents"
        metadata = self._extract_metadata(crawl_response)
        return OwnerCrawlResult(status=status, documents=documents, crawl_metadata=metadata)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _create_crawler(self):
        crawler_cls = self._crawler_cls
        if crawler_cls is None:
            raise AdaptiveOwnerEnricherError("AdaptiveCrawler class unavailable")

        try:
            return crawler_cls()
        except Exception as exc:
            raise AdaptiveOwnerEnricherError(f"Failed to initialise AdaptiveCrawler: {exc}") from exc

    async def _run_crawler(self, crawler: Any, website_url: str) -> Any:
        """Invoke the crawler using best-effort duck typing across Crawl4AI versions."""

        options = {
            "seed_urls": [website_url],
            "queries": list(self.settings.query_terms),
            "max_depth": self.settings.max_depth,
            "max_pages": self.settings.max_pages,
            "confidence_threshold": self.settings.confidence_threshold,
            "saturation_threshold": self.settings.saturation_threshold,
        }

        # Preferred async API
        if hasattr(crawler, "crawl"):
            result = crawler.crawl(**options)
            if asyncio.iscoroutine(result):
                return await result
            return result

        if hasattr(crawler, "run"):
            result = crawler.run(**options)  # may be sync or async
            if asyncio.iscoroutine(result):
                return await result
            return result

        raise AdaptiveOwnerEnricherError(
            "Unsupported Crawl4AI AdaptiveCrawler interface; expected `crawl` or `run` method."
        )

    def _dispose_crawler(self, crawler: Any) -> None:
        """Clean up crawler resources if the implementation exposes a close method."""

        closeable = getattr(crawler, "close", None)
        if callable(closeable):
            try:
                closeable()
            except Exception:  # pragma: no cover - best effort cleanup
                logger.debug("Failed to close AdaptiveCrawler cleanly", exc_info=True)

    def _extract_documents(self, response: Any) -> Sequence[OwnerDocument]:
        documents: Iterable[Any]

        if isinstance(response, dict):
            documents = response.get("documents") or response.get("results") or []
        elif hasattr(response, "documents"):
            documents = getattr(response, "documents")
        elif hasattr(response, "results"):
            documents = getattr(response, "results")
        else:
            documents = []

        parsed: list[OwnerDocument] = []
        for raw_doc in documents:
            doc = self._coerce_document(raw_doc)
            if doc:
                parsed.append(doc)
        return parsed

    def _coerce_document(self, raw_doc: Any) -> Optional[OwnerDocument]:
        if isinstance(raw_doc, OwnerDocument):
            return raw_doc

        if isinstance(raw_doc, dict):
            url = raw_doc.get("url") or raw_doc.get("source_url")
            metadata = raw_doc.get("metadata") or {}
            if not url and isinstance(metadata, dict):
                url = metadata.get("source_url")
            if not url:
                return None
            content = raw_doc.get("content") or raw_doc.get("text") or ""
            title = raw_doc.get("title") or raw_doc.get("page_title")
            confidence = raw_doc.get("confidence") or raw_doc.get("score")
            return OwnerDocument(url=url, title=title, content=content, confidence=confidence, metadata=metadata)

        # Some Crawl4AI versions return lightweight objects with attributes.
        url = getattr(raw_doc, "url", None) or getattr(raw_doc, "source_url", None)
        if not url:
            url = getattr(getattr(raw_doc, "metadata", None), "get", lambda *_: None)("source_url")
        if not url:
            return None
        content = getattr(raw_doc, "content", "") or getattr(raw_doc, "text", "")
        title = getattr(raw_doc, "title", None) or getattr(raw_doc, "page_title", None)
        confidence = getattr(raw_doc, "confidence", None) or getattr(raw_doc, "score", None)
        metadata = getattr(raw_doc, "metadata", {}) or {}
        return OwnerDocument(url=url, title=title, content=content, confidence=confidence, metadata=metadata)

    def _extract_metadata(self, response: Any) -> Dict[str, Any]:
        if isinstance(response, dict):
            return response.get("metadata", {}) or {}
        metadata = getattr(response, "metadata", None)
        if isinstance(metadata, dict):
            return metadata
        return {}
