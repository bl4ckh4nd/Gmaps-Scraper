"""Service orchestrating adaptive crawl + LLM owner extraction."""

from __future__ import annotations

import logging
from typing import Callable, Optional, Sequence, TYPE_CHECKING

from ..config import OwnerEnrichmentSettings
from ..models import Business, OwnerDetails, OwnerDocument
from .openrouter_client import (
    OpenRouterClient,
    OpenRouterClientError,
    extract_owner_name_from_response,
)
from .text_filters import extract_owner_snippets_with_sources

if TYPE_CHECKING:
    from ..scraper.adaptive_owner_enricher import AdaptiveOwnerEnricher


class OwnerEnrichmentService:
    """Coordinates owner enrichment pipeline for a business."""

    def __init__(
        self,
        settings: OwnerEnrichmentSettings,
        *,
        logger: Optional[logging.Logger] = None,
        enricher_factory: Optional[Callable[[OwnerEnrichmentSettings], "AdaptiveOwnerEnricher"]] = None,
    ) -> None:
        self.settings = settings
        self.logger = logger or logging.getLogger(__name__)
        self._enricher_factory = enricher_factory or self._default_enricher_factory
        self._adaptive_enricher: Optional["AdaptiveOwnerEnricher"] = None
        self._openrouter_client: Optional[OpenRouterClient] = None

    def is_enabled(self) -> bool:
        return bool(self.settings.enabled)

    def enrich_business(self, business: Business) -> OwnerDetails:
        if not self.is_enabled():
            return OwnerDetails(status="disabled", reason="feature_disabled")

        if not business.website:
            return OwnerDetails(status="no_website", reason="website_missing")

        try:
            enricher = self._get_enricher()
        except Exception as exc:
            self.logger.error("Owner crawl failed for %s: %s", business.website, exc)
            return OwnerDetails.from_response(
                None,
                status="crawler_unavailable",
                reason=str(exc),
            )

        try:
            crawl_result = enricher.crawl_owner_content_sync(business.website)
        except Exception as exc:
            self.logger.error("Owner crawl failed for %s: %s", business.website, exc)
            return OwnerDetails.from_response(
                None,
                status="crawl_failed",
                reason=str(exc),
            )

        if crawl_result.status == "crawl_failed":
            return OwnerDetails.from_response(
                None,
                status="crawl_failed",
                reason=crawl_result.error or "crawl_failed",
                debug_payload=crawl_result.to_dict(),
            )

        if not crawl_result.documents:
            return OwnerDetails.from_response(
                None,
                status="no_documents",
                reason="no_owner_documents",
                debug_payload=crawl_result.to_dict(),
            )

        text_snippet, evidence_documents = extract_owner_snippets_with_sources(crawl_result.documents)
        if not text_snippet:
            return OwnerDetails.from_response(
                None,
                status="no_content",
                reason="no_relevant_content",
                debug_payload=crawl_result.to_dict(),
            )

        client = self._get_openrouter_client()
        if client is None:
            return OwnerDetails.from_response(
                None,
                status="llm_unavailable",
                reason="openrouter_client_uninitialised",
                debug_payload=crawl_result.to_dict(),
            )

        conversation = self._build_messages(text_snippet)
        response = None
        error_reason: Optional[str] = None
        for attempt in range(max(1, self.settings.max_llm_retries)):
            try:
                response = client.create_chat_completion_sync(
                    conversation,
                    response_format=self.settings.llm_response_format,
                )
                break
            except OpenRouterClientError as exc:
                error_reason = str(exc)
                self.logger.warning(
                    "OpenRouter attempt %s failed for %s: %s",
                    attempt + 1,
                    business.website,
                    exc,
                )
        else:
            return OwnerDetails.from_response(
                None,
                status="llm_failed",
                reason=error_reason or "openrouter_error",
                debug_payload=crawl_result.to_dict(),
            )

        owner_name = extract_owner_name_from_response(response)
        if not owner_name:
            return OwnerDetails.from_response(
                None,
                status="owner_not_found",
                reason="llm_no_owner",
                debug_payload={
                    "crawl": crawl_result.to_dict(),
                    "llm_response": response,
                },
            )

        best_document = self._select_source_document(
            owner_name.strip(),
            evidence_documents=evidence_documents,
            fallback_documents=crawl_result.documents,
        )

        return OwnerDetails.from_response(
            owner_name.strip(),
            status="owner_found",
            confidence=best_document.confidence if best_document else None,
            source_url=best_document.url if best_document else None,
            llm_model=client.default_model,
            debug_payload={
                "llm_response": response if self.settings.log_prompts else {},
                "crawl_status": crawl_result.status,
            },
        )

    def _get_openrouter_client(self) -> Optional[OpenRouterClient]:
        if self._openrouter_client:
            return self._openrouter_client
        try:
            client = OpenRouterClient.from_env(
                api_key_env=self.settings.openrouter_api_key_env,
                default_model=self.settings.openrouter_default_model,
                allow_missing=False,
                timeout=self.settings.request_timeout_ms / 1000.0,
            )
        except OpenRouterClientError as exc:
            self.logger.error("Failed to initialise OpenRouter client: %s", exc)
            return None

        self._openrouter_client = client
        return client

    def _build_messages(self, content_snippet: str) -> list[dict[str, str]]:
        system_prompt = (
            "You are a data extraction assistant. Your task is to read provided website content and return the "
            "full name of the legal owner or managing director responsible for a business. Output must be JSON "
            "with a single key 'owner_name'. If the information is not present, use null. Do not include any "
            "additional keys or explanatory text."
        )

        user_prompt = (
            "Follow these steps:\n"
            "1. Read the excerpts carefully.\n"
            "2. Identify the most specific individual legally responsible for the business (owner, managing director, "
            "Geschäftsführer, etc.).\n"
            "3. If multiple names appear, choose the one most clearly labelled as the owner/manager.\n"
            "4. Return only JSON matching {\"owner_name\": ""<string or null>""}.\n"
            "5. Use null if the owner is not explicitly stated.\n\n"
            f"Website excerpts:\n{content_snippet}"
        )
        return [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]

    def _get_enricher(self) -> "AdaptiveOwnerEnricher":
        if self._adaptive_enricher:
            return self._adaptive_enricher

        try:
            self._adaptive_enricher = self._enricher_factory(self.settings)
        except Exception as exc:
            raise RuntimeError(str(exc)) from exc

        return self._adaptive_enricher

    @staticmethod
    def _default_enricher_factory(settings: OwnerEnrichmentSettings) -> "AdaptiveOwnerEnricher":
        from ..scraper.adaptive_owner_enricher import AdaptiveOwnerEnricher

        return AdaptiveOwnerEnricher(settings)

    def _select_source_document(
        self,
        owner_name: str,
        *,
        evidence_documents: Sequence[OwnerDocument],
        fallback_documents: Sequence[OwnerDocument],
    ) -> Optional[OwnerDocument]:
        owner_name_lower = owner_name.lower()

        matching_evidence = [
            doc for doc in evidence_documents
            if doc.content and owner_name_lower in doc.content.lower()
        ]
        if matching_evidence:
            return max(matching_evidence, key=lambda d: d.confidence or 0.0)

        evidence_with_content = [doc for doc in evidence_documents if doc.content]
        if evidence_with_content:
            return max(evidence_with_content, key=lambda d: d.confidence or 0.0)

        fallback_with_content = [doc for doc in fallback_documents if doc.content]
        if fallback_with_content:
            return max(fallback_with_content, key=lambda d: d.confidence or 0.0)

        return fallback_documents[0] if fallback_documents else None


def enrich_business_owner(
    business: Business,
    service: OwnerEnrichmentService,
) -> OwnerDetails:
    """Functional helper to enrich owner details for a business."""

    return service.enrich_business(business)
