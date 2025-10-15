"""Minimal OpenRouter API client."""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, Dict, Iterable, List, Optional

import httpx

logger = logging.getLogger(__name__)


class OpenRouterClientError(Exception):
    """Generic OpenRouter client failure."""


class OpenRouterClient:
    """HTTP client for OpenRouter's OpenAI-compatible API."""

    DEFAULT_BASE_URL = "https://openrouter.ai/api/v1"

    def __init__(
        self,
        api_key: Optional[str] = None,
        *,
        default_model: Optional[str] = None,
        base_url: Optional[str] = None,
        referer: Optional[str] = None,
        title: Optional[str] = None,
        timeout: float = 30.0,
    ) -> None:
        self.api_key = api_key
        self.default_model = default_model
        self.base_url = (base_url or self.DEFAULT_BASE_URL).rstrip("/")
        self.referer = referer
        self.title = title
        self.timeout = timeout

    @classmethod
    def from_env(
        cls,
        *,
        api_key_env: str,
        default_model: Optional[str] = None,
        allow_missing: bool = False,
        **kwargs: Any,
    ) -> "OpenRouterClient":
        api_key = os.getenv(api_key_env)
        if not api_key and not allow_missing:
            raise OpenRouterClientError(
                f"OpenRouter API key not found in environment variable {api_key_env}"
            )
        return cls(api_key=api_key, default_model=default_model, **kwargs)

    async def create_chat_completion(
        self,
        messages: Iterable[Dict[str, str]],
        *,
        model: Optional[str] = None,
        response_format: Optional[str] = None,
        temperature: Optional[float] = None,
        max_output_tokens: Optional[int] = None,
        extra_params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "model": model or self.default_model,
            "messages": list(messages),
        }
        if not payload["model"]:
            raise OpenRouterClientError("Model must be specified for OpenRouter requests")

        if response_format:
            payload["response_format"] = {"type": response_format}
        if temperature is not None:
            payload["temperature"] = temperature
        if max_output_tokens is not None:
            payload["max_output_tokens"] = max_output_tokens
        if extra_params:
            payload.update(extra_params)

        headers = self._build_headers()
        url = f"{self.base_url}/chat/completions"

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            try:
                response = await client.post(url, json=payload, headers=headers)
            except httpx.HTTPError as exc:
                logger.exception("OpenRouter request failed: %s", exc)
                raise OpenRouterClientError(f"OpenRouter request failed: {exc}") from exc

        if response.status_code >= 400:
            logger.error("OpenRouter responded with %s: %s", response.status_code, response.text)
            raise OpenRouterClientError(
                f"OpenRouter error {response.status_code}: {response.text}"
            )

        return response.json()

    async def list_models(self) -> List[Dict[str, Any]]:
        headers = self._build_headers()
        url = f"{self.base_url}/models"
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            try:
                response = await client.get(url, headers=headers)
            except httpx.HTTPError as exc:
                logger.exception("Failed to list OpenRouter models: %s", exc)
                raise OpenRouterClientError(
                    f"Failed to list OpenRouter models: {exc}"
                ) from exc

        if response.status_code >= 400:
            raise OpenRouterClientError(
                f"Failed to list models ({response.status_code}): {response.text}"
            )

        data = response.json()
        if isinstance(data, dict):
            return data.get("data") or data.get("models") or []
        if isinstance(data, list):
            return data
        return []

    def _build_headers(self) -> Dict[str, str]:
        if not self.api_key:
            raise OpenRouterClientError("OpenRouter API key missing")

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        if self.referer:
            headers["HTTP-Referer"] = self.referer
        if self.title:
            headers["X-Title"] = self.title
        return headers

    def create_chat_completion_sync(
        self,
        messages: Iterable[Dict[str, str]],
        **kwargs: Any,
    ) -> Dict[str, Any]:
        return asyncio.run(self.create_chat_completion(messages, **kwargs))

    def list_models_sync(self) -> List[Dict[str, Any]]:
        return asyncio.run(self.list_models())


def extract_owner_name_from_response(response: Dict[str, Any]) -> Optional[str]:
    """Pull owner name from a structured OpenRouter response."""
    if not response:
        return None

    if "choices" in response and response["choices"]:
        message = response["choices"][0].get("message") or {}
        if isinstance(message, dict):
            if "parsed" in message:
                parsed = message["parsed"]
                if isinstance(parsed, dict):
                    return parsed.get("owner_name") or parsed.get("owner")
            content = message.get("content")
            if isinstance(content, str):
                return content.strip()
    return None


def filter_free_models(models: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return only models that advertise zero-cost usage."""

    free_models: List[Dict[str, Any]] = []
    for model in models:
        if not isinstance(model, dict):
            continue
        pricing = model.get("pricing") or {}
        prompt_cost = pricing.get("prompt") if isinstance(pricing, dict) else None
        completion_cost = pricing.get("completion") if isinstance(pricing, dict) else None
        if prompt_cost in (0, "0", "0.0") and completion_cost in (0, "0", "0.0"):
            free_models.append(model)
        elif model.get("id", "").endswith(":free"):
            free_models.append(model)
    return free_models
