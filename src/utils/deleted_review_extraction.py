"""Helpers for extracting and normalizing deleted-review notices."""

from __future__ import annotations

from dataclasses import dataclass
import json
import logging
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from .openrouter_client import OpenRouterClient, OpenRouterClientError

logger = logging.getLogger(__name__)


@dataclass
class DeletedReviewNotice:
    """Normalized deleted-review notice extracted from Google Maps."""

    min_count: Optional[int] = None
    max_count: Optional[int] = None
    raw_text: str = ""
    source: str = "deterministic"
    confidence: Optional[float] = None

    @property
    def exact_count(self) -> Optional[int]:
        if self.min_count is not None and self.min_count == self.max_count:
            return self.min_count
        return None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "min_count": self.min_count,
            "max_count": self.max_count,
            "raw_text": self.raw_text,
            "source": self.source,
            "confidence": self.confidence,
        }


@dataclass
class DeletedReviewInput:
    """Payload used for batch LLM normalization."""

    place_id: str
    business_name: str
    notice_text: str
    maps_url: str = ""
    raw_context: str = ""

    def to_prompt_payload(self) -> Dict[str, str]:
        return {
            "place_id": self.place_id,
            "business_name": self.business_name,
            "notice_text": self.notice_text,
            "maps_url": self.maps_url,
            "raw_context": self.raw_context,
        }


GERMAN_NUMBER_WORDS = {
    "null": 0,
    "ein": 1,
    "eins": 1,
    "eine": 1,
    "einer": 1,
    "einen": 1,
    "zwei": 2,
    "drei": 3,
    "vier": 4,
    "fuenf": 5,
    "fünf": 5,
    "sechs": 6,
    "sieben": 7,
    "acht": 8,
    "neun": 9,
    "zehn": 10,
    "elf": 11,
    "zwoelf": 12,
    "zwölf": 12,
    "dreizehn": 13,
    "vierzehn": 14,
    "fuenfzehn": 15,
    "fünfzehn": 15,
    "sechzehn": 16,
    "siebzehn": 17,
    "achtzehn": 18,
    "neunzehn": 19,
    "zwanzig": 20,
    "dreissig": 30,
    "dreißig": 30,
    "vierzig": 40,
    "fuenfzig": 50,
    "fünfzig": 50,
    "sechzig": 60,
    "siebzig": 70,
    "achtzig": 80,
    "neunzig": 90,
    "hundert": 100,
}

ENGLISH_NUMBER_WORDS = {
    "zero": 0,
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
    "eleven": 11,
    "twelve": 12,
    "thirteen": 13,
    "fourteen": 14,
    "fifteen": 15,
    "sixteen": 16,
    "seventeen": 17,
    "eighteen": 18,
    "nineteen": 19,
    "twenty": 20,
    "thirty": 30,
    "forty": 40,
    "fifty": 50,
    "sixty": 60,
    "seventy": 70,
    "eighty": 80,
    "ninety": 90,
    "hundred": 100,
}

NOTICE_KEYWORDS = (
    "entfernt",
    "removed",
    "gelöscht",
    "gestrichen",
)

REVIEW_NOUN_KEYWORDS = (
    "bewertungen",
    "rezensionen",
    "reviews",
)

LEGAL_CONTEXT_KEYWORDS = (
    "beschwerden",
    "complaints",
    "diffamierung",
    "defamation",
    "verleumdung",
    "deutschem recht",
    "german law",
)

WINDOW_CONTEXT_KEYWORDS = (
    "365 tage",
    "365 days",
    "letzten 365",
    "last 365",
)

GENERIC_DISCLOSURE_PHRASES = (
    "nicht von google überprüft",
    "not checked by google",
    "gefälschten inhalten",
    "fake content",
)

RANGE_SEPARATORS = r"(?:-|–|—|bis|to|through)"


def extract_deleted_review_notice_text(text: str) -> str:
    """Return the most likely deleted-review notice line from a block of text."""

    if not text:
        return ""

    for line in _iter_candidate_lines(text):
        if _looks_like_deleted_review_notice(line):
            return line.strip()

    normalized = _normalize_text(text)
    for sentence in re.split(r"(?<=[.!?])\s+", normalized):
        if _looks_like_deleted_review_notice(sentence):
            return sentence.strip()

    return ""


def parse_deleted_review_notice(text: str) -> DeletedReviewNotice:
    """Parse a deleted-review notice into normalized bounds."""

    raw_text = text.strip() if text else ""
    if not raw_text:
        return DeletedReviewNotice()

    normalized = _normalize_text(raw_text)
    min_count, max_count = _parse_count_bounds(normalized)
    return DeletedReviewNotice(
        min_count=min_count,
        max_count=max_count,
        raw_text=raw_text,
    )


def normalize_deleted_review_notice_payload(
    payload: Mapping[str, Any],
) -> DeletedReviewNotice:
    """Coerce a JSON payload returned by the LLM into a notice object."""

    min_count = _coerce_optional_int(
        _first_present(payload, "deleted_review_count_min", "min_count", "min", "lower")
    )
    max_count = _coerce_optional_int(
        _first_present(payload, "deleted_review_count_max", "max_count", "max", "upper")
    )
    raw_text = str(
        _first_present(payload, "deleted_review_notice", "raw_text", "text", default="")
    ).strip()

    if min_count is None and max_count is None:
        exact = _coerce_optional_int(
            _first_present(payload, "deleted_review_count", "count", "value")
        )
        if exact is not None:
            min_count = exact
            max_count = exact

    return DeletedReviewNotice(
        min_count=min_count,
        max_count=max_count,
        raw_text=raw_text,
        source=str(payload.get("source") or "llm"),
        confidence=_coerce_optional_float(payload.get("confidence")),
    )


class DeletedReviewBulkNormalizer:
    """Batch normalizer backed by OpenRouter."""

    def __init__(
        self,
        client: OpenRouterClient,
        *,
        model: Optional[str] = None,
        response_format: str = "json_object",
        temperature: float = 0.0,
    ) -> None:
        self.client = client
        self.model = model or client.default_model
        self.response_format = response_format
        self.temperature = temperature

    def normalize(self, entries: Sequence[DeletedReviewInput]) -> Dict[str, DeletedReviewNotice]:
        if not entries:
            return {}

        response = self.client.create_chat_completion_sync(
            self._build_messages(entries),
            model=self.model,
            response_format=self.response_format,
            temperature=self.temperature,
        )

        parsed = _extract_json_payload(response)
        if not isinstance(parsed, dict):
            raise OpenRouterClientError("Deleted-review normalizer expected a JSON object response")

        normalized: Dict[str, DeletedReviewNotice] = {}
        for entry in entries:
            raw_entry = parsed.get(entry.place_id)
            if raw_entry is None:
                raw_entry = parsed.get(str(entry.place_id))
            if isinstance(raw_entry, dict):
                normalized[entry.place_id] = normalize_deleted_review_notice_payload(raw_entry)
            elif isinstance(raw_entry, str):
                normalized[entry.place_id] = parse_deleted_review_notice(raw_entry)
            elif raw_entry is None:
                normalized[entry.place_id] = DeletedReviewNotice()
            else:
                normalized[entry.place_id] = normalize_deleted_review_notice_payload({"value": raw_entry})

        return normalized

    def _build_messages(self, entries: Sequence[DeletedReviewInput]) -> List[Dict[str, str]]:
        system_prompt = (
            "You normalize Google Maps deleted-review notices. "
            "Return JSON only. The top-level object must use the place_id of each listing as the key. "
            "Each value must include deleted_review_count_min, deleted_review_count_max, deleted_review_notice, "
            "and source. Preserve ranges exactly when the notice is approximate."
        )
        user_payload = {
            "listings": [entry.to_prompt_payload() for entry in entries],
            "instructions": {
                "range_policy": "Use numeric bounds. For exact numbers set min=max. For phrases like 'more than 100' leave max null.",
            },
        }
        user_prompt = (
            "Normalize these listings and return only JSON.\n"
            "Use place_id as the top-level key for each listing.\n"
            f"{json.dumps(user_payload, ensure_ascii=False)}"
        )
        return [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]


def _parse_count_bounds(text: str) -> tuple[Optional[int], Optional[int]]:
    range_match = _match_range(text)
    if range_match:
        return range_match

    more_than_match = _match_more_than(text)
    if more_than_match is not None:
        return more_than_match, None

    single = _extract_first_number(text)
    if single is not None:
        return single, single

    return None, None


def _match_range(text: str) -> Optional[tuple[int, int]]:
    digit_range = re.search(
        rf"(?P<start>\d+)\s*{RANGE_SEPARATORS}\s*(?P<end>\d+)",
        text,
        flags=re.IGNORECASE,
    )
    if digit_range:
        start = int(digit_range.group("start"))
        end = int(digit_range.group("end"))
        return _ordered_bounds(start, end)

    word_range = re.search(
        rf"(?P<start>[a-zäöüß0-9]+)\s*{RANGE_SEPARATORS}\s*(?P<end>[a-zäöüß0-9]+)",
        text,
        flags=re.IGNORECASE,
    )
    if word_range:
        start = _word_or_number_to_int(word_range.group("start"))
        end = _word_or_number_to_int(word_range.group("end"))
        if start is not None and end is not None:
            return _ordered_bounds(start, end)

    return None


def _match_more_than(text: str) -> Optional[int]:
    more_than = re.search(
        r"(?:more than|mehr als|über|ueber|above)\s+([a-zäöüß0-9]+)",
        text,
        flags=re.IGNORECASE,
    )
    if more_than:
        value = _word_or_number_to_int(more_than.group(1))
        if value is not None:
            return value + 1
    return None


def _extract_first_number(text: str) -> Optional[int]:
    digit_match = re.search(r"\b(\d+)\b", text)
    if digit_match:
        return int(digit_match.group(1))

    for token in re.findall(r"\b[a-zäöüß]+\b", text, flags=re.IGNORECASE):
        value = _word_or_number_to_int(token)
        if value is not None:
            return value
    return None


def _word_or_number_to_int(token: str) -> Optional[int]:
    token = token.strip().lower().replace("ü", "ue").replace("ö", "oe").replace("ä", "ae")
    if not token:
        return None

    if token.isdigit():
        return int(token)

    if token in GERMAN_NUMBER_WORDS:
        return GERMAN_NUMBER_WORDS[token]
    if token in ENGLISH_NUMBER_WORDS:
        return ENGLISH_NUMBER_WORDS[token]
    return None


def _ordered_bounds(start: int, end: int) -> tuple[int, int]:
    if start <= end:
        return start, end
    return end, start


def _normalize_text(text: str) -> str:
    return " ".join(text.split())


def _iter_candidate_lines(text: str) -> Iterable[str]:
    for line in text.splitlines():
        cleaned = line.strip()
        if cleaned:
            yield cleaned


def _looks_like_deleted_review_notice(text: str) -> bool:
    normalized = _normalize_text(text).lower()
    if not normalized:
        return False

    if any(phrase in normalized for phrase in GENERIC_DISCLOSURE_PHRASES):
        return False

    if not any(keyword in normalized for keyword in NOTICE_KEYWORDS):
        return False

    if not any(keyword in normalized for keyword in REVIEW_NOUN_KEYWORDS):
        return False

    if any(keyword in normalized for keyword in LEGAL_CONTEXT_KEYWORDS):
        return True

    return any(keyword in normalized for keyword in WINDOW_CONTEXT_KEYWORDS)


def _extract_json_payload(response: Any) -> Any:
    if not isinstance(response, dict):
        return response

    if "choices" not in response or not response["choices"]:
        return None

    message = response["choices"][0].get("message") or {}
    if not isinstance(message, dict):
        return None

    if "parsed" in message:
        return message["parsed"]

    content = message.get("content")
    if isinstance(content, str):
        try:
            return json.loads(content)
        except (TypeError, ValueError):
            return None

    if isinstance(content, list):
        text_parts: List[str] = []
        for part in content:
            if isinstance(part, dict) and isinstance(part.get("text"), str):
                text_parts.append(part["text"])
        if text_parts:
            try:
                return json.loads("".join(text_parts))
            except (TypeError, ValueError):
                return None

    return None


def _coerce_optional_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _coerce_optional_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _first_present(payload: Mapping[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in payload:
            return payload.get(key)
    return default
