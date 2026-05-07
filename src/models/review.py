"""Review data model for Google Maps scraper."""

from dataclasses import dataclass
from typing import Optional, Dict, Any
import csv
import hashlib
import io
import math
import re


@dataclass
class Review:
    """Data model for a review from Google Maps."""

    HASH_VERSION = 2
    
    place_id: str
    business_name: str
    business_address: str
    reviewer_name: str = ""
    review_text: str = ""
    rating: int = 0
    review_date: str = ""
    owner_response: str = ""
    language: str = "unknown"
    review_hash: str = ""
    
    def __post_init__(self):
        """Validate and clean data after initialization."""
        self.place_id = self._coerce_text(self.place_id)
        self.business_name = self._coerce_text(self.business_name)
        self.business_address = self._coerce_text(self.business_address)
        self.reviewer_name = self._coerce_text(self.reviewer_name)
        self.review_text = self._coerce_text(self.review_text)
        self.review_date = self._coerce_text(self.review_date)
        self.owner_response = self._coerce_text(self.owner_response)
        self.language = self._coerce_text(self.language) or "unknown"
        self.rating = self._coerce_rating(self.rating)

        # Validate rating
        if self.rating < 0 or self.rating > 5:
            self.rating = 0

        if self.reviewer_name or self.review_text or self.rating > 0:
            self.review_hash = self.build_review_hash(
                self.place_id,
                self.reviewer_name,
                self.rating,
                self.review_text,
            )
        else:
            self.review_hash = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert review to dictionary for CSV export."""
        return {
            'place_id': self.place_id,
            'business_name': self.business_name,
            'business_address': self.business_address,
            'reviewer_name': self.reviewer_name,
            'review_text': self.review_text,
            'rating': self.rating,
            'review_date': self.review_date,
            'owner_response': self.owner_response,
            'language': self.language,
            'review_hash': self.review_hash,
        }
    
    def to_csv_row(self) -> str:
        """Convert review to CSV row string."""
        data = self.to_dict()
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=data.keys())
        writer.writerow(data)
        return output.getvalue().strip()
    
    @classmethod
    def get_csv_header(cls) -> str:
        """Get CSV header for review data."""
        dummy = cls(place_id="", business_name="", business_address="")
        data = dummy.to_dict()
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=data.keys())
        writer.writeheader()
        return output.getvalue().strip()
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Review':
        """Create Review instance from dictionary."""
        return cls(
            place_id=data.get('place_id', ''),
            business_name=data.get('business_name', ''),
            business_address=data.get('business_address', ''),
            reviewer_name=data.get('reviewer_name', ''),
            review_text=data.get('review_text', ''),
            rating=data.get('rating', 0),
            review_date=data.get('review_date', ''),
            owner_response=data.get('owner_response', ''),
            language=data.get('language', 'unknown'),
            review_hash=data.get('review_hash', ''),
        )
    
    def is_valid(self) -> bool:
        """Check if review has minimum required data."""
        return bool(self.place_id and self.business_name and 
                   (self.review_text or self.rating > 0))
    
    def has_text_content(self) -> bool:
        """Check if review has meaningful text content."""
        return len(self.review_text.strip()) > 0
    
    def has_owner_response(self) -> bool:
        """Check if review has an owner response."""
        return len(self.owner_response.strip()) > 0

    @classmethod
    def build_review_hash(
        cls,
        place_id: str,
        reviewer_name: str,
        rating: int,
        review_text: str,
    ) -> str:
        normalized_parts = [
            cls._normalize_hash_component(place_id),
            cls._normalize_hash_component(reviewer_name),
            str(int(rating or 0)),
            cls._normalize_hash_component(review_text),
        ]
        payload = "\x1f".join(normalized_parts)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    @staticmethod
    def _normalize_hash_component(value: str) -> str:
        normalized_value = Review._coerce_text(value)
        if not normalized_value:
            return ""

        normalized = re.sub(r"\s+", " ", normalized_value)
        return normalized.casefold()

    @staticmethod
    def _coerce_text(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, float) and math.isnan(value):
            return ""
        if isinstance(value, str):
            return value.strip()
        return str(value).strip()

    @staticmethod
    def _coerce_rating(value: Any) -> int:
        if value is None:
            return 0
        if isinstance(value, float) and math.isnan(value):
            return 0

        try:
            return int(float(value))
        except (TypeError, ValueError):
            return 0
