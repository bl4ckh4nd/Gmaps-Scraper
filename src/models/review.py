"""Review data model for Google Maps scraper."""

from dataclasses import dataclass
from typing import Optional, Dict, Any
import csv
import io


@dataclass
class Review:
    """Data model for a review from Google Maps."""
    
    place_id: str
    business_name: str
    business_address: str
    reviewer_name: str = ""
    review_text: str = ""
    rating: int = 0
    review_date: str = ""
    owner_response: str = ""
    language: str = "unknown"
    
    def __post_init__(self):
        """Validate and clean data after initialization."""
        # Validate rating
        if self.rating < 0 or self.rating > 5:
            self.rating = 0
        
        # Clean text fields
        self.review_text = self.review_text.strip() if self.review_text else ""
        self.reviewer_name = self.reviewer_name.strip() if self.reviewer_name else ""
        self.owner_response = self.owner_response.strip() if self.owner_response else ""
        self.review_date = self.review_date.strip() if self.review_date else ""
    
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
            'language': self.language
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
            rating=int(data.get('rating', 0)),
            review_date=data.get('review_date', ''),
            owner_response=data.get('owner_response', ''),
            language=data.get('language', 'unknown')
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