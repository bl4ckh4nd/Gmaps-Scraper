"""Business data model for Google Maps scraper."""

from dataclasses import dataclass, field
from typing import Optional, Dict, Any
import csv
import io


@dataclass
class Business:
    """Data model for a business listing from Google Maps."""
    
    place_id: str
    name: str
    address: str = ""
    website: Optional[str] = None
    phone: Optional[str] = None
    review_count: int = 0
    review_average: float = 0.0
    store_shopping: str = "No"
    in_store_pickup: str = "No"
    store_delivery: str = "No"
    place_type: str = ""
    opens_at: str = ""
    introduction: str = "None Found"
    maps_url: str = ""
    # Review analysis metrics
    reply_rate_good: float = 0.0
    reply_rate_bad: float = 0.0
    avg_time_between_reviews: Optional[float] = None
    
    def __post_init__(self):
        """Validate and clean data after initialization."""
        # Ensure website has proper protocol
        if self.website and not self.website.startswith(('http://', 'https://')):
            self.website = f"https://{self.website}"
        
        # Clean phone number
        if self.phone:
            self.phone = self.phone.strip()
        
        # Validate review average
        if self.review_average < 0 or self.review_average > 5:
            self.review_average = 0.0
        
        # Validate review count
        if self.review_count < 0:
            self.review_count = 0
        
        # Validate reply rates (should be 0-100)
        if self.reply_rate_good < 0 or self.reply_rate_good > 100:
            self.reply_rate_good = 0.0
        if self.reply_rate_bad < 0 or self.reply_rate_bad > 100:
            self.reply_rate_bad = 0.0
        
        # Validate average time between reviews (should be positive or None)
        if self.avg_time_between_reviews is not None and self.avg_time_between_reviews < 0:
            self.avg_time_between_reviews = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert business to dictionary for CSV export."""
        return {
            'Place ID': self.place_id,
            'Names': self.name,
            'Address': self.address,
            'Website': self.website or "",
            'Phone Number': self.phone or "",
            'Review Count': self.review_count,
            'Average Review': self.review_average,
            'Store Shopping': self.store_shopping,
            'In Store Pickup': self.in_store_pickup,
            'Delivery': self.store_delivery,
            'Type': self.place_type,
            'Opens At': self.opens_at,
            'Introduction': self.introduction,
            'Maps URL': self.maps_url,
            'Reply Rate Good (%)': self.reply_rate_good,
            'Reply Rate Bad (%)': self.reply_rate_bad,
            'Avg Days Between Reviews': self.avg_time_between_reviews
        }
    
    def to_csv_row(self) -> str:
        """Convert business to CSV row string."""
        data = self.to_dict()
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=data.keys())
        writer.writerow(data)
        return output.getvalue().strip()
    
    @classmethod
    def get_csv_header(cls) -> str:
        """Get CSV header for business data."""
        dummy = cls(place_id="", name="")
        data = dummy.to_dict()
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=data.keys())
        writer.writeheader()
        return output.getvalue().strip()
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Business':
        """Create Business instance from dictionary."""
        return cls(
            place_id=data.get('Place ID', ''),
            name=data.get('Names', ''),
            address=data.get('Address', ''),
            website=data.get('Website'),
            phone=data.get('Phone Number'),
            review_count=int(data.get('Review Count', 0)),
            review_average=float(data.get('Average Review', 0.0)),
            store_shopping=data.get('Store Shopping', 'No'),
            in_store_pickup=data.get('In Store Pickup', 'No'),
            store_delivery=data.get('Delivery', 'No'),
            place_type=data.get('Type', ''),
            opens_at=data.get('Opens At', ''),
            introduction=data.get('Introduction', 'None Found'),
            maps_url=data.get('Maps URL', ''),
            reply_rate_good=float(data.get('Reply Rate Good (%)', 0.0)),
            reply_rate_bad=float(data.get('Reply Rate Bad (%)', 0.0)),
            avg_time_between_reviews=float(data.get('Avg Days Between Reviews', 0)) if data.get('Avg Days Between Reviews') else None
        )
    
    def is_duplicate_of(self, other: 'Business') -> bool:
        """Check if this business is a duplicate of another."""
        return (self.name == other.name and 
                self.address == other.address and
                self.place_id == other.place_id)
    
    def update_service_info(self, info_text: str) -> None:
        """Update service information based on info text."""
        if not info_text:
            return
            
        info_lower = info_text.lower().replace("\n", "")
        
        if 'shop' in info_lower:
            self.store_shopping = "Yes"
        elif 'pickup' in info_lower:
            self.in_store_pickup = "Yes"
        elif 'delivery' in info_lower:
            self.store_delivery = "Yes"