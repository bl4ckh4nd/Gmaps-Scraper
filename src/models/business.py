"""Business data model for Google Maps scraper."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any
import csv
import io

from .owner_enrichment import OwnerDetails


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
    deleted_review_count_min: Optional[int] = None
    deleted_review_count_max: Optional[int] = None
    deleted_review_notice: str = ""
    store_shopping: str = "No"
    in_store_pickup: str = "No"
    store_delivery: str = "No"
    place_type: str = ""
    opens_at: str = ""
    introduction: str = "None Found"
    maps_url: str = ""
    source_query: str = ""
    # Review analysis metrics
    reply_rate_good: float = 0.0
    reply_rate_bad: float = 0.0
    avg_time_between_reviews: Optional[float] = None
    reviews_last_365d_min: int = 0
    reviews_last_365d_max: int = 0
    reviews_last_365d_mid: float = 0.0
    deleted_review_rate_min_pct: Optional[float] = None
    deleted_review_rate_max_pct: Optional[float] = None
    deleted_review_rate_mid_pct: Optional[float] = None
    review_window_coverage_status: str = "not_requested"
    review_window_cutoff_observed: str = ""
    website_status: str = ""
    website_modernity_score: Optional[int] = None
    website_modernity_reason: str = ""
    website_uses_https: Optional[bool] = None
    website_mobile_friendly_hint: Optional[bool] = None
    website_structured_data_hint: Optional[bool] = None
    website_stale_or_broken_hint: Optional[bool] = None
    owner_details: OwnerDetails = field(default_factory=OwnerDetails)
    export_contact_fields: bool = field(default=True, repr=False, compare=False)
    export_business_details: bool = field(default=True, repr=False, compare=False)
    export_review_summary: bool = field(default=True, repr=False, compare=False)
    export_review_analytics: bool = field(default=True, repr=False, compare=False)
    export_deleted_review_signals: bool = field(default=True, repr=False, compare=False)
    export_website_modernity: bool = field(default=False, repr=False, compare=False)
    
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

        if self.deleted_review_count_min is not None and self.deleted_review_count_min < 0:
            self.deleted_review_count_min = None
        if self.deleted_review_count_max is not None and self.deleted_review_count_max < 0:
            self.deleted_review_count_max = None
        if (
            self.deleted_review_count_min is not None
            and self.deleted_review_count_max is not None
            and self.deleted_review_count_min > self.deleted_review_count_max
        ):
            self.deleted_review_count_min = None
            self.deleted_review_count_max = None
        
        # Validate reply rates (should be 0-100)
        if self.reply_rate_good < 0 or self.reply_rate_good > 100:
            self.reply_rate_good = 0.0
        if self.reply_rate_bad < 0 or self.reply_rate_bad > 100:
            self.reply_rate_bad = 0.0
        
        # Validate average time between reviews (should be positive or None)
        if self.avg_time_between_reviews is not None and self.avg_time_between_reviews < 0:
            self.avg_time_between_reviews = None
        if self.reviews_last_365d_min < 0:
            self.reviews_last_365d_min = 0
        if self.reviews_last_365d_max < 0:
            self.reviews_last_365d_max = 0
        if self.reviews_last_365d_min > self.reviews_last_365d_max:
            self.reviews_last_365d_min = 0
            self.reviews_last_365d_max = 0
            self.reviews_last_365d_mid = 0.0
        if self.website_modernity_score is not None:
            self.website_modernity_score = max(0, min(100, int(self.website_modernity_score)))
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert business to dictionary for CSV export."""
        return {
            'Place ID': self.place_id,
            'Names': self.name,
            'Address': self.address,
            'Website': self.website if self.export_contact_fields and self.website else "",
            'Phone Number': self.phone if self.export_contact_fields and self.phone else "",
            'Review Count': self.review_count if self.export_review_summary else "",
            'Deleted Review Count Min': (
                self.deleted_review_count_min
                if self.export_deleted_review_signals and self.deleted_review_count_min is not None
                else ""
            ),
            'Deleted Review Count Max': (
                self.deleted_review_count_max
                if self.export_deleted_review_signals and self.deleted_review_count_max is not None
                else ""
            ),
            'Deleted Review Notice': (
                self.deleted_review_notice if self.export_deleted_review_signals else ""
            ),
            'Average Review': self.review_average if self.export_review_summary else "",
            'Store Shopping': self.store_shopping if self.export_business_details else "",
            'In Store Pickup': self.in_store_pickup if self.export_business_details else "",
            'Delivery': self.store_delivery if self.export_business_details else "",
            'Type': self.place_type if self.export_business_details else "",
            'Opens At': self.opens_at if self.export_business_details else "",
            'Introduction': self.introduction if self.export_business_details else "",
            'Maps URL': self.maps_url,
            'Search Query': self.source_query,
            'Reply Rate Good (%)': self.reply_rate_good if self.export_review_analytics else "",
            'Reply Rate Bad (%)': self.reply_rate_bad if self.export_review_analytics else "",
            'Avg Days Between Reviews': self.avg_time_between_reviews if self.export_review_analytics else "",
            'Reviews Last 365d Min': self.reviews_last_365d_min if self.export_review_analytics else "",
            'Reviews Last 365d Max': self.reviews_last_365d_max if self.export_review_analytics else "",
            'Reviews Last 365d Mid': self.reviews_last_365d_mid if self.export_review_analytics else "",
            'Deleted Review Rate Min (%)': (
                self.deleted_review_rate_min_pct
                if self.export_review_analytics and self.deleted_review_rate_min_pct is not None
                else ""
            ),
            'Deleted Review Rate Max (%)': (
                self.deleted_review_rate_max_pct
                if self.export_review_analytics and self.deleted_review_rate_max_pct is not None
                else ""
            ),
            'Deleted Review Rate Mid (%)': (
                self.deleted_review_rate_mid_pct
                if self.export_review_analytics and self.deleted_review_rate_mid_pct is not None
                else ""
            ),
            'Review Window Coverage Status': (
                self.review_window_coverage_status if self.export_review_analytics else ""
            ),
            'Review Window Cutoff Observed': (
                self.review_window_cutoff_observed if self.export_review_analytics else ""
            ),
            'Website Status': self.website_status if self.export_website_modernity else "",
            'Website Modernity Score': (
                self.website_modernity_score if self.export_website_modernity and self.website_modernity_score is not None else ""
            ),
            'Website Modernity Reason': self.website_modernity_reason if self.export_website_modernity else "",
            'Website Uses HTTPS': _optional_bool_to_text(self.website_uses_https) if self.export_website_modernity else "",
            'Website Mobile Friendly Hint': _optional_bool_to_text(self.website_mobile_friendly_hint) if self.export_website_modernity else "",
            'Website Structured Data Hint': _optional_bool_to_text(self.website_structured_data_hint) if self.export_website_modernity else "",
            'Website Stale/Broken Hint': _optional_bool_to_text(self.website_stale_or_broken_hint) if self.export_website_modernity else "",
            'Owner Name': self.owner_details.owner_name or "",
            'Owner Status': self.owner_details.status,
            'Owner Confidence': self.owner_details.confidence if self.owner_details.confidence is not None else "",
            'Owner Source URL': self.owner_details.source_url or "",
            'Owner Last Checked': self.owner_details.last_checked.isoformat() if self.owner_details.last_checked else "",
            'Owner LLM Model': self.owner_details.llm_model or "",
            'Owner Reason': self.owner_details.reason or "",
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
            deleted_review_count_min=_optional_int(data.get('Deleted Review Count Min')),
            deleted_review_count_max=_optional_int(data.get('Deleted Review Count Max')),
            deleted_review_notice=_optional_text(data.get('Deleted Review Notice')),
            review_average=float(data.get('Average Review', 0.0)),
            store_shopping=data.get('Store Shopping', 'No'),
            in_store_pickup=data.get('In Store Pickup', 'No'),
            store_delivery=data.get('Delivery', 'No'),
            place_type=data.get('Type', ''),
            opens_at=data.get('Opens At', ''),
            introduction=data.get('Introduction', 'None Found'),
            maps_url=data.get('Maps URL', ''),
            source_query=_optional_text(data.get('Search Query')),
            reply_rate_good=float(data.get('Reply Rate Good (%)', 0.0)),
            reply_rate_bad=float(data.get('Reply Rate Bad (%)', 0.0)),
            avg_time_between_reviews=float(data.get('Avg Days Between Reviews', 0)) if data.get('Avg Days Between Reviews') else None,
            reviews_last_365d_min=_optional_int(data.get('Reviews Last 365d Min')) or 0,
            reviews_last_365d_max=_optional_int(data.get('Reviews Last 365d Max')) or 0,
            reviews_last_365d_mid=_optional_float(data.get('Reviews Last 365d Mid')) or 0.0,
            deleted_review_rate_min_pct=_optional_float(data.get('Deleted Review Rate Min (%)')),
            deleted_review_rate_max_pct=_optional_float(data.get('Deleted Review Rate Max (%)')),
            deleted_review_rate_mid_pct=_optional_float(data.get('Deleted Review Rate Mid (%)')),
            review_window_coverage_status=_optional_text(data.get('Review Window Coverage Status')) or 'not_requested',
            review_window_cutoff_observed=_optional_text(data.get('Review Window Cutoff Observed')),
            website_status=_optional_text(data.get('Website Status')),
            website_modernity_score=_optional_int(data.get('Website Modernity Score')),
            website_modernity_reason=_optional_text(data.get('Website Modernity Reason')),
            website_uses_https=_optional_bool(data.get('Website Uses HTTPS')),
            website_mobile_friendly_hint=_optional_bool(data.get('Website Mobile Friendly Hint')),
            website_structured_data_hint=_optional_bool(data.get('Website Structured Data Hint')),
            website_stale_or_broken_hint=_optional_bool(data.get('Website Stale/Broken Hint')),
            export_website_modernity=any(
                key in data
                for key in (
                    'Website Status',
                    'Website Modernity Score',
                    'Website Modernity Reason',
                    'Website Uses HTTPS',
                    'Website Mobile Friendly Hint',
                    'Website Structured Data Hint',
                    'Website Stale/Broken Hint',
                )
            ),
            owner_details=_owner_details_from_dict(data)
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


def _owner_details_from_dict(data: Dict[str, Any]) -> OwnerDetails:
    details = OwnerDetails(
        owner_name=data.get('Owner Name') or None,
        status=data.get('Owner Status') or 'not_requested',
        confidence=float(data['Owner Confidence']) if data.get('Owner Confidence') not in (None, "") else None,
        source_url=data.get('Owner Source URL') or None,
        llm_model=data.get('Owner LLM Model') or None,
        reason=data.get('Owner Reason') or None,
    )

    last_checked = data.get('Owner Last Checked')
    if last_checked:
        try:
            details.last_checked = datetime.fromisoformat(str(last_checked))
        except ValueError:
            details.last_checked = None

    return details


def _optional_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _optional_text(value: Any) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, float) and value != value:
        return ""
    return str(value)


def _optional_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _optional_bool(value: Any) -> Optional[bool]:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"yes", "true", "1"}:
        return True
    if text in {"no", "false", "0"}:
        return False
    return None


def _optional_bool_to_text(value: Optional[bool]) -> str:
    if value is True:
        return "Yes"
    if value is False:
        return "No"
    return ""
