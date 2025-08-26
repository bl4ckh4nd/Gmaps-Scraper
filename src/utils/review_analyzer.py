"""Review analysis utilities for calculating business metrics."""

from datetime import datetime, timedelta
from typing import List, Tuple, Optional, Dict, Any
import re
from ..models.review import Review


def parse_review_date(text: str, reference_date: datetime = None) -> datetime:
    """
    Convert relative review dates (German/English) into actual datetimes.
    
    Args:
        text: Review date text (e.g. 'vor einem Monat', 'a month ago')
        reference_date: Reference date to calculate from (default: today)
        
    Returns:
        Parsed datetime object
    """
    if reference_date is None:
        reference_date = datetime.now()
        
    text = text.strip().lower()
    
    # Remove 'bearbeitet:' prefix if present
    if text.startswith('bearbeitet:'):
        text = text.replace('bearbeitet:', '').strip()
    
    # Handle German dates
    if text in ["heute", "today"]:
        return reference_date
    if text in ["gestern", "yesterday"]:
        return reference_date - timedelta(days=1)
    if text in ["vor einem tag", "vor 1 tag"]:
        return reference_date - timedelta(days=1)
    if text in ["vor einer woche", "vor 1 woche"]:
        return reference_date - timedelta(weeks=1)
    if text in ["vor einem monat", "vor 1 monat"]:
        return reference_date - timedelta(days=30)
    if text in ["vor einem jahr", "vor 1 jahr"]:
        return reference_date - timedelta(days=365)
    
    # Handle English dates
    if text in ["a day ago", "1 day ago"]:
        return reference_date - timedelta(days=1)
    if text in ["a week ago", "1 week ago"]:
        return reference_date - timedelta(weeks=1)
    if text in ["a month ago", "1 month ago"]:
        return reference_date - timedelta(days=30)
    if text in ["a year ago", "1 year ago"]:
        return reference_date - timedelta(days=365)
    
    # Handle German numerical patterns
    german_match = re.match(r"vor (\d+) (tag|tagen|woche|wochen|monat|monaten|jahr|jahren)", text)
    if german_match:
        num = int(german_match.group(1))
        unit = german_match.group(2)
        if "tag" in unit:
            return reference_date - timedelta(days=num)
        elif "woche" in unit:
            return reference_date - timedelta(weeks=num)
        elif "monat" in unit:
            return reference_date - timedelta(days=num * 30)
        elif "jahr" in unit:
            return reference_date - timedelta(days=num * 365)
    
    # Handle English numerical patterns
    english_match = re.match(r"(\d+) (day|days|week|weeks|month|months|year|years) ago", text)
    if english_match:
        num = int(english_match.group(1))
        unit = english_match.group(2)
        if "day" in unit:
            return reference_date - timedelta(days=num)
        elif "week" in unit:
            return reference_date - timedelta(weeks=num)
        elif "month" in unit:
            return reference_date - timedelta(days=num * 30)
        elif "year" in unit:
            return reference_date - timedelta(days=num * 365)
    
    # Try to parse absolute dates (fallback)
    try:
        # Common date formats
        for date_format in ["%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y", "%m/%d/%Y"]:
            try:
                return datetime.strptime(text, date_format)
            except ValueError:
                continue
    except Exception:
        pass
    
    # Fallback: return reference date if parsing fails
    return reference_date


def calculate_reply_rates(reviews: List[Review]) -> Tuple[float, float]:
    """
    Calculate reply rates for good and bad reviews.
    
    Args:
        reviews: List of Review objects
        
    Returns:
        Tuple of (good_review_reply_rate, bad_review_reply_rate) as percentages
    """
    if not reviews:
        return 0.0, 0.0
    
    good_reviews = [r for r in reviews if r.rating >= 4]
    bad_reviews = [r for r in reviews if r.rating <= 2]
    
    # Calculate reply rates
    good_reply_rate = 0.0
    bad_reply_rate = 0.0
    
    if good_reviews:
        good_replies = sum(1 for r in good_reviews if r.owner_response and r.owner_response.strip())
        good_reply_rate = (good_replies / len(good_reviews)) * 100
    
    if bad_reviews:
        bad_replies = sum(1 for r in bad_reviews if r.owner_response and r.owner_response.strip())
        bad_reply_rate = (bad_replies / len(bad_reviews)) * 100
    
    return good_reply_rate, bad_reply_rate


def calculate_avg_time_between_reviews(reviews: List[Review], months: int = 12) -> Optional[float]:
    """
    Calculate average time between reviews in the last N months.
    
    Args:
        reviews: List of Review objects with parsed dates
        months: Number of months to look back (default: 12)
        
    Returns:
        Average days between reviews, or None if insufficient data
    """
    if len(reviews) < 2:
        return None
    
    # Parse review dates
    reference_date = datetime.now()
    review_dates = []
    
    for review in reviews:
        if hasattr(review, 'parsed_date') and review.parsed_date:
            review_dates.append(review.parsed_date)
        elif review.review_date:
            parsed_date = parse_review_date(review.review_date, reference_date)
            review_dates.append(parsed_date)
    
    if len(review_dates) < 2:
        return None
    
    # Filter to last N months
    cutoff_date = reference_date - timedelta(days=months * 30)
    recent_dates = [date for date in review_dates if date > cutoff_date]
    
    if len(recent_dates) < 2:
        return None
    
    # Sort dates and calculate differences
    recent_dates.sort()
    time_diffs = []
    
    for i in range(1, len(recent_dates)):
        diff = recent_dates[i] - recent_dates[i-1]
        time_diffs.append(diff.days)
    
    if not time_diffs:
        return None
    
    # Return average time in days
    return sum(time_diffs) / len(time_diffs)


def analyze_reviews(reviews: List[Review]) -> Dict[str, Any]:
    """
    Analyze reviews and return comprehensive metrics.
    
    Args:
        reviews: List of Review objects
        
    Returns:
        Dictionary containing all review metrics
    """
    if not reviews:
        return {
            'reply_rate_good': 0.0,
            'reply_rate_bad': 0.0,
            'avg_time_between_reviews': None,
            'total_reviews': 0,
            'good_reviews': 0,
            'bad_reviews': 0,
            'neutral_reviews': 0
        }
    
    # Parse dates for all reviews if not already parsed
    reference_date = datetime.now()
    for review in reviews:
        if not hasattr(review, 'parsed_date') or not review.parsed_date:
            review.parsed_date = parse_review_date(review.review_date or "", reference_date)
    
    # Calculate reply rates
    reply_rate_good, reply_rate_bad = calculate_reply_rates(reviews)
    
    # Calculate average time between reviews
    avg_time = calculate_avg_time_between_reviews(reviews)
    
    # Count review types
    good_reviews = len([r for r in reviews if r.rating >= 4])
    bad_reviews = len([r for r in reviews if r.rating <= 2])
    neutral_reviews = len([r for r in reviews if 2 < r.rating < 4])
    
    return {
        'reply_rate_good': round(reply_rate_good, 1),
        'reply_rate_bad': round(reply_rate_bad, 1), 
        'avg_time_between_reviews': round(avg_time, 1) if avg_time else None,
        'total_reviews': len(reviews),
        'good_reviews': good_reviews,
        'bad_reviews': bad_reviews,
        'neutral_reviews': neutral_reviews
    }


def get_review_summary_stats(reviews: List[Review]) -> Dict[str, Any]:
    """
    Get summary statistics for reviews.
    
    Args:
        reviews: List of Review objects
        
    Returns:
        Dictionary with summary statistics
    """
    if not reviews:
        return {
            'has_reviews': False,
            'recent_activity': False,
            'responsive_to_complaints': False,
            'consistent_quality': None
        }
    
    analysis = analyze_reviews(reviews)
    
    # Determine recent activity (reviews in last 3 months)
    recent_cutoff = datetime.now() - timedelta(days=90)
    recent_reviews = []
    
    for review in reviews:
        review_date = getattr(review, 'parsed_date', None)
        if review_date and review_date >= recent_cutoff:
            recent_reviews.append(review)
    
    return {
        'has_reviews': len(reviews) > 0,
        'recent_activity': len(recent_reviews) > 0,
        'responsive_to_complaints': analysis['reply_rate_bad'] > 50,
        'consistent_quality': analysis['avg_time_between_reviews'] is not None and analysis['avg_time_between_reviews'] < 30,
        'total_recent_reviews': len(recent_reviews)
    }