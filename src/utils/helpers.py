"""Utility functions for Google Maps scraper."""

import re
from typing import Optional
import logging


def extract_place_id(url: str) -> str:
    """Extract the unique place ID from a Google Maps URL.
    
    Args:
        url: Google Maps URL
        
    Returns:
        Place ID string, or the full URL if extraction fails
    """
    try:
        # Look for the !19s pattern which is followed by the place ID
        if '!19s' in url:
            # Extract everything after !19s
            place_id = url.split('!19s')[1].split('!')[0]
            return place_id
        
        # Alternative method - look for the data= pattern
        elif 'data=' in url:
            parts = url.split('/')
            # Find the part with business ID
            for part in parts:
                if ':0x' in part:
                    return part
        
        # If neither method works, use the full URL (less efficient)
        return url
    except Exception:
        return url


def parse_star_rating(star_text: Optional[str]) -> int:
    """Extract numeric rating from star text like '5 Sterne' or '1 Stern'.
    
    Args:
        star_text: Rating text from aria-label
        
    Returns:
        Numeric rating (0-5), or 0 if parsing fails
    """
    if not star_text:
        return 0
    
    # Extract the number from text like "5 Sterne" or "1 Stern"
    match = re.search(r'(\d+)', star_text)
    if match:
        rating = int(match.group(1))
        # Ensure rating is in valid range
        return max(0, min(5, rating))
    return 0


def detect_language(text: str) -> str:
    """Simple language detection based on common words.
    
    Args:
        text: Text to analyze
        
    Returns:
        Language code ('de', 'en', or 'unknown')
    """
    if not text:
        return "unknown"
        
    # Simple language detection based on common words
    german_words = ['und', 'der', 'die', 'das', 'ist', 'sehr', 'gut', 'für', 'mit', 'von', 'nicht']
    english_words = ['and', 'the', 'is', 'very', 'good', 'for', 'with', 'not', 'this', 'that']
    
    text_lower = text.lower()
    words = re.findall(r'\b\w+\b', text_lower)
    
    german_count = sum(1 for word in words if word in german_words)
    english_count = sum(1 for word in words if word in english_words)
    
    if german_count > english_count:
        return "de"
    elif english_count > 0:
        return "en"
    else:
        return "unknown"


def clean_website_url(website: str) -> str:
    """Clean and normalize website URL.
    
    Args:
        website: Raw website string
        
    Returns:
        Properly formatted URL
    """
    if not website:
        return ""
    
    website = website.strip()
    
    # Add https:// if no protocol specified
    if website and not website.startswith(('http://', 'https://')):
        website = f"https://{website}"
    
    return website


def clean_phone_number(phone: str) -> str:
    """Clean and normalize phone number.
    
    Args:
        phone: Raw phone number string
        
    Returns:
        Cleaned phone number
    """
    if not phone:
        return ""
    
    return phone.strip()


def parse_review_count(review_text: str) -> int:
    """Parse review count from text like '(123)' or '123 reviews'.
    
    Args:
        review_text: Text containing review count
        
    Returns:
        Numeric review count, or 0 if parsing fails
    """
    if not review_text:
        return 0
    
    # Remove parentheses and commas
    clean_text = review_text.replace('(', '').replace(')', '').replace(',', '').strip()
    
    # Extract first number found
    match = re.search(r'(\d+)', clean_text)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return 0
    
    return 0


def parse_rating_value(rating_text: str) -> float:
    """Parse rating value from text like '4,5 Sterne' or '4.5 stars'.
    
    Args:
        rating_text: Text containing rating value
        
    Returns:
        Numeric rating (0.0-5.0), or 0.0 if parsing fails
    """
    if not rating_text:
        return 0.0
    
    # Look for decimal number with comma or dot
    matches = re.search(r'(\d+[.,]\d+|\d+)', rating_text)
    if matches:
        try:
            rating_str = matches.group(1).replace(',', '.')
            rating = float(rating_str)
            # Ensure rating is in valid range
            return max(0.0, min(5.0, rating))
        except ValueError:
            return 0.0
    
    return 0.0


def clean_text(text: str) -> str:
    """Clean text by removing extra whitespace and newlines.
    
    Args:
        text: Raw text
        
    Returns:
        Cleaned text
    """
    if not text:
        return ""
    
    # Replace multiple whitespace with single space and strip
    cleaned = re.sub(r'\s+', ' ', text.strip())
    return cleaned


def is_valid_email(email: str) -> bool:
    """Check if email address is valid.
    
    Args:
        email: Email address to validate
        
    Returns:
        True if email appears valid
    """
    if not email:
        return False
    
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email.strip()))


def setup_retry_logger(name: str) -> logging.Logger:
    """Set up a logger with retry information.
    
    Args:
        name: Logger name
        
    Returns:
        Configured logger
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    
    return logger