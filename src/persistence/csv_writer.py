"""CSV writer for business and review data."""

import os
import pandas as pd
import time
from pathlib import Path
from typing import List, Union, Optional
import logging

from ..models.business import Business
from ..models.review import Review
from ..utils.exceptions import PersistenceException


class CSVWriter:
    """Handles CSV file operations for business and review data."""
    
    def __init__(self, result_filename: str = 'result.csv', 
                 reviews_filename: str = 'reviews.csv'):
        """Initialize CSV writer with file paths.
        
        Args:
            result_filename: Filename for business data
            reviews_filename: Filename for review data
        """
        self.result_filename = result_filename
        self.reviews_filename = reviews_filename
        self.logger = logging.getLogger(__name__)
        self._review_file_checked = False
    
    def write_business(self, business: Business, check_duplicates: bool = True) -> bool:
        """Write a single business record to CSV.
        
        Args:
            business: Business instance to write
            check_duplicates: Whether to check for duplicates before writing
            
        Returns:
            True if record was written, False if it was a duplicate
            
        Raises:
            PersistenceException: If writing fails
        """
        try:
            data = business.to_dict()
            return self._append_to_csv(data, self.result_filename, 
                                     check_duplicates=check_duplicates)
        except Exception as e:
            raise PersistenceException(f"Failed to write business data: {e}") from e
    
    def write_businesses(self, businesses: List[Business], 
                        check_duplicates: bool = True) -> int:
        """Write multiple business records to CSV.
        
        Args:
            businesses: List of Business instances to write
            check_duplicates: Whether to check for duplicates before writing
            
        Returns:
            Number of records actually written (excluding duplicates)
            
        Raises:
            PersistenceException: If writing fails
        """
        written_count = 0
        for business in businesses:
            if self.write_business(business, check_duplicates):
                written_count += 1
        return written_count
    
    def write_review(self, review: Review) -> bool:
        """Write a single review to CSV.
        
        Args:
            review: Review instance to write
            
        Returns:
            True if review was written successfully
            
        Raises:
            PersistenceException: If writing fails
        """
        if not review.is_valid():
            self.logger.warning(f"Invalid review skipped: {review.place_id}")
            return False
            
        try:
            data = review.to_dict()
            return self._append_to_csv(data, self.reviews_filename, 
                                     check_duplicates=False)
        except Exception as e:
            raise PersistenceException(f"Failed to write review data: {e}") from e
    
    def write_reviews(self, reviews: List[Review]) -> int:
        """Write multiple reviews to CSV in batch.
        
        Args:
            reviews: List of Review instances to write
            
        Returns:
            Number of reviews actually written
            
        Raises:
            PersistenceException: If batch writing fails
        """
        if not reviews:
            return 0
            
        valid_reviews = [r for r in reviews if r.is_valid()]
        valid_reviews = self._deduplicate_review_batch(valid_reviews)
        if not valid_reviews:
            self.logger.warning("No valid reviews to write")
            return 0
        
        try:
            # Convert reviews to DataFrame for batch writing
            review_dicts = [review.to_dict() for review in valid_reviews]
            df = pd.DataFrame(review_dicts)
            expected_columns = list(review_dicts[0].keys())
            
            # Check if file exists to determine if header is needed
            file_exists = os.path.isfile(self.reviews_filename)
            if file_exists:
                self._ensure_existing_review_file_current(expected_columns)

            # Write to CSV, append if file exists
            df.to_csv(self.reviews_filename, mode='a', header=not file_exists,
                      index=False, encoding='utf-8-sig')

            self.logger.info(f"Wrote {len(valid_reviews)} reviews to {self.reviews_filename}")
            return len(valid_reviews)
            
        except Exception as e:
            raise PersistenceException(f"Failed to batch write reviews: {e}") from e
    
    def _append_to_csv(self, new_data: dict, filename: str, 
                      check_duplicates: bool = True) -> bool:
        """Append new data to CSV file with optional duplicate checking.
        
        Args:
            new_data: Dictionary of data to append
            filename: CSV filename
            check_duplicates: Whether to check for duplicates
            
        Returns:
            True if data was written, False if it was a duplicate
            
        Raises:
            PersistenceException: If file operations fail
        """
        try:
            expected_columns = list(new_data.keys())

            # Check if file exists
            if not os.path.exists(filename):
                df = pd.DataFrame([new_data], columns=expected_columns)
                df.to_csv(filename, index=False)
                return True

            existing_df = pd.read_csv(filename)

            # Ensure legacy files are upgraded to the new schema before appending
            existing_df = self._ensure_business_schema(existing_df, expected_columns, filename)
            existing_df = self._ensure_review_schema(existing_df, expected_columns, filename)

            if check_duplicates and len(existing_df) > 0:
                if 'Names' in new_data and 'Address' in new_data:
                    duplicate = existing_df[
                        (existing_df.get('Names') == new_data['Names']) &
                        (existing_df.get('Address') == new_data['Address'])
                    ]
                    if len(duplicate) > 0:
                        self.logger.info(f"Skipping duplicate: {new_data.get('Names', 'Unknown')}")
                        return False

                elif 'place_id' in new_data and 'reviewer_name' in new_data:
                    if 'review_hash' in new_data and 'review_hash' in existing_df.columns:
                        duplicate = existing_df[
                            (existing_df.get('place_id') == new_data['place_id']) &
                            (existing_df.get('review_hash') == new_data['review_hash'])
                        ]
                    else:
                        duplicate = existing_df[
                            (existing_df.get('place_id') == new_data['place_id']) &
                            (existing_df.get('reviewer_name') == new_data['reviewer_name']) &
                            (existing_df.get('review_text') == new_data['review_text'])
                        ]
                    if len(duplicate) > 0:
                        return False

            # Append to existing file with aligned header order
            df = pd.DataFrame([new_data], columns=expected_columns)
            df.to_csv(filename, mode='a', header=False, index=False)
            return True

        except Exception as e:
            raise PersistenceException(f"Failed to append to {filename}: {e}") from e

    def _ensure_business_schema(self, df: pd.DataFrame, expected_columns: list[str], filename: str) -> pd.DataFrame:
        """Upgrade legacy CSV files to match the current business schema."""

        if df.columns.tolist() == expected_columns:
            return df

        if 'Owner Name' not in expected_columns:
            return df

        upgraded_df = df.reindex(columns=expected_columns, fill_value="")
        try:
            upgraded_df.to_csv(filename, index=False)
            self.logger.info("Upgraded business CSV schema to match the current output columns")
        except Exception as exc:
            raise PersistenceException(f"Failed to upgrade business CSV schema: {exc}") from exc

        return upgraded_df

    def _ensure_review_schema(self, df: pd.DataFrame, expected_columns: list[str], filename: str) -> pd.DataFrame:
        """Upgrade legacy review CSV files to match the current review schema."""
        if 'review_hash' not in expected_columns:
            return df

        upgraded_df = df.copy()
        if 'review_hash' not in upgraded_df.columns:
            upgraded_df['review_hash'] = ""

        recomputed_hashes = upgraded_df.apply(
            lambda row: Review.from_dict(row.to_dict()).review_hash,
            axis=1,
        )
        existing_hashes = upgraded_df['review_hash'].fillna("").astype(str)
        needs_rewrite = (
            df.columns.tolist() != expected_columns
            or not existing_hashes.equals(recomputed_hashes)
        )

        upgraded_df['review_hash'] = recomputed_hashes
        upgraded_df = upgraded_df.reindex(columns=expected_columns, fill_value="")
        if not needs_rewrite:
            return upgraded_df

        try:
            upgraded_df.to_csv(filename, index=False)
            self.logger.info("Upgraded review CSV schema and refreshed review hashes")
        except Exception as exc:
            raise PersistenceException(f"Failed to upgrade review CSV schema: {exc}") from exc

        return upgraded_df

    def _ensure_existing_review_file_current(self, expected_columns: list[str]) -> None:
        """Refresh an existing review CSV to the current schema/hash format once per writer."""
        if self._review_file_checked or not os.path.exists(self.reviews_filename):
            return

        existing_df = pd.read_csv(self.reviews_filename).fillna("")
        self._ensure_review_schema(existing_df, expected_columns, self.reviews_filename)
        self._review_file_checked = True

    @staticmethod
    def _deduplicate_review_batch(reviews: List[Review]) -> List[Review]:
        seen_hashes = set()
        deduplicated: List[Review] = []

        for review in reviews:
            review_hash = review.review_hash or Review.build_review_hash(
                review.place_id,
                review.reviewer_name,
                review.rating,
                review.review_text,
            )
            if review_hash in seen_hashes:
                continue
            seen_hashes.add(review_hash)
            deduplicated.append(review)

        return deduplicated
    
    def backup_files(self) -> tuple[Optional[str], Optional[str]]:
        """Create timestamped backups of existing CSV files.
        
        Returns:
            Tuple of (business_backup_filename, reviews_backup_filename)
        """
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        business_backup = None
        reviews_backup = None
        
        try:
            if os.path.exists(self.result_filename):
                business_backup = f'result_{timestamp}.csv'
                os.rename(self.result_filename, business_backup)
                self.logger.info(f"Backed up business data to {business_backup}")
            
            if os.path.exists(self.reviews_filename):
                reviews_backup = f'reviews_{timestamp}.csv'
                os.rename(self.reviews_filename, reviews_backup)
                self.logger.info(f"Backed up reviews data to {reviews_backup}")
                
        except Exception as e:
            self.logger.error(f"Failed to create backups: {e}")
        
        return business_backup, reviews_backup
    
    def deduplicate_business_data(self) -> int:
        """Remove duplicate entries from business CSV file.
        
        Returns:
            Number of duplicates removed
            
        Raises:
            PersistenceException: If deduplication fails
        """
        if not os.path.exists(self.result_filename):
            return 0
            
        try:
            df = pd.read_csv(self.result_filename)
            original_count = len(df)
            
            # Remove duplicates based on name and address
            df = df.drop_duplicates(subset=['Names', 'Address'])
            
            # Save deduplicated data
            df.to_csv(self.result_filename, index=False)
            
            duplicates_removed = original_count - len(df)
            if duplicates_removed > 0:
                self.logger.info(f"Removed {duplicates_removed} duplicates from business data")
            
            return duplicates_removed
            
        except Exception as e:
            raise PersistenceException(f"Failed to deduplicate business data: {e}") from e
    
    def get_business_count(self) -> int:
        """Get the current count of business records.
        
        Returns:
            Number of business records in CSV file
        """
        if not os.path.exists(self.result_filename):
            return 0
            
        try:
            df = pd.read_csv(self.result_filename)
            return len(df)
        except Exception:
            return 0
    
    def get_review_count(self) -> int:
        """Get the current count of review records.
        
        Returns:
            Number of review records in CSV file
        """
        if not os.path.exists(self.reviews_filename):
            return 0
            
        try:
            df = pd.read_csv(self.reviews_filename)
            return len(df)
        except Exception:
            return 0
