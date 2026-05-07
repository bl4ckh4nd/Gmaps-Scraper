import pandas as pd

from src.models import Review
from src.services import CategoryReportService
from src.utils.review_analyzer import analyze_reviews, inspect_review_date


def test_inspect_review_date_marks_one_year_bucket_ambiguous():
    info = inspect_review_date("vor einem Jahr")

    assert info["bucket"] == "one_year"
    assert info["ambiguous_one_year_bucket"] is True
    assert info["definitely_within_window"] is False
    assert info["older_than_one_year_bucket"] is False


def test_analyze_reviews_returns_recent_window_bounds_and_deleted_rates():
    reviews = [
        Review(place_id="p1", business_name="Cafe", business_address="Addr", rating=5, review_date="vor 2 Monaten"),
        Review(place_id="p1", business_name="Cafe", business_address="Addr", rating=4, review_date="vor 11 Monaten"),
        Review(place_id="p1", business_name="Cafe", business_address="Addr", rating=3, review_date="vor einem Jahr"),
        Review(place_id="p1", business_name="Cafe", business_address="Addr", rating=2, review_date="vor 2 Jahren"),
    ]

    metrics = analyze_reviews(
        reviews,
        collection_metadata={
            "coverage_status": "estimated",
            "oldest_review_date_text": "vor 2 Jahren",
        },
        deleted_review_bounds={"min": 2, "max": 5},
    )

    assert metrics["reviews_last_365d_min"] == 2
    assert metrics["reviews_last_365d_max"] == 3
    assert metrics["reviews_last_365d_mid"] == 2.5
    assert metrics["review_window_coverage_status"] == "estimated"
    assert metrics["review_window_cutoff_observed"] == "vor 2 Jahren"
    assert round(metrics["deleted_review_rate_min_pct"], 2) == 40.0
    assert round(metrics["deleted_review_rate_max_pct"], 2) == 71.43
    assert round(metrics["deleted_review_rate_mid_pct"], 2) == 58.33


def test_category_report_service_groups_by_query_and_type(tmp_path):
    csv_path = tmp_path / "result.csv"
    pd.DataFrame(
        [
            {
                "Place ID": "1",
                "Names": "Hotel One",
                "Search Query": "hotels in berlin",
                "Type": "Hotel",
                "Reviews Last 365d Min": 10,
                "Reviews Last 365d Mid": 12,
                "Reviews Last 365d Max": 14,
                "Deleted Review Rate Min (%)": 5,
                "Deleted Review Rate Mid (%)": 8,
                "Deleted Review Rate Max (%)": 10,
                "Review Window Coverage Status": "exact",
            },
            {
                "Place ID": "2",
                "Names": "Hotel Two",
                "Search Query": "hotels in berlin",
                "Type": "Hotel",
                "Reviews Last 365d Min": 20,
                "Reviews Last 365d Mid": 24,
                "Reviews Last 365d Max": 28,
                "Deleted Review Rate Min (%)": 6,
                "Deleted Review Rate Mid (%)": 9,
                "Deleted Review Rate Max (%)": 12,
                "Review Window Coverage Status": "estimated",
            },
            {
                "Place ID": "3",
                "Names": "Restaurant One",
                "Search Query": "restaurants in berlin",
                "Type": "Restaurant",
                "Reviews Last 365d Min": 30,
                "Reviews Last 365d Mid": 35,
                "Reviews Last 365d Max": 40,
                "Deleted Review Rate Min (%)": 2,
                "Deleted Review Rate Mid (%)": 3,
                "Deleted Review Rate Max (%)": 4,
                "Review Window Coverage Status": "exact",
            },
        ]
    ).to_csv(csv_path, index=False)

    service = CategoryReportService()
    output_path = service.build_summary(str(csv_path))

    assert output_path is not None

    summary = pd.read_csv(output_path).fillna(0)
    hotel_row = summary[summary["Search Query"] == "hotels in berlin"].iloc[0]

    assert int(hotel_row["business_count"]) == 2
    assert round(float(hotel_row["avg_reviews_last_365d_mid"]), 2) == 18.0
    assert round(float(hotel_row["avg_deleted_rate_mid_pct"]), 2) == 8.5
    assert int(hotel_row["exact"]) == 1
    assert int(hotel_row["estimated"]) == 1
