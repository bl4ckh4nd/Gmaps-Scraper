from src.utils.helpers import extract_place_id, extract_review_date_text, parse_review_count


def test_parse_review_count_prefers_review_total_over_rating_value():
    assert parse_review_count("4,5 Sterne 1.553 Rezensionen") == 1553


def test_parse_review_count_ignores_non_review_strings():
    assert parse_review_count("0,4 km entfernt") == 0


def test_extract_review_date_text_finds_relative_dates_inside_review_text():
    text = "Super Aufenthalt\nvor 8 Monaten\nTolles Personal und saubere Zimmer."
    assert extract_review_date_text(text) == "vor 8 Monaten"


def test_extract_review_date_text_finds_english_relative_dates():
    text = "Great stay\na year ago\nWould book again."
    assert extract_review_date_text(text) == "a year ago"


def test_extract_review_date_text_handles_hours_and_edited_prefix():
    text = "Bearbeitet: vor 3 Stunden auf Google\nNEU"
    assert extract_review_date_text(text) == "vor 3 Stunden"


def test_extract_place_id_strips_google_maps_query_parameters():
    url = (
        "https://www.google.com/maps/place/Test/data=!4m7!3m6!1s0x123:0x456!"
        "8m2!3d51.0!4d7.0!16s%2Fg%2F11abc!19sChIJTESTPLACEID?authuser=0&hl=de&rclk=1"
    )

    assert extract_place_id(url) == "ChIJTESTPLACEID"
