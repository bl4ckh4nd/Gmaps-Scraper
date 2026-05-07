import json

from src.models import Business
from src.utils.deleted_review_extraction import (
    DeletedReviewBulkNormalizer,
    DeletedReviewInput,
    extract_deleted_review_notice_text,
    parse_deleted_review_notice,
)


class DummyDeletedReviewClient:
    def __init__(self, response):
        self.default_model = "dummy/model:free"
        self.response = response
        self.last_messages = None
        self.last_response_format = None
        self.last_model = None

    def create_chat_completion_sync(self, messages, model=None, response_format=None, temperature=None):
        self.last_messages = list(messages)
        self.last_response_format = response_format
        self.last_model = model
        return self.response


def test_parse_deleted_review_notice_parses_german_range():
    notice = parse_deleted_review_notice(
        "Sechs bis zehn Bewertungen aufgrund von Beschwerden wegen Diffamierung entfernt."
    )
    assert notice.min_count == 6
    assert notice.max_count == 10
    assert "Diffamierung" in notice.raw_text


def test_parse_deleted_review_notice_parses_numeric_range():
    notice = parse_deleted_review_notice("21-50 Bewertungen wurden entfernt.")
    assert notice.min_count == 21
    assert notice.max_count == 50


def test_parse_deleted_review_notice_parses_more_than_phrase():
    notice = parse_deleted_review_notice("More than 100 reviews removed.")
    assert notice.min_count == 101
    assert notice.max_count is None


def test_extract_deleted_review_notice_text_rejects_generic_google_disclosure():
    text = (
        "Rezensionen werden zwar nicht von Google überprüft, Google sucht aber gezielt "
        "nach gefälschten Inhalten und entfernt diese."
    )
    assert extract_deleted_review_notice_text(text) == ""


def test_extract_deleted_review_notice_text_requires_legal_context():
    text = (
        "In den letzten 365 Tagen wurden 11 bis 20 Rezensionen von diesem Profil "
        "aufgrund von Beschwerden wegen Diffamierung nach deutschem Recht entfernt."
    )
    assert extract_deleted_review_notice_text(text) == text


def test_extract_deleted_review_notice_text_prefers_specific_notice_over_generic_disclosure():
    text = (
        "Rezensionen werden von Google nicht überprüft. Google sucht jedoch gezielt nach "
        "gefälschten Inhalten und entfernt diese.\n"
        "Sechs bis zehn Bewertungen aufgrund von Beschwerden wegen Diffamierung entfernt."
    )
    assert extract_deleted_review_notice_text(text) == (
        "Sechs bis zehn Bewertungen aufgrund von Beschwerden wegen Diffamierung entfernt."
    )


def test_business_serialization_includes_deleted_review_fields():
    business = Business(
        place_id="pid-1",
        name="Cafe",
        deleted_review_count_min=6,
        deleted_review_count_max=10,
        deleted_review_notice="Sechs bis zehn Bewertungen entfernt.",
    )

    data = business.to_dict()

    assert data["Deleted Review Count Min"] == 6
    assert data["Deleted Review Count Max"] == 10
    assert data["Deleted Review Notice"] == "Sechs bis zehn Bewertungen entfernt."


def test_deleted_review_bulk_normalizer_returns_place_id_keyed_json():
    response = {
        "choices": [
            {
                "message": {
                    "parsed": {
                        "place-1": {
                            "deleted_review_count_min": 6,
                            "deleted_review_count_max": 10,
                            "deleted_review_notice": "Sechs bis zehn Bewertungen entfernt.",
                            "source": "llm",
                        },
                        "place-2": {
                            "deleted_review_count": 21,
                            "deleted_review_notice": "21-50 removed.",
                        },
                    }
                }
            }
        ]
    }
    client = DummyDeletedReviewClient(response)
    normalizer = DeletedReviewBulkNormalizer(client)

    result = normalizer.normalize(
        [
            DeletedReviewInput(place_id="place-1", business_name="One", notice_text="irrelevant"),
            DeletedReviewInput(place_id="place-2", business_name="Two", notice_text="irrelevant"),
        ]
    )

    assert client.last_response_format == "json_object"
    assert "place_id" in client.last_messages[1]["content"]
    assert result["place-1"].min_count == 6
    assert result["place-1"].max_count == 10
    assert result["place-2"].min_count == 21
    assert result["place-2"].max_count == 21


def test_deleted_review_bulk_normalizer_builds_structured_prompt():
    client = DummyDeletedReviewClient(
        {"choices": [{"message": {"content": json.dumps({"place-1": {"count": 6}})}}]}
    )
    normalizer = DeletedReviewBulkNormalizer(client)

    normalizer.normalize(
        [DeletedReviewInput(place_id="place-1", business_name="One", notice_text="Sechs bis zehn")]
    )

    prompt = client.last_messages[1]["content"]
    assert "place-1" in prompt
    assert "Sechs bis zehn" in prompt
