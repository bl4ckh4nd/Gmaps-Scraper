import datetime
import json
import csv

import pandas as pd
import pytest

from src.config import Config, OwnerEnrichmentSettings
from src.models import OwnerDocument, OwnerCrawlResult, OwnerDetails, Business
from src.persistence.csv_writer import CSVWriter
from src.scraper.adaptive_owner_enricher import AdaptiveOwnerEnricher
from src.utils.openrouter_client import extract_owner_name_from_response, filter_free_models
from src.utils.owner_enrichment_service import OwnerEnrichmentService
from src.utils.text_filters import extract_owner_snippets
from src.services import OwnerCSVEnricher, OwnerCSVEnrichmentOptions


class DummyClient:
    def __init__(self, owner_name: str):
        self.default_model = "dummy/model:free"
        self._owner_name = owner_name
        self.last_messages = None
        self.last_response_format = None

    def create_chat_completion_sync(self, messages, response_format=None):
        self.last_messages = list(messages)
        self.last_response_format = response_format
        return {
            "choices": [
                {
                    "message": {
                        "parsed": {"owner_name": self._owner_name},
                    }
                }
            ]
        }


class DummyEnricher:
    def __init__(self, documents):
        self._documents = documents

    def crawl_owner_content_sync(self, website_url):
        return OwnerCrawlResult(status="documents_found", documents=self._documents)


def test_owner_details_serialization_sets_timestamp():
    details = OwnerDetails.from_response(
        "Jane Doe",
        status="owner_found",
        confidence=0.85,
        source_url="https://example.com/imprint",
        llm_model="dummy/model:free",
    )
    assert details.owner_name == "Jane Doe"
    assert details.status == "owner_found"
    assert isinstance(details.last_checked, datetime.datetime)


def test_extract_owner_snippets_prioritises_keywords():
    docs = [
        OwnerDocument(url="https://example.com/imprint", content="Owner: Jane Doe"),
        OwnerDocument(url="https://example.com/about", content="We love coffee"),
    ]
    snippet = extract_owner_snippets(docs)
    assert "Jane Doe" in snippet
    assert "coffee" not in snippet


def test_adaptive_owner_enricher_runs_local_crawler():
    captured_options = {}

    class DummyCrawler:
        def crawl(self, **kwargs):
            captured_options.update(kwargs)
            return {
                "documents": [
                    {
                        "url": "https://example.com/imprint",
                        "content": "Owner: Jane Doe",
                        "confidence": 0.9,
                    }
                ],
                "metadata": {"pages": 2},
            }

    settings = OwnerEnrichmentSettings(max_pages=5, max_depth=3)
    enricher = AdaptiveOwnerEnricher(settings, crawler_cls=DummyCrawler)
    result = enricher.crawl_owner_content_sync("https://example.com")

    assert result.status == "documents_found"
    assert result.documents[0].url == "https://example.com/imprint"
    assert captured_options["max_pages"] == 5
    assert captured_options["max_depth"] == 3
    assert captured_options["queries"] == list(settings.query_terms)


def test_adaptive_owner_enricher_extract_documents_handles_metadata():
    settings = OwnerEnrichmentSettings()
    enricher = AdaptiveOwnerEnricher(settings, crawler_cls=lambda: None)  # type: ignore[arg-type]
    response = {
        "documents": [
            {
                "url": "https://example.com/imprint",
                "content": "Owner: Jane Doe",
                "confidence": 0.92,
            }
        ]
    }
    docs = list(enricher._extract_documents(response))
    assert len(docs) == 1
    assert docs[0].url == "https://example.com/imprint"
    assert "Jane Doe" in docs[0].content


def test_owner_enrichment_service_disabled_returns_disabled_status():
    settings = OwnerEnrichmentSettings(enabled=False)
    service = OwnerEnrichmentService(settings)
    business = Business(place_id="1", name="Test Biz")
    details = service.enrich_business(business)
    assert details.status == "disabled"


def test_owner_enrichment_service_missing_website_returns_no_website():
    settings = OwnerEnrichmentSettings(enabled=True)
    service = OwnerEnrichmentService(settings)
    service._adaptive_enricher = DummyEnricher([])
    service._get_openrouter_client = lambda: DummyClient("Jane Doe")
    business = Business(place_id="1", name="Test Biz")
    details = service.enrich_business(business)
    assert details.status == "no_website"


def test_owner_enrichment_service_success_flow():
    settings = OwnerEnrichmentSettings(enabled=True)
    service = OwnerEnrichmentService(settings)
    documents = [
        OwnerDocument(
            url="https://example.com/imprint",
            content="Owner: Jane Doe",
            confidence=0.93,
        )
    ]
    service._adaptive_enricher = DummyEnricher(documents)
    client = DummyClient("Jane Doe")
    service._get_openrouter_client = lambda: client
    business = Business(place_id="1", name="Test Biz", website="https://example.com")
    details = service.enrich_business(business)
    assert details.status == "owner_found"
    assert details.owner_name == "Jane Doe"
    assert details.source_url == "https://example.com/imprint"
    assert client.last_response_format == settings.llm_response_format
    assert client.last_messages is not None
    system_msg = client.last_messages[0]["content"]
    user_msg = client.last_messages[1]["content"]
    assert "JSON" in system_msg
    assert "owner_name" in user_msg


def test_extract_owner_name_from_response_handles_missing():
    assert extract_owner_name_from_response({}) is None
    assert extract_owner_name_from_response({"choices": []}) is None


def test_extract_owner_name_from_response_parses_json_content():
    response = {
        "choices": [
            {
                "message": {
                    "content": '{"owner_name": "Jane Doe"}',
                }
            }
        ]
    }
    assert extract_owner_name_from_response(response) == "Jane Doe"


def test_extract_owner_name_from_response_ignores_non_owner_json():
    response = {
        "choices": [
            {
                "message": {
                    "content": '{"result": "no owner listed"}',
                }
            }
        ]
    }
    assert extract_owner_name_from_response(response) is None


def test_filter_free_models_returns_free_entries():
    models = [
        {"id": "model-a:free", "pricing": {"prompt": 0, "completion": 0}},
        {"id": "model-b", "pricing": {"prompt": 0.001, "completion": 0.001}},
    ]
    filtered = filter_free_models(models)
    assert len(filtered) == 1
    assert filtered[0]["id"] == "model-a:free"


def test_csv_writer_upgrades_legacy_schema(tmp_path):
    legacy_columns = [
        'Place ID',
        'Names',
        'Address',
        'Website',
        'Phone Number',
        'Review Count',
        'Average Review',
        'Store Shopping',
        'In Store Pickup',
        'Delivery',
        'Type',
        'Opens At',
        'Introduction',
        'Maps URL',
        'Reply Rate Good (%)',
        'Reply Rate Bad (%)',
        'Avg Days Between Reviews',
    ]

    legacy_data = {
        'Place ID': 'pid-1',
        'Names': 'Legacy Shop',
        'Address': 'Example Street',
        'Website': '',
        'Phone Number': '',
        'Review Count': 0,
        'Average Review': 0.0,
        'Store Shopping': 'No',
        'In Store Pickup': 'No',
        'Delivery': 'No',
        'Type': '',
        'Opens At': '',
        'Introduction': '',
        'Maps URL': '',
        'Reply Rate Good (%)': 0.0,
        'Reply Rate Bad (%)': 0.0,
        'Avg Days Between Reviews': '',
    }

    output_path = tmp_path / 'result.csv'
    pd.DataFrame([legacy_data], columns=legacy_columns).to_csv(output_path, index=False)

    writer = CSVWriter(result_filename=str(output_path))
    business = Business(place_id='pid-2', name='New Shop', address='Another St')
    business.owner_details = OwnerDetails.from_response(
        'Jane Doe', status='owner_found', source_url='https://example.com/imprint'
    )

    write_result = writer.write_business(business)
    assert write_result is True

    upgraded_df = pd.read_csv(output_path)
    assert 'Owner Name' in upgraded_df.columns
    assert upgraded_df.iloc[0]['Owner Name'] == ''
    assert upgraded_df.iloc[1]['Owner Name'] == 'Jane Doe'


class DummyOwnerService:
    def __init__(self, owner_name: str = "Ada Lovelace"):
        self.owner_name = owner_name
        self.calls = 0

    def enrich_business(self, business: Business) -> OwnerDetails:
        self.calls += 1
        return OwnerDetails.from_response(
            self.owner_name,
            status="owner_found",
            source_url=business.website or "https://example.com",
            confidence=0.95,
        )


def test_owner_csv_enricher_enriches_rows(tmp_path):
    csv_path = tmp_path / 'result.csv'
    rows = [
        {
            'Place ID': 'pid-1',
            'Names': 'Coffee Hub',
            'Address': 'Main Street 1',
            'Website': 'https://coffeehub.example',
            'Phone Number': '',
            'Review Count': 0,
            'Average Review': 0.0,
            'Store Shopping': 'No',
            'In Store Pickup': 'No',
            'Delivery': 'No',
            'Type': '',
            'Opens At': '',
            'Introduction': '',
            'Maps URL': '',
            'Reply Rate Good (%)': 0.0,
            'Reply Rate Bad (%)': 0.0,
            'Avg Days Between Reviews': '',
        }
    ]
    pd.DataFrame(rows).to_csv(csv_path, index=False)

    config = Config()
    enricher = OwnerCSVEnricher(config)
    enricher._owner_service = DummyOwnerService()

    options = OwnerCSVEnrichmentOptions(
        input_path=csv_path,
        output_path=tmp_path / 'owner_enriched.csv',
        skip_existing=False,
    )

    result = enricher.enrich(options)

    enriched_df = pd.read_csv(result.output_path)
    assert enriched_df.iloc[0]['Owner Name'] == 'Ada Lovelace'
    assert result.owners_found == 1
    assert enricher._owner_service.calls == 1


def test_owner_enrichment_service_prefers_source_matching_owner_name():
    settings = OwnerEnrichmentSettings(enabled=True)
    service = OwnerEnrichmentService(settings)
    documents = [
        OwnerDocument(
            url="https://example.com/imprint",
            content="Owner: Jane Doe\nManaging Director: Jane Doe",
            confidence=0.40,
        ),
        OwnerDocument(
            url="https://example.com/home",
            content="Welcome to our homepage",
            confidence=0.99,
        ),
    ]
    service._adaptive_enricher = DummyEnricher(documents)
    service._get_openrouter_client = lambda: DummyClient("Jane Doe")
    business = Business(place_id="1", name="Test Biz", website="https://example.com")

    details = service.enrich_business(business)

    assert details.status == "owner_found"
    assert details.source_url == "https://example.com/imprint"


def test_owner_csv_enricher_rejects_in_place_resume(tmp_path):
    csv_path = tmp_path / "result.csv"
    pd.DataFrame([{"Place ID": "pid-1", "Names": "Biz", "Address": "", "Website": ""}]).to_csv(
        csv_path,
        index=False,
    )

    enricher = OwnerCSVEnricher(Config())
    options = OwnerCSVEnrichmentOptions(
        input_path=csv_path,
        in_place=True,
        resume=True,
    )

    with pytest.raises(ValueError, match="cannot be resumed safely"):
        enricher.enrich(options)


def test_owner_csv_enricher_resume_legacy_state_uses_row_checkpoint(tmp_path):
    input_path = tmp_path / "input.csv"
    output_path = tmp_path / "output.csv"
    state_path = tmp_path / "output.csv.state.json"

    rows = [
        {
            "Place ID": "pid-1",
            "Names": "Coffee Hub",
            "Address": "Main Street 1",
            "Website": "https://coffeehub.example",
            "Phone Number": "",
            "Review Count": 0,
            "Average Review": 0.0,
            "Store Shopping": "No",
            "In Store Pickup": "No",
            "Delivery": "No",
            "Type": "",
            "Opens At": "",
            "Introduction": "",
            "Maps URL": "",
            "Reply Rate Good (%)": 0.0,
            "Reply Rate Bad (%)": 0.0,
            "Avg Days Between Reviews": "",
        },
        {
            "Place ID": "pid-1",
            "Names": "Coffee Hub",
            "Address": "Main Street 1",
            "Website": "https://coffeehub.example",
            "Phone Number": "",
            "Review Count": 0,
            "Average Review": 0.0,
            "Store Shopping": "No",
            "In Store Pickup": "No",
            "Delivery": "No",
            "Type": "",
            "Opens At": "",
            "Introduction": "",
            "Maps URL": "",
            "Reply Rate Good (%)": 0.0,
            "Reply Rate Bad (%)": 0.0,
            "Avg Days Between Reviews": "",
        },
        {
            "Place ID": "pid-3",
            "Names": "Bakery Point",
            "Address": "Second Street 2",
            "Website": "https://bakery.example",
            "Phone Number": "",
            "Review Count": 0,
            "Average Review": 0.0,
            "Store Shopping": "No",
            "In Store Pickup": "No",
            "Delivery": "No",
            "Type": "",
            "Opens At": "",
            "Introduction": "",
            "Maps URL": "",
            "Reply Rate Good (%)": 0.0,
            "Reply Rate Bad (%)": 0.0,
            "Avg Days Between Reviews": "",
        },
    ]
    pd.DataFrame(rows).to_csv(input_path, index=False)

    # Simulate an interrupted prior run with one already-written output row.
    first_business = Business.from_dict(rows[0])
    first_business.owner_details = OwnerDetails.from_response(
        "Ada Lovelace",
        status="owner_found",
        source_url="https://coffeehub.example",
    )
    with output_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(first_business.to_dict().keys()))
        writer.writeheader()
        writer.writerow(first_business.to_dict())

    # Legacy state format from older signature-based implementation.
    state_path.write_text(json.dumps({"processed": ["legacy-signature"]}), encoding="utf-8")

    config = Config()
    enricher = OwnerCSVEnricher(config)
    enricher._owner_service = DummyOwnerService(owner_name="Grace Hopper")
    options = OwnerCSVEnrichmentOptions(
        input_path=input_path,
        output_path=output_path,
        resume=True,
        state_path=state_path,
        skip_existing=False,
    )

    result = enricher.enrich(options)

    assert result.total_rows == 3
    assert result.processed_rows == 2
    assert enricher._owner_service.calls == 2
    assert not state_path.exists()

    enriched_df = pd.read_csv(output_path)
    assert len(enriched_df) == 3
