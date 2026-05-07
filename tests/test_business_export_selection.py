from src.models import Business


def test_business_to_dict_blanks_unselected_extraction_groups():
    business = Business(
        place_id="p1",
        name="Cafe",
        website="https://example.com",
        phone="+49 123",
        review_count=12,
        review_average=4.6,
        deleted_review_count_min=1,
        deleted_review_count_max=3,
        deleted_review_notice="Removed reviews notice",
        place_type="Cafe",
        opens_at="Opens at 09:00",
        introduction="Specialty coffee",
        reply_rate_good=25.0,
        reviews_last_365d_min=4,
        website_status="reachable",
        website_modernity_score=75,
        website_modernity_reason="reachable, https, mobile_hint",
        export_contact_fields=False,
        export_business_details=False,
        export_review_summary=False,
        export_review_analytics=False,
        export_deleted_review_signals=False,
        export_website_modernity=False,
    )

    data = business.to_dict()

    assert data["Website"] == ""
    assert data["Phone Number"] == ""
    assert data["Review Count"] == ""
    assert data["Average Review"] == ""
    assert data["Deleted Review Notice"] == ""
    assert data["Type"] == ""
    assert data["Reply Rate Good (%)"] == ""
    assert data["Website Status"] == ""


def test_business_to_dict_exports_website_modernity_hints_when_enabled():
    business = Business(
        place_id="p1",
        name="Cafe",
        website_status="reachable",
        website_modernity_score=80,
        website_modernity_reason="reachable, https, mobile_hint",
        website_uses_https=True,
        website_mobile_friendly_hint=True,
        website_structured_data_hint=False,
        website_stale_or_broken_hint=False,
        export_website_modernity=True,
    )

    data = business.to_dict()

    assert data["Website Status"] == "reachable"
    assert data["Website Modernity Score"] == 80
    assert data["Website Uses HTTPS"] == "Yes"
    assert data["Website Structured Data Hint"] == "No"
