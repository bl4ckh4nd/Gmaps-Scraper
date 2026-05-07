from src.utils.website_quality import assess_website_quality


class _FakeResponse:
    def __init__(self, *, url: str, text: str, status_code: int = 200):
        self.url = url
        self.text = text
        self.status_code = status_code
        self.headers = {"content-type": "text/html; charset=utf-8"}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"bad status {self.status_code}")


class _FakeClient:
    def __init__(self, response):
        self._response = response

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def get(self, url):
        return self._response


def test_assess_website_quality_reports_missing_website():
    assessment = assess_website_quality("")

    assert assessment.status == "missing"
    assert assessment.modernity_score is None


def test_assess_website_quality_scores_modern_reachable_site(monkeypatch):
    response = _FakeResponse(
        url="https://example.com",
        text="""
            <html>
              <head>
                <meta name="viewport" content="width=device-width, initial-scale=1">
                <script type="application/ld+json">{"@context":"https://schema.org"}</script>
              </head>
              <body>Hello</body>
            </html>
        """,
    )
    monkeypatch.setattr(
        "src.utils.website_quality.httpx.Client",
        lambda **kwargs: _FakeClient(response),
    )

    assessment = assess_website_quality("example.com")

    assert assessment.status == "reachable"
    assert assessment.uses_https is True
    assert assessment.mobile_friendly_hint is True
    assert assessment.structured_data_hint is True
    assert assessment.stale_or_broken_hint is False
    assert assessment.modernity_score == 100
