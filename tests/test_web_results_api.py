import io
import sys
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WEB_DIR = ROOT / "web"
if str(WEB_DIR) not in sys.path:
    sys.path.insert(0, str(WEB_DIR))

import app as web_app
import scraper_service
from scraper_service import JobConfig, JobStatus


def _make_job(job_id: str, status: str, search_term: str = "coffee") -> JobStatus:
    return JobStatus(
        job_id=job_id,
        status=status,
        config=JobConfig(search_term=search_term, total_results=25, job_type='scrape'),
        progress={'current': 0, 'total': 25, 'percentage': 0},
        start_time="2026-02-25T12:00:00",
        end_time="2026-02-25T12:10:00" if status == "completed" else None,
    )


def test_results_metadata_includes_row_count_and_download_url(tmp_path, monkeypatch):
    job = _make_job("job-meta", "completed")
    business_csv = tmp_path / "business.csv"
    business_csv.write_text("name,address\nA,Street 1\nB,Street 2\n", encoding="utf-8")

    monkeypatch.setattr(web_app.scraper_manager, "get_job_status", lambda job_id: job)
    monkeypatch.setattr(
        web_app.scraper_manager,
        "get_job_results",
        lambda job_id: {"business_data": str(business_csv), "reviews_data": None, "log_file": None},
    )

    client = web_app.app.test_client()
    response = client.get("/api/jobs/job-meta/results")
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["files"]["business_data"]["row_count"] == 2
    assert payload["files"]["business_data"]["download_url"] == "/api/jobs/job-meta/download/business_data"


def test_preview_endpoint_returns_paginated_rows(tmp_path, monkeypatch):
    job = _make_job("job-preview", "completed")
    business_csv = tmp_path / "business.csv"
    business_csv.write_text(
        "name,address\nA,Street 1\nB,Street 2\nC,Street 3\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(web_app.scraper_manager, "get_job_status", lambda job_id: job)
    monkeypatch.setattr(
        web_app.scraper_manager,
        "get_job_results",
        lambda job_id: {"business_data": str(business_csv), "reviews_data": None, "log_file": None},
    )

    client = web_app.app.test_client()
    response = client.get("/api/jobs/job-preview/preview/business_data?limit=2&offset=1")
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["columns"] == ["name", "address"]
    assert len(payload["rows"]) == 2
    assert payload["rows"][0]["name"] == "B"
    assert payload["total_rows"] == 3
    assert payload["has_more"] is False


def test_download_all_returns_zip_archive(tmp_path, monkeypatch):
    job = _make_job("job-zip", "completed")
    business_csv = tmp_path / "business.csv"
    reviews_csv = tmp_path / "reviews.csv"
    log_file = tmp_path / "scraper.log"
    business_csv.write_text("name\nA\n", encoding="utf-8")
    reviews_csv.write_text("review\nGreat\n", encoding="utf-8")
    log_file.write_text("ok", encoding="utf-8")

    monkeypatch.setattr(web_app.scraper_manager, "get_job_status", lambda job_id: job)
    monkeypatch.setattr(
        web_app.scraper_manager,
        "get_job_results",
        lambda job_id: {
            "business_data": str(business_csv),
            "reviews_data": str(reviews_csv),
            "log_file": str(log_file),
        },
    )

    client = web_app.app.test_client()
    response = client.get("/api/jobs/job-zip/download/all")
    assert response.status_code == 200
    assert response.mimetype == "application/zip"

    archive = zipfile.ZipFile(io.BytesIO(response.data))
    names = set(archive.namelist())
    assert "job-zip_business_data.csv" in names
    assert "job-zip_reviews_data.csv" in names
    assert "job-zip_log_file.log" in names


def test_list_jobs_supports_server_side_filter_and_pagination(monkeypatch):
    jobs = [
        _make_job("job-completed", "completed", "coffee"),
        _make_job("job-failed", "failed", "bakery"),
        _make_job("job-running", "running", "pizza"),
    ]
    monkeypatch.setattr(web_app.scraper_manager, "list_jobs", lambda limit=None: jobs)

    client = web_app.app.test_client()
    response = client.get("/api/jobs?status=completed,failed&limit=1&page=2")
    assert response.status_code == 200
    payload = response.get_json()

    assert payload["total"] == 2
    assert payload["page"] == 2
    assert payload["limit"] == 1
    assert payload["has_more"] is False
    assert len(payload["jobs"]) == 1
    assert payload["jobs"][0]["status"] == "failed"


def test_validate_job_config_accepts_extraction_overrides():
    job_config, error = web_app.validate_job_config({
        "search_term": "coffee",
        "total_results": 25,
        "config_overrides": {
            "extraction": {
                "contact_fields": True,
                "review_rows": False,
                "website_modernity": True,
            }
        },
    })

    assert error is None
    assert job_config is not None
    assert job_config.config_overrides["extraction"]["website_modernity"] is True


def test_validate_job_config_rejects_non_boolean_extraction_override():
    job_config, error = web_app.validate_job_config({
        "search_term": "coffee",
        "total_results": 25,
        "config_overrides": {
            "extraction": {
                "review_rows": "yes",
            }
        },
    })

    assert job_config is None
    assert error == "extraction.review_rows must be a boolean"


def test_scraper_manager_uses_postgres_runner_mode(monkeypatch):
    monkeypatch.setenv("SCRAPER_USE_POSTGRES_RUNNER", "1")
    manager = scraper_service.ScraperManager()
    called = {}

    def _start(config):
        called["config"] = config
        return "job-postgres"

    monkeypatch.setattr(manager, "_start_orchestrated_scrape_job", _start)

    job_id = manager.start_job(JobConfig(search_term="coffee", total_results=25, job_type="scrape"))

    assert job_id == "job-postgres"
    assert called["config"].search_term == "coffee"


def test_operations_endpoint_returns_manager_payload(monkeypatch):
    job = _make_job("job-ops", "failed")
    job.available_actions = ["retry", "restart_from_scratch"]
    monkeypatch.setattr(web_app.scraper_manager, "get_job_status", lambda job_id: job)
    monkeypatch.setattr(
        web_app.scraper_manager,
        "get_job_operations",
        lambda job_id: {
            "backend": "postgres_runner",
            "available_actions": ["retry", "restart_from_scratch"],
            "events": [{"event_type": "job_failed", "message": "boom"}],
            "artifacts": [],
            "checkpoint": {"payload": {"results_count": 10}},
            "session": {"runner_id": "runner-1", "status": "failed"},
            "runner": {"runner_id": "runner-1"},
        },
    )

    client = web_app.app.test_client()
    response = client.get("/api/jobs/job-ops/operations")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["backend"] == "postgres_runner"
    assert payload["available_actions"] == ["retry", "restart_from_scratch"]
    assert payload["session"]["runner_id"] == "runner-1"


def test_job_action_endpoint_executes_manager_action(monkeypatch):
    monkeypatch.setattr(
        web_app.scraper_manager,
        "execute_job_action",
        lambda job_id, action: {"job_id": job_id, "status": "queued", "action": action},
    )

    client = web_app.app.test_client()
    response = client.post("/api/jobs/job-ops/actions", json={"action": "retry"})

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["job_id"] == "job-ops"
    assert payload["action"] == "retry"


def test_list_jobs_supports_orchestrator_status_filter(monkeypatch):
    waiting_job = _make_job("job-waiting", "waiting_for_slot", "coffee")
    failed_job = _make_job("job-failed", "failed", "bakery")
    monkeypatch.setattr(web_app.scraper_manager, "list_jobs", lambda limit=None: [waiting_job, failed_job])

    client = web_app.app.test_client()
    response = client.get("/api/jobs?status=waiting_for_slot")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["total"] == 1
    assert payload["jobs"][0]["status"] == "waiting_for_slot"
