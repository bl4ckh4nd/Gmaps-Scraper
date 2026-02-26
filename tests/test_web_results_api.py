import io
import sys
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WEB_DIR = ROOT / "web"
if str(WEB_DIR) not in sys.path:
    sys.path.insert(0, str(WEB_DIR))

import app as web_app
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

