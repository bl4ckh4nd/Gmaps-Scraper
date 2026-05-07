from src.navigation import GridNavigator
from src.persistence.orchestrator_store import ORCHESTRATOR_SCHEMA_SQL
from src.persistence.postgres_store import SCHEMA_SQL
from src.services.orchestration import OrchestrationConfig
from src.services.queue_service import QueueConfig


def test_queue_config_uses_environment_defaults(monkeypatch):
    monkeypatch.setenv("REDIS_URL", "redis://example:6379/2")
    monkeypatch.setenv("SCRAPER_DISCOVERY_QUEUE", "discover")
    monkeypatch.setenv("SCRAPER_LISTING_QUEUE", "listing")
    monkeypatch.setenv("SCRAPER_JOB_TIMEOUT_SECONDS", "120")
    monkeypatch.setenv("SCRAPER_JOB_RETRY_ATTEMPTS", "3")

    config = QueueConfig.from_env()

    assert config.redis_url == "redis://example:6379/2"
    assert config.discovery_queue == "discover"
    assert config.listing_queue == "listing"
    assert config.default_timeout_seconds == 120
    assert config.default_retry_attempts == 3


def test_postgres_schema_tracks_listing_mode_state_separately():
    assert "listing_mode_state" in SCHEMA_SQL
    assert "PRIMARY KEY (campaign_id, place_id, mode)" in SCHEMA_SQL
    assert "coverage_status" in SCHEMA_SQL


def test_orchestrator_schema_tracks_jobs_and_browser_sessions():
    assert "CREATE TABLE IF NOT EXISTS jobs" in ORCHESTRATOR_SCHEMA_SQL
    assert "CREATE TABLE IF NOT EXISTS browser_sessions" in ORCHESTRATOR_SCHEMA_SQL
    assert "CREATE TABLE IF NOT EXISTS runner_nodes" in ORCHESTRATOR_SCHEMA_SQL


def test_orchestration_config_reads_runner_environment(monkeypatch):
    monkeypatch.setenv("SCRAPER_USE_POSTGRES_RUNNER", "1")
    monkeypatch.setenv("MAX_ACTIVE_JOBS", "4")
    monkeypatch.setenv("MAX_ACTIVE_BROWSER_SESSIONS", "3")
    monkeypatch.setenv("RUNNER_HEADED", "true")
    monkeypatch.setenv("RUNNER_NOVNC_BASE_URL", "http://localhost:7900")

    config = OrchestrationConfig.from_env()

    assert config.enabled is True
    assert config.max_active_jobs == 4
    assert config.max_active_browser_sessions == 3
    assert config.runner_headed is True
    assert config.runner_novnc_base_url == "http://localhost:7900"


def test_grid_cells_have_stable_ids_for_durable_campaigns():
    navigator = GridNavigator((51.8, 7.4, 52.0, 7.7), grid_size=2, zoom_level=13)

    assert [cell.id for cell in navigator.grid_cells] == [
        "1_1",
        "1_2",
        "2_1",
        "2_2",
    ]
