"""Postgres-first orchestration persistence for scheduler/runner services."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from .postgres_store import PostgresStore
from ..utils.exceptions import PersistenceException


ORCHESTRATOR_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    job_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued',
    priority INTEGER NOT NULL DEFAULT 0,
    created_by TEXT,
    config_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    progress_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    output_dir TEXT,
    failure_category TEXT,
    failure_reason TEXT,
    last_error_message TEXT,
    recommended_action TEXT,
    current_run_id TEXT,
    cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
    backoff_until TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS job_runs (
    run_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    attempt_number INTEGER NOT NULL,
    status TEXT NOT NULL,
    runner_id TEXT,
    browser_session_id TEXT,
    heartbeat_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    exit_code INTEGER,
    failure_category TEXT,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS browser_sessions (
    session_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    job_run_id TEXT NOT NULL REFERENCES job_runs(run_id) ON DELETE CASCADE,
    runner_id TEXT NOT NULL,
    status TEXT NOT NULL,
    headed BOOLEAN NOT NULL DEFAULT TRUE,
    display_name TEXT,
    novnc_url TEXT,
    state_dir TEXT,
    artifact_dir TEXT,
    leased_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    released_at TIMESTAMPTZ,
    last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS job_checkpoints (
    job_id TEXT PRIMARY KEY REFERENCES jobs(job_id) ON DELETE CASCADE,
    job_run_id TEXT REFERENCES job_runs(run_id) ON DELETE CASCADE,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS job_events (
    id BIGSERIAL PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    job_run_id TEXT REFERENCES job_runs(run_id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    level TEXT NOT NULL DEFAULT 'info',
    message TEXT NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS job_artifacts (
    id BIGSERIAL PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    job_run_id TEXT REFERENCES job_runs(run_id) ON DELETE CASCADE,
    artifact_type TEXT NOT NULL,
    artifact_path TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS rate_limit_policies (
    action_type TEXT PRIMARY KEY,
    max_concurrent INTEGER NOT NULL,
    min_interval_seconds INTEGER NOT NULL DEFAULT 0,
    cooldown_seconds INTEGER NOT NULL DEFAULT 0,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS rate_limit_leases (
    lease_id TEXT PRIMARY KEY,
    action_type TEXT NOT NULL REFERENCES rate_limit_policies(action_type) ON DELETE CASCADE,
    job_id TEXT REFERENCES jobs(job_id) ON DELETE CASCADE,
    runner_id TEXT,
    browser_session_id TEXT REFERENCES browser_sessions(session_id) ON DELETE CASCADE,
    acquired_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ,
    released_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'active'
);

CREATE TABLE IF NOT EXISTS runner_nodes (
    runner_id TEXT PRIMARY KEY,
    hostname TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'idle',
    max_sessions INTEGER NOT NULL DEFAULT 1,
    supports_headed BOOLEAN NOT NULL DEFAULT TRUE,
    novnc_base_url TEXT,
    version TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_jobs_status_priority
    ON jobs (status, priority DESC, created_at);
CREATE INDEX IF NOT EXISTS idx_job_runs_status
    ON job_runs (status, heartbeat_at);
CREATE INDEX IF NOT EXISTS idx_browser_sessions_status
    ON browser_sessions (status, released_at, last_heartbeat_at);
CREATE INDEX IF NOT EXISTS idx_job_artifacts_job
    ON job_artifacts (job_id, artifact_type, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_runner_nodes_status
    ON runner_nodes (status, last_heartbeat_at);
"""


DEFAULT_RATE_LIMIT_POLICIES = (
    ("browser_session", 2, 0, 30, {"scope": "global"}),
    ("maps_search", 2, 5, 30, {"scope": "maps"}),
    ("listing_open", 2, 2, 15, {"scope": "maps"}),
    ("review_scroll", 1, 1, 15, {"scope": "maps"}),
)


@dataclass
class OrchestratorJobRecord:
    job_id: str
    job_type: str
    status: str
    priority: int
    created_by: Optional[str]
    config_payload: dict[str, Any]
    progress_payload: dict[str, Any]
    output_dir: Optional[str]
    failure_category: Optional[str]
    failure_reason: Optional[str]
    last_error_message: Optional[str]
    recommended_action: Optional[str]
    current_run_id: Optional[str]
    cancel_requested: bool
    backoff_until: Optional[datetime]
    created_at: datetime
    updated_at: datetime
    started_at: Optional[datetime]
    completed_at: Optional[datetime]


@dataclass
class JobRunRecord:
    run_id: str
    job_id: str
    attempt_number: int
    status: str
    runner_id: Optional[str]
    browser_session_id: Optional[str]
    heartbeat_at: Optional[datetime]
    started_at: datetime
    completed_at: Optional[datetime]
    failure_category: Optional[str]
    error_message: Optional[str]


@dataclass
class BrowserSessionRecord:
    session_id: str
    job_id: str
    job_run_id: str
    runner_id: str
    status: str
    headed: bool
    display_name: Optional[str]
    novnc_url: Optional[str]
    state_dir: Optional[str]
    artifact_dir: Optional[str]
    leased_at: datetime
    released_at: Optional[datetime]
    last_heartbeat_at: datetime


@dataclass
class ClaimedJobExecution:
    job: OrchestratorJobRecord
    run: JobRunRecord
    session: BrowserSessionRecord


class OrchestratorStore(PostgresStore):
    """Durable Postgres-backed store for the scheduler/runner architecture."""

    def initialize_schema(self) -> None:
        super().initialize_schema()
        try:
            with self._connect() as conn:
                conn.execute(ORCHESTRATOR_SCHEMA_SQL)
        except Exception as exc:
            raise PersistenceException(
                f"Failed to initialize orchestrator schema: {exc}"
            ) from exc
        self.seed_default_rate_limit_policies()

    def seed_default_rate_limit_policies(self) -> None:
        with self._connect() as conn:
            for action_type, max_concurrent, min_interval, cooldown, metadata in DEFAULT_RATE_LIMIT_POLICIES:
                conn.execute(
                    """
                    INSERT INTO rate_limit_policies (
                        action_type, max_concurrent, min_interval_seconds,
                        cooldown_seconds, metadata
                    ) VALUES (%s, %s, %s, %s, %s::jsonb)
                    ON CONFLICT (action_type) DO UPDATE SET
                        max_concurrent = EXCLUDED.max_concurrent,
                        min_interval_seconds = EXCLUDED.min_interval_seconds,
                        cooldown_seconds = EXCLUDED.cooldown_seconds,
                        metadata = EXCLUDED.metadata,
                        updated_at = now()
                    """,
                    (
                        action_type,
                        max_concurrent,
                        min_interval,
                        cooldown,
                        json.dumps(metadata),
                    ),
                )

    def queue_job(
        self,
        *,
        job_type: str,
        config_payload: dict[str, Any],
        progress_payload: Optional[dict[str, Any]] = None,
        output_dir: Optional[str] = None,
        priority: int = 0,
        created_by: str = "web",
        job_id: Optional[str] = None,
    ) -> str:
        job_id = job_id or str(uuid.uuid4())
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO jobs (
                    job_id, job_type, status, priority, created_by, config_payload,
                    progress_payload, output_dir
                ) VALUES (%s, %s, 'queued', %s, %s, %s::jsonb, %s::jsonb, %s)
                """,
                (
                    job_id,
                    job_type,
                    priority,
                    created_by,
                    json.dumps(config_payload or {}, default=str),
                    json.dumps(progress_payload or {}, default=str),
                    output_dir,
                ),
            )
        self.add_job_event(
            job_id,
            event_type="job_queued",
            message="Job was queued for Postgres-runner execution.",
            details={"job_type": job_type},
        )
        return job_id

    def get_job(self, job_id: str) -> OrchestratorJobRecord:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT job_id, job_type, status, priority, created_by, config_payload,
                       progress_payload, output_dir, failure_category, failure_reason,
                       last_error_message, recommended_action, current_run_id,
                       cancel_requested, backoff_until, created_at, updated_at,
                       started_at, completed_at
                FROM jobs
                WHERE job_id = %s
                """,
                (job_id,),
            ).fetchone()
        if row is None:
            raise PersistenceException(f"Job not found: {job_id}")
        return self._row_to_job(row)

    def list_jobs(self, limit: int = 50) -> list[OrchestratorJobRecord]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT job_id, job_type, status, priority, created_by, config_payload,
                       progress_payload, output_dir, failure_category, failure_reason,
                       last_error_message, recommended_action, current_run_id,
                       cancel_requested, backoff_until, created_at, updated_at,
                       started_at, completed_at
                FROM jobs
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (limit,),
            ).fetchall()
        return [self._row_to_job(row) for row in rows]

    def register_runner_node(
        self,
        *,
        runner_id: str,
        hostname: str,
        max_sessions: int,
        supports_headed: bool,
        novnc_base_url: Optional[str] = None,
        version: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO runner_nodes (
                    runner_id, hostname, status, max_sessions, supports_headed,
                    novnc_base_url, version, metadata
                ) VALUES (%s, %s, 'idle', %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (runner_id) DO UPDATE SET
                    hostname = EXCLUDED.hostname,
                    max_sessions = EXCLUDED.max_sessions,
                    supports_headed = EXCLUDED.supports_headed,
                    novnc_base_url = EXCLUDED.novnc_base_url,
                    version = EXCLUDED.version,
                    metadata = EXCLUDED.metadata,
                    updated_at = now(),
                    last_heartbeat_at = now()
                """,
                (
                    runner_id,
                    hostname,
                    max_sessions,
                    supports_headed,
                    novnc_base_url,
                    version,
                    json.dumps(metadata or {}, default=str),
                ),
            )

    def heartbeat_runner_node(
        self,
        runner_id: str,
        *,
        status: str,
        metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE runner_nodes
                SET status = %s,
                    metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb,
                    updated_at = now(),
                    last_heartbeat_at = now()
                WHERE runner_id = %s
                """,
                (status, json.dumps(metadata or {}, default=str), runner_id),
            )

    def count_active_jobs(self) -> int:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*)
                FROM jobs
                WHERE status IN ('starting_session', 'running', 'backoff')
                """,
            ).fetchone()
        return int(row[0] or 0)

    def count_active_browser_sessions(self) -> int:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*)
                FROM browser_sessions
                WHERE status IN ('leased', 'running') AND released_at IS NULL
                """,
            ).fetchone()
        return int(row[0] or 0)

    def promote_ready_jobs(self, *, limit: int) -> int:
        if limit <= 0:
            return 0
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT job_id
                FROM jobs
                WHERE status IN ('queued', 'retry_pending')
                  AND cancel_requested = FALSE
                  AND (backoff_until IS NULL OR backoff_until <= now())
                ORDER BY priority DESC, created_at
                LIMIT %s
                """,
                (limit,),
            ).fetchall()
            job_ids = [row[0] for row in rows]
            for job_id in job_ids:
                conn.execute(
                    """
                    UPDATE jobs
                    SET status = 'waiting_for_slot',
                        recommended_action = NULL,
                        updated_at = now()
                    WHERE job_id = %s
                    """,
                    (job_id,),
                )
        for job_id in job_ids:
            self.add_job_event(
                job_id,
                event_type="job_waiting_for_slot",
                message="Job is waiting for a browser-session slot.",
            )
        return len(job_ids)

    def mark_stale_job_runs(
        self,
        *,
        heartbeat_timeout_seconds: int,
        max_job_retries: int,
        backoff_seconds: int,
    ) -> int:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT r.run_id, r.job_id, COALESCE(r.browser_session_id, '')
                FROM job_runs r
                JOIN jobs j ON j.job_id = r.job_id
                WHERE r.status IN ('starting_session', 'running')
                  AND (
                    r.heartbeat_at IS NULL
                    OR r.heartbeat_at < now() - (%s * INTERVAL '1 second')
                  )
                  AND j.status IN ('starting_session', 'running', 'backoff')
                """,
                (heartbeat_timeout_seconds,),
            ).fetchall()
        for row in rows:
            session_id = row[2] or None
            self.fail_job_run(
                row[1],
                row[0],
                session_id,
                error_message="Runner heartbeat timed out.",
                failure_category="heartbeat_timeout",
                recommended_action="retry",
                max_job_retries=max_job_retries,
                backoff_seconds=backoff_seconds,
            )
        return len(rows)

    def claim_waiting_job(
        self,
        *,
        runner_id: str,
        max_active_browser_sessions: int,
        headed: bool,
        display_name: str,
        novnc_url: Optional[str],
        state_dir: str,
        artifact_dir: str,
    ) -> Optional[ClaimedJobExecution]:
        with self._connect() as conn:
            active_sessions = conn.execute(
                """
                SELECT COUNT(*)
                FROM browser_sessions
                WHERE status IN ('leased', 'running') AND released_at IS NULL
                """
            ).fetchone()
            if int(active_sessions[0] or 0) >= max_active_browser_sessions:
                return None

            row = conn.execute(
                """
                SELECT job_id, job_type, status, priority, created_by, config_payload,
                       progress_payload, output_dir, failure_category, failure_reason,
                       last_error_message, recommended_action, current_run_id,
                       cancel_requested, backoff_until, created_at, updated_at,
                       started_at, completed_at
                FROM jobs
                WHERE status = 'waiting_for_slot' AND cancel_requested = FALSE
                ORDER BY priority DESC, created_at
                LIMIT 1
                FOR UPDATE SKIP LOCKED
                """
            ).fetchone()
            if row is None:
                return None

            job = self._row_to_job(row)
            attempt_row = conn.execute(
                "SELECT COALESCE(MAX(attempt_number), 0) + 1 FROM job_runs WHERE job_id = %s",
                (job.job_id,),
            ).fetchone()
            attempt_number = int(attempt_row[0] or 1)
            run_id = str(uuid.uuid4())
            session_id = str(uuid.uuid4())

            conn.execute(
                """
                INSERT INTO job_runs (
                    run_id, job_id, attempt_number, status, runner_id,
                    browser_session_id, heartbeat_at
                ) VALUES (%s, %s, %s, 'starting_session', %s, %s, now())
                """,
                (run_id, job.job_id, attempt_number, runner_id, session_id),
            )
            conn.execute(
                """
                INSERT INTO browser_sessions (
                    session_id, job_id, job_run_id, runner_id, status, headed,
                    display_name, novnc_url, state_dir, artifact_dir
                ) VALUES (%s, %s, %s, %s, 'leased', %s, %s, %s, %s, %s)
                """,
                (
                    session_id,
                    job.job_id,
                    run_id,
                    runner_id,
                    headed,
                    display_name,
                    novnc_url,
                    state_dir,
                    artifact_dir,
                ),
            )
            conn.execute(
                """
                UPDATE jobs
                SET status = 'starting_session',
                    current_run_id = %s,
                    started_at = COALESCE(started_at, now()),
                    updated_at = now(),
                    failure_category = NULL,
                    failure_reason = NULL,
                    last_error_message = NULL
                WHERE job_id = %s
                """,
                (run_id, job.job_id),
            )

        claimed = ClaimedJobExecution(
            job=self.get_job(job.job_id),
            run=self.get_job_run(run_id),
            session=self.get_browser_session(session_id),
        )
        self.add_job_event(
            claimed.job.job_id,
            job_run_id=claimed.run.run_id,
            event_type="job_claimed",
            message=f"Runner {runner_id} claimed the job.",
            details={"runner_id": runner_id},
        )
        return claimed

    def get_job_run(self, run_id: str) -> JobRunRecord:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT run_id, job_id, attempt_number, status, runner_id,
                       browser_session_id, heartbeat_at, started_at, completed_at,
                       failure_category, error_message
                FROM job_runs
                WHERE run_id = %s
                """,
                (run_id,),
            ).fetchone()
        if row is None:
            raise PersistenceException(f"Job run not found: {run_id}")
        return JobRunRecord(
            run_id=row[0],
            job_id=row[1],
            attempt_number=int(row[2]),
            status=row[3],
            runner_id=row[4],
            browser_session_id=row[5],
            heartbeat_at=row[6],
            started_at=row[7],
            completed_at=row[8],
            failure_category=row[9],
            error_message=row[10],
        )

    def get_browser_session(self, session_id: str) -> BrowserSessionRecord:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT session_id, job_id, job_run_id, runner_id, status, headed,
                       display_name, novnc_url, state_dir, artifact_dir, leased_at,
                       released_at, last_heartbeat_at
                FROM browser_sessions
                WHERE session_id = %s
                """,
                (session_id,),
            ).fetchone()
        if row is None:
            raise PersistenceException(f"Browser session not found: {session_id}")
        return BrowserSessionRecord(
            session_id=row[0],
            job_id=row[1],
            job_run_id=row[2],
            runner_id=row[3],
            status=row[4],
            headed=bool(row[5]),
            display_name=row[6],
            novnc_url=row[7],
            state_dir=row[8],
            artifact_dir=row[9],
            leased_at=row[10],
            released_at=row[11],
            last_heartbeat_at=row[12],
        )

    def mark_job_run_running(self, job_id: str, run_id: str, session_id: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE jobs
                SET status = 'running', updated_at = now()
                WHERE job_id = %s
                """,
                (job_id,),
            )
            conn.execute(
                """
                UPDATE job_runs
                SET status = 'running', heartbeat_at = now()
                WHERE run_id = %s
                """,
                (run_id,),
            )
            conn.execute(
                """
                UPDATE browser_sessions
                SET status = 'running', last_heartbeat_at = now()
                WHERE session_id = %s
                """,
                (session_id,),
            )

    def heartbeat_job_run(
        self,
        job_id: str,
        run_id: str,
        session_id: str,
        *,
        progress_payload: Optional[dict[str, Any]] = None,
        checkpoint_payload: Optional[dict[str, Any]] = None,
        runner_id: Optional[str] = None,
    ) -> None:
        with self._connect() as conn:
            if progress_payload is not None:
                conn.execute(
                    """
                    UPDATE jobs
                    SET progress_payload = COALESCE(progress_payload, '{}'::jsonb) || %s::jsonb,
                        updated_at = now()
                    WHERE job_id = %s
                    """,
                    (json.dumps(progress_payload, default=str), job_id),
                )
            else:
                conn.execute(
                    "UPDATE jobs SET updated_at = now() WHERE job_id = %s",
                    (job_id,),
                )
            conn.execute(
                "UPDATE job_runs SET heartbeat_at = now() WHERE run_id = %s",
                (run_id,),
            )
            conn.execute(
                """
                UPDATE browser_sessions
                SET status = 'running', last_heartbeat_at = now()
                WHERE session_id = %s
                """,
                (session_id,),
            )
            if checkpoint_payload is not None:
                conn.execute(
                    """
                    INSERT INTO job_checkpoints (job_id, job_run_id, payload)
                    VALUES (%s, %s, %s::jsonb)
                    ON CONFLICT (job_id) DO UPDATE SET
                        job_run_id = EXCLUDED.job_run_id,
                        payload = EXCLUDED.payload,
                        updated_at = now()
                    """,
                    (job_id, run_id, json.dumps(checkpoint_payload, default=str)),
                )
            if runner_id:
                conn.execute(
                    """
                    UPDATE runner_nodes
                    SET status = 'busy', updated_at = now(), last_heartbeat_at = now()
                    WHERE runner_id = %s
                    """,
                    (runner_id,),
                )

    def complete_job_run(
        self,
        job_id: str,
        run_id: str,
        session_id: Optional[str],
        *,
        progress_payload: Optional[dict[str, Any]] = None,
        runner_id: Optional[str] = None,
    ) -> None:
        with self._connect() as conn:
            if progress_payload is not None:
                conn.execute(
                    """
                    UPDATE jobs
                    SET progress_payload = %s::jsonb,
                        status = CASE WHEN cancel_requested THEN 'cancelled' ELSE 'completed' END,
                        completed_at = now(),
                        updated_at = now(),
                        recommended_action = NULL
                    WHERE job_id = %s
                    """,
                    (json.dumps(progress_payload, default=str), job_id),
                )
            else:
                conn.execute(
                    """
                    UPDATE jobs
                    SET status = CASE WHEN cancel_requested THEN 'cancelled' ELSE 'completed' END,
                        completed_at = now(),
                        updated_at = now(),
                        recommended_action = NULL
                    WHERE job_id = %s
                    """,
                    (job_id,),
                )
            conn.execute(
                """
                UPDATE job_runs
                SET status = 'completed', completed_at = now(), heartbeat_at = now()
                WHERE run_id = %s
                """,
                (run_id,),
            )
            if session_id:
                conn.execute(
                    """
                    UPDATE browser_sessions
                    SET status = 'completed', released_at = now(), last_heartbeat_at = now()
                    WHERE session_id = %s
                    """,
                    (session_id,),
                )
            if runner_id:
                conn.execute(
                    """
                    UPDATE runner_nodes
                    SET status = 'idle', updated_at = now(), last_heartbeat_at = now()
                    WHERE runner_id = %s
                    """,
                    (runner_id,),
                )

    def fail_job_run(
        self,
        job_id: str,
        run_id: str,
        session_id: Optional[str],
        *,
        error_message: str,
        failure_category: str,
        recommended_action: str,
        max_job_retries: int,
        backoff_seconds: int,
        runner_id: Optional[str] = None,
    ) -> None:
        run = self.get_job_run(run_id)
        job = self.get_job(job_id)
        should_retry = (
            not job.cancel_requested
            and run.attempt_number < max_job_retries
            and recommended_action == "retry"
        )
        next_status = "retry_pending" if should_retry else ("cancelled" if job.cancel_requested else "failed")
        backoff_until = (
            datetime.now(timezone.utc) + timedelta(seconds=backoff_seconds)
            if should_retry
            else None
        )
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE jobs
                SET status = %s,
                    failure_category = %s,
                    failure_reason = %s,
                    last_error_message = %s,
                    recommended_action = %s,
                    backoff_until = %s,
                    completed_at = CASE WHEN %s IN ('failed', 'cancelled') THEN now() ELSE completed_at END,
                    updated_at = now()
                WHERE job_id = %s
                """,
                (
                    next_status,
                    failure_category,
                    failure_category,
                    error_message[:4000],
                    recommended_action,
                    backoff_until,
                    next_status,
                    job_id,
                ),
            )
            conn.execute(
                """
                UPDATE job_runs
                SET status = 'failed',
                    completed_at = now(),
                    heartbeat_at = now(),
                    failure_category = %s,
                    error_message = %s
                WHERE run_id = %s
                """,
                (failure_category, error_message[:4000], run_id),
            )
            if session_id:
                conn.execute(
                    """
                    UPDATE browser_sessions
                    SET status = 'failed', released_at = now(), last_heartbeat_at = now()
                    WHERE session_id = %s
                    """,
                    (session_id,),
                )
            if runner_id:
                conn.execute(
                    """
                    UPDATE runner_nodes
                    SET status = 'idle', updated_at = now(), last_heartbeat_at = now()
                    WHERE runner_id = %s
                    """,
                    (runner_id,),
                )
        self.add_job_event(
            job_id,
            job_run_id=run_id,
            event_type="job_failed",
            level="error",
            message=error_message[:4000],
            details={"failure_category": failure_category, "next_status": next_status},
        )

    def request_job_cancel(self, job_id: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE jobs
                SET cancel_requested = TRUE,
                    status = CASE
                        WHEN status IN ('queued', 'waiting_for_slot', 'retry_pending', 'backoff')
                        THEN 'cancelled'
                        ELSE status
                    END,
                    completed_at = CASE
                        WHEN status IN ('queued', 'waiting_for_slot', 'retry_pending', 'backoff')
                        THEN now()
                        ELSE completed_at
                    END,
                    updated_at = now(),
                    recommended_action = 'cancel'
                WHERE job_id = %s
                """,
                (job_id,),
            )
        self.add_job_event(
            job_id,
            event_type="job_cancel_requested",
            message="Cancellation was requested for the job.",
        )

    def is_job_cancel_requested(self, job_id: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT cancel_requested FROM jobs WHERE job_id = %s",
                (job_id,),
            ).fetchone()
        return bool(row and row[0])

    def add_job_event(
        self,
        job_id: str,
        *,
        event_type: str,
        message: str,
        level: str = "info",
        details: Optional[dict[str, Any]] = None,
        job_run_id: Optional[str] = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO job_events (job_id, job_run_id, event_type, level, message, details)
                VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                """,
                (
                    job_id,
                    job_run_id,
                    event_type,
                    level,
                    message[:4000],
                    json.dumps(details or {}, default=str),
                ),
            )

    def add_job_artifact(
        self,
        job_id: str,
        *,
        artifact_type: str,
        artifact_path: str,
        metadata: Optional[dict[str, Any]] = None,
        job_run_id: Optional[str] = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO job_artifacts (job_id, job_run_id, artifact_type, artifact_path, metadata)
                VALUES (%s, %s, %s, %s, %s::jsonb)
                """,
                (
                    job_id,
                    job_run_id,
                    artifact_type,
                    artifact_path,
                    json.dumps(metadata or {}, default=str),
                ),
            )

    def get_job_artifacts(self, job_id: str) -> dict[str, str]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT artifact_type, artifact_path
                FROM job_artifacts
                WHERE job_id = %s
                ORDER BY created_at DESC, id DESC
                """,
                (job_id,),
            ).fetchall()
        artifacts: dict[str, str] = {}
        for artifact_type, artifact_path in rows:
            artifacts.setdefault(artifact_type, artifact_path)
        return artifacts

    def list_job_artifacts(
        self,
        job_id: str,
        *,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT artifact_type, artifact_path, metadata, created_at
                FROM job_artifacts
                WHERE job_id = %s
                ORDER BY created_at DESC, id DESC
                LIMIT %s
                """,
                (job_id, limit),
            ).fetchall()
        return [
            {
                "artifact_type": row[0],
                "artifact_path": row[1],
                "metadata": dict(row[2] or {}),
                "created_at": row[3].isoformat() if row[3] else None,
            }
            for row in rows
        ]

    def get_job_checkpoint(self, job_id: str) -> Optional[dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT job_run_id, payload, updated_at
                FROM job_checkpoints
                WHERE job_id = %s
                """,
                (job_id,),
            ).fetchone()
        if row is None:
            return None
        return {
            "job_run_id": row[0],
            "payload": dict(row[1] or {}),
            "updated_at": row[2].isoformat() if row[2] else None,
        }

    def list_job_events(
        self,
        job_id: str,
        *,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT job_run_id, event_type, level, message, details, created_at
                FROM job_events
                WHERE job_id = %s
                ORDER BY created_at DESC, id DESC
                LIMIT %s
                """,
                (job_id, limit),
            ).fetchall()
        return [
            {
                "job_run_id": row[0],
                "event_type": row[1],
                "level": row[2],
                "message": row[3],
                "details": dict(row[4] or {}),
                "created_at": row[5].isoformat() if row[5] else None,
            }
            for row in rows
        ]

    def get_current_browser_session(self, job_id: str) -> Optional[BrowserSessionRecord]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT session_id, job_id, job_run_id, runner_id, status, headed,
                       display_name, novnc_url, state_dir, artifact_dir, leased_at,
                       released_at, last_heartbeat_at
                FROM browser_sessions
                WHERE job_id = %s
                ORDER BY leased_at DESC
                LIMIT 1
                """,
                (job_id,),
            ).fetchone()
        if row is None:
            return None
        return BrowserSessionRecord(
            session_id=row[0],
            job_id=row[1],
            job_run_id=row[2],
            runner_id=row[3],
            status=row[4],
            headed=bool(row[5]),
            display_name=row[6],
            novnc_url=row[7],
            state_dir=row[8],
            artifact_dir=row[9],
            leased_at=row[10],
            released_at=row[11],
            last_heartbeat_at=row[12],
        )

    def get_runner_node(self, runner_id: str) -> Optional[dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT runner_id, hostname, status, max_sessions, supports_headed,
                       novnc_base_url, version, metadata, last_heartbeat_at
                FROM runner_nodes
                WHERE runner_id = %s
                """,
                (runner_id,),
            ).fetchone()
        if row is None:
            return None
        return {
            "runner_id": row[0],
            "hostname": row[1],
            "status": row[2],
            "max_sessions": int(row[3] or 0),
            "supports_headed": bool(row[4]),
            "novnc_base_url": row[5],
            "version": row[6],
            "metadata": dict(row[7] or {}),
            "last_heartbeat_at": row[8].isoformat() if row[8] else None,
        }

    def requeue_job(
        self,
        job_id: str,
        *,
        reset_progress: bool = False,
        clear_checkpoint: bool = False,
    ) -> None:
        job = self.get_job(job_id)
        progress_payload = {} if reset_progress else dict(job.progress_payload or {})
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE jobs
                SET status = 'queued',
                    cancel_requested = FALSE,
                    current_run_id = NULL,
                    failure_category = NULL,
                    failure_reason = NULL,
                    last_error_message = NULL,
                    recommended_action = NULL,
                    backoff_until = NULL,
                    completed_at = NULL,
                    progress_payload = %s::jsonb,
                    updated_at = now()
                WHERE job_id = %s
                """,
                (json.dumps(progress_payload, default=str), job_id),
            )
            if clear_checkpoint:
                conn.execute(
                    "DELETE FROM job_checkpoints WHERE job_id = %s",
                    (job_id,),
                )
        self.add_job_event(
            job_id,
            event_type="job_requeued",
            message=(
                "Job was requeued from scratch."
                if reset_progress
                else "Job was requeued from its last known state."
            ),
            details={
                "reset_progress": reset_progress,
                "clear_checkpoint": clear_checkpoint,
            },
        )

    @staticmethod
    def _row_to_job(row) -> OrchestratorJobRecord:
        return OrchestratorJobRecord(
            job_id=row[0],
            job_type=row[1],
            status=row[2],
            priority=int(row[3] or 0),
            created_by=row[4],
            config_payload=dict(row[5] or {}),
            progress_payload=dict(row[6] or {}),
            output_dir=row[7],
            failure_category=row[8],
            failure_reason=row[9],
            last_error_message=row[10],
            recommended_action=row[11],
            current_run_id=row[12],
            cancel_requested=bool(row[13]),
            backoff_until=row[14],
            created_at=row[15],
            updated_at=row[16],
            started_at=row[17],
            completed_at=row[18],
        )


def get_orchestrator_store(database_url: Optional[str] = None) -> OrchestratorStore:
    return OrchestratorStore(database_url=database_url)
