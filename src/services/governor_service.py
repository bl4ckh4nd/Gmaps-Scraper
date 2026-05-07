"""Centralized concurrency and backoff policy helpers."""

from __future__ import annotations

from dataclasses import dataclass

from ..persistence.orchestrator_store import OrchestratorStore
from .orchestration import OrchestrationConfig


@dataclass
class GovernorSnapshot:
    active_jobs: int
    active_browser_sessions: int
    available_job_slots: int
    available_browser_session_slots: int


class GlobalGovernor:
    """Small first-step governor for coarse global job/session limits."""

    def __init__(self, store: OrchestratorStore, config: OrchestrationConfig):
        self.store = store
        self.config = config

    def seed_default_policies(self) -> None:
        self.store.seed_default_rate_limit_policies()

    def snapshot(self) -> GovernorSnapshot:
        active_jobs = self.store.count_active_jobs()
        active_sessions = self.store.count_active_browser_sessions()
        return GovernorSnapshot(
            active_jobs=active_jobs,
            active_browser_sessions=active_sessions,
            available_job_slots=max(0, self.config.max_active_jobs - active_jobs),
            available_browser_session_slots=max(
                0,
                self.config.max_active_browser_sessions - active_sessions,
            ),
        )
