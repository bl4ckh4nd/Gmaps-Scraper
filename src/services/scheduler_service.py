"""Scheduler loop for the Postgres-first runner architecture."""

from __future__ import annotations

import logging
import time

from ..persistence.orchestrator_store import OrchestratorStore
from .governor_service import GlobalGovernor
from .orchestration import OrchestrationConfig


class SchedulerService:
    """Promotes queued jobs and recovers stale runs."""

    def __init__(
        self,
        *,
        store: OrchestratorStore | None = None,
        config: OrchestrationConfig | None = None,
    ) -> None:
        self.store = store or OrchestratorStore()
        self.config = config or OrchestrationConfig.from_env()
        self.logger = logging.getLogger(__name__)
        self.governor = GlobalGovernor(self.store, self.config)

    def run_once(self) -> dict[str, int]:
        self.store.initialize_schema()
        self.governor.seed_default_policies()
        stale_runs = self.store.mark_stale_job_runs(
            heartbeat_timeout_seconds=self.config.heartbeat_timeout_seconds,
            max_job_retries=self.config.max_job_retries,
            backoff_seconds=self.config.default_backoff_seconds,
        )
        snapshot = self.governor.snapshot()
        promoted_jobs = self.store.promote_ready_jobs(
            limit=snapshot.available_job_slots,
        )
        if stale_runs:
            self.logger.warning("Recovered %s stale runner jobs", stale_runs)
        if promoted_jobs:
            self.logger.info("Promoted %s jobs into waiting_for_slot", promoted_jobs)
        return {"stale_runs": stale_runs, "promoted_jobs": promoted_jobs}

    def run_forever(self) -> None:
        self.logger.info("Starting scheduler loop")
        while True:
            self.run_once()
            time.sleep(self.config.scheduler_poll_seconds)
