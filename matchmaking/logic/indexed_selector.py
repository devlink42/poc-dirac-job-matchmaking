#!/usr/bin/env python3
"""Indexed in-memory job selection for high-throughput matchmaking."""

from __future__ import annotations

import random
from collections import Counter, defaultdict
from random import Random
from threading import RLock

from matchmaking.core.match import is_matching
from matchmaking.core.utils import assign_job_to_site
from matchmaking.models.config import SchedulingConfig
from matchmaking.models.job import Job
from matchmaking.models.node import Node
from matchmaking.models.utils import JobStatus

_OwnerQueues = dict[str, list[Job]]
_GroupQueues = dict[str, _OwnerQueues]


class IndexedJobSelector:
    """Select jobs from persistent queues without scanning the entire pool."""

    def __init__(self, jobs: list[Job], config: SchedulingConfig) -> None:
        """Build FIFO partitions and running counters once.

        Args:
            jobs: Complete candidate and running-job pool.
            config: Scheduling priorities and per-site running limits.
        """
        queues: defaultdict[str, defaultdict[str, defaultdict[str, list[Job]]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(list))
        )
        self._running_by_group: Counter[str] = Counter()
        self._running_by_owner: Counter[str] = Counter()
        self._running_by_site_type: Counter[tuple[str, str]] = Counter()

        for job in jobs:
            if job.status == JobStatus.WAITING:
                queues[job.type][job.group][job.owner].append(job)
            elif job.status == JobStatus.RUNNING:
                self._increment_running(job)

        self._queues: dict[str, _GroupQueues] = {
            job_type: {
                group: {
                    owner: sorted(owner_jobs, key=lambda job: job.submit_time) for owner, owner_jobs in owners.items()
                }
                for group, owners in groups.items()
            }
            for job_type, groups in queues.items()
        }
        self._config = config
        self._lock = RLock()

    def select(self, node: Node, rng: Random | None = None) -> Job | None:
        """Atomically select and assign the highest-ranked compatible job.

        Args:
            node: Compute node requesting work.
            rng: Optional deterministic generator for weighted priorities.

        Returns:
            The assigned job, or None when no compatible job is available.
        """
        with self._lock:
            candidates: dict[str, Job | None] = {}

            def candidate_for(job_type: str) -> Job | None:
                if job_type not in candidates:
                    candidates[job_type] = self._best_candidate(job_type, node)
                return candidates[job_type]

            selected: Job | None = None
            for priority in self._config.job_type_priorities:
                if isinstance(priority, dict):
                    relevant = {
                        job_type: weight for job_type, weight in priority.items() if candidate_for(job_type) is not None
                    }
                    if not relevant:
                        continue

                    random_value = (rng if rng else random).uniform(0, sum(relevant.values()))  # noqa: S311
                    cumulative_weight = 0
                    for job_type in sorted(relevant):
                        cumulative_weight += relevant[job_type]
                        if random_value <= cumulative_weight:
                            selected = candidate_for(job_type)
                            break
                else:
                    selected = candidate_for(priority)

                if selected is not None:
                    break

            if selected is None:
                selected = self._best_fallback_candidate(node, candidates)
            if selected is None:
                return None

            assign_job_to_site(selected, node.site)
            self._increment_running(selected)
            return selected

    def _best_candidate(self, job_type: str, node: Node) -> Job | None:
        site_config = self._config.by_site.get(node.site)
        site_limits = site_config.running_limits if site_config else {}
        if self._running_by_site_type[(node.site, job_type)] >= site_limits.get(job_type, float("inf")):
            return None

        best_job: Job | None = None
        best_score = None
        for group, owners in self._queues.get(job_type, {}).items():
            for owner, jobs in owners.items():
                for job in jobs:
                    if job.status == JobStatus.WAITING and is_matching(job, node):
                        score = (
                            self._running_by_group[group],
                            self._running_by_owner[owner],
                            job.submit_time,
                        )
                        if best_score is None or score < best_score:
                            best_job = job
                            best_score = score
                        break

        return best_job

    def _best_fallback_candidate(self, node: Node, candidates: dict[str, Job | None]) -> Job | None:
        best_job = None
        best_score = None
        for job_type in self._queues:
            candidate = candidates.get(job_type)
            if job_type not in candidates:
                candidate = self._best_candidate(job_type, node)
            if candidate is None:
                continue

            score = (
                self._running_by_group[candidate.group],
                self._running_by_owner[candidate.owner],
                candidate.submit_time,
            )
            if best_score is None or score < best_score:
                best_job = candidate
                best_score = score

        return best_job

    def _increment_running(self, job: Job) -> None:
        self._running_by_group[job.group] += 1
        self._running_by_owner[job.owner] += 1
        self._running_by_site_type[(job.assigned_site or "", job.type)] += 1
