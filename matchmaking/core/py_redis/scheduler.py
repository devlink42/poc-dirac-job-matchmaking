#!/usr/bin/env python3

from __future__ import annotations

import redis
from pydantic import ValidationError

from matchmaking.config.logger import logger
from matchmaking.config.py_redis.config import PY_REDIS_JOB_KEY
from matchmaking.models.job import Job

_DEFAULT_CANDIDATE_JOBS_COUNT = 1000


def fetch_candidate_jobs(
    redis_client: redis.Redis,
    candidate_jobs_count: int,
) -> list[Job]:
    """Sample random jobs from Redis without loading the full key space into memory.

    Uses ``HRANDFIELD`` to obtain *candidate_jobs_count* unique random job IDs in a
    single O(count) operation, then fetches their JSON payloads via ``HMGET``.
    Memory consumption is therefore proportional to *candidate_jobs_count*, not to
    the total number of jobs stored — critical when the job hash has millions of
    entries and many Locust users run concurrently.

    Args:
        redis_client: A connected Redis client with ``decode_responses=True``.
        candidate_jobs_count: How many jobs to sample. When the hash contains fewer
            entries than requested, all available jobs are returned.

    Returns:
        A list of validated :class:`~matchmaking.models.job.Job` objects.  May
        be shorter than *candidate_jobs_count* if some stored payloads are missing
        or fail Pydantic validation (those are silently skipped with a warning).
    """
    job_ids: list[str] = redis_client.hrandfield(PY_REDIS_JOB_KEY, candidate_jobs_count)
    if not job_ids:
        return []

    raw_jobs: list[str | None] = redis_client.hmget(PY_REDIS_JOB_KEY, job_ids)
    jobs: list[Job] = []

    for raw in raw_jobs:
        if raw is None:
            continue

        try:
            jobs.append(Job.model_validate_json(raw))
        except ValidationError as exc:
            logger.warning("Skipping malformed job payload in Redis: %s", exc)

    return jobs
