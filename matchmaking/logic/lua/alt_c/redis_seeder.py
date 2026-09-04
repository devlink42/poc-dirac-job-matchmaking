#!/usr/bin/env python3
"""Seed normalized Alternative C requirement groups into Redis."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import redis

from matchmaking.models.job import Job, MatchingSpecs
from matchmaking.models.lua.alt_c.requirement_group import RequirementGroup
from matchmaking.models.utils import JobStatus


class RedisSeeder:
    """Store jobs once while indexing their normalized requirement group by site.

    Args:
        redis_client: Redis client used to create transactional pipelines.
        known_sites: Complete site catalogue used to expand a matching specification
            whose site is ``None``.
        memory_bucket_mb: Bucket size for RAM and scratch-space requirements.
        cpu_work_bucket: Bucket size for CPU-work requirements.
        wall_time_bucket_seconds: Bucket size for wall-time requirements.
        core_bucket: Bucket size for CPU and GPU count ranges.
        watch_retries: Maximum optimistic-transaction attempts per job.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        *,
        known_sites: Iterable[str] = (),
        memory_bucket_mb: int = 1000,
        cpu_work_bucket: int = 1000,
        wall_time_bucket_seconds: int = 1000,
        core_bucket: int = 1,
        watch_retries: int = 3,
    ) -> None:
        bucket_values = {
            "memory_bucket_mb": memory_bucket_mb,
            "cpu_work_bucket": cpu_work_bucket,
            "wall_time_bucket_seconds": wall_time_bucket_seconds,
            "core_bucket": core_bucket,
            "watch_retries": watch_retries,
        }
        for name, value in bucket_values.items():
            if isinstance(value, bool) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")

        site_values = tuple(known_sites)
        if any(not isinstance(site, str) or not site for site in site_values):
            raise ValueError("known_sites must contain non-empty strings")

        self._redis = redis_client
        self._known_sites = tuple(sorted(set(site_values)))
        self._memory_bucket_mb = memory_bucket_mb
        self._cpu_work_bucket = cpu_work_bucket
        self._wall_time_bucket_seconds = wall_time_bucket_seconds
        self._core_bucket = core_bucket
        self._watch_retries = watch_retries

    def build_requirement_group(self, job: Job) -> RequirementGroup:
        """Build the canonical group shared by all site variants of a job.

        Args:
            job: Waiting job whose matching specifications are normalized.

        Returns:
            The validated, content-addressed requirement group.

        Raises:
            ValueError: If the job cannot be represented by one site-set queue.
        """
        if not job.job_id:
            raise ValueError("job_id must be a non-empty string")
        if job.status != JobStatus.WAITING:
            raise ValueError("only waiting jobs can be seeded")

        eligible_sites = self._resolve_eligible_sites(job.matching_specs)
        payloads = [self._build_payload(job, spec) for spec in job.matching_specs]
        first_payload = payloads[0]
        if any(payload != first_payload for payload in payloads[1:]):
            raise ValueError("matching_specs must differ only by site for a site-set queue")

        return RequirementGroup.model_validate({**first_payload, "eligible_sites": eligible_sites})

    def seed_job(self, job: Job) -> str:
        """Atomically add a job and its group indexes to Redis.

        The requirement hash is written only when absent. ``WATCH`` prevents a
        concurrent matcher from deleting the group between that check and the
        transactional ``SADD``/``HSET``/``LPUSH`` sequence.

        Args:
            job: Waiting job to enqueue.

        Returns:
            The deterministic requirement-group identifier.

        Raises:
            redis.WatchError: If every optimistic-transaction attempt conflicts.
            ValueError: If the job cannot be represented by this schema.
        """
        group = self.build_requirement_group(job)
        req_group_id = group.req_group_id
        group_key = f"req_group:{req_group_id}"
        last_error: redis.WatchError | None = None

        for _ in range(self._watch_retries):
            pipeline = self._redis.pipeline(transaction=True)
            try:
                pipeline.watch(group_key)
                group_exists = bool(pipeline.exists(group_key))
                pipeline.multi()

                for site in group.eligible_sites:
                    index_key = self._index_key(
                        site,
                        group.job_type,
                        group.architecture,
                        group.has_gpu,
                    )
                    pipeline.sadd(index_key, req_group_id)

                if not group_exists:
                    pipeline.hset(group_key, mapping=group.to_redis_hash())

                pipeline.lpush(f"queue:{req_group_id}", job.job_id)
                pipeline.execute()
            except redis.WatchError as error:
                last_error = error
            else:
                return req_group_id
            finally:
                pipeline.reset()

        raise redis.WatchError(f"could not seed requirement group after {self._watch_retries} attempts") from last_error

    def _build_payload(self, job: Job, spec: MatchingSpecs) -> dict[str, Any]:
        min_cpu_cores = self._round_up(int(spec.cpu.num_cores.min), self._core_bucket)
        max_cpu_cores = self._round_down(int(spec.cpu.num_cores.max), self._core_bucket)
        if max_cpu_cores < min_cpu_cores:
            raise ValueError("core_bucket makes the normalized CPU range empty")

        min_ram_mb = 0
        if spec.cpu.ram_mb is not None:
            request = spec.cpu.ram_mb.request
            limit = spec.cpu.ram_mb.limit
            request_mb = int(request.overhead) + int(request.per_core) * min_cpu_cores
            limit_mb = int(limit.overhead) + int(limit.per_core) * min_cpu_cores
            min_ram_mb = self._round_up(max(request_mb, limit_mb), self._memory_bucket_mb)

        gpu = spec.gpu
        min_gpu_count = self._round_up(int(gpu.count.min), self._core_bucket) if gpu is not None else 0
        max_gpu_count = self._round_down(int(gpu.count.max), self._core_bucket) if gpu is not None else 0
        if max_gpu_count < min_gpu_count:
            raise ValueError("core_bucket makes the normalized GPU range empty")

        architecture = spec.cpu.architecture
        microarchitecture = architecture.microarchitecture_level

        return {
            "job_type": job.type,
            "system_name": spec.system.name,
            "min_system_glibc": str(spec.system.glibc) if spec.system.glibc is not None else None,
            "requires_user_namespaces": spec.system.user_namespaces,
            "min_wall_time": self._round_optional_up(spec.wall_time, self._wall_time_bucket_seconds),
            "min_cpu_work": self._round_optional_up(spec.cpu_work, self._cpu_work_bucket),
            "min_cpu_cores": min_cpu_cores,
            "max_cpu_cores": max_cpu_cores,
            "min_ram_mb": min_ram_mb,
            "architecture": architecture.name,
            "min_microarchitecture_level": self._round_up(int(microarchitecture.min), 1),
            "max_microarchitecture_level": (
                self._round_down(int(microarchitecture.max), 1) if microarchitecture.max is not None else None
            ),
            "has_gpu": min_gpu_count > 0,
            "min_gpu_count": min_gpu_count,
            "max_gpu_count": max_gpu_count,
            "min_gpu_ram_mb": (self._round_up(int(gpu.ram_mb), self._memory_bucket_mb) if gpu is not None else None),
            "gpu_vendor": gpu.vendor if gpu is not None else None,
            "min_gpu_compute_capability": (str(gpu.compute_capability.min) if gpu is not None else None),
            "max_gpu_compute_capability": (
                str(gpu.compute_capability.max) if gpu is not None and gpu.compute_capability.max is not None else None
            ),
            "min_gpu_driver_version": (
                str(gpu.driver_version) if gpu is not None and gpu.driver_version is not None else None
            ),
            "min_scratch_mb": (
                self._round_up(int(spec.io.scratch_mb), self._memory_bucket_mb) if spec.io is not None else None
            ),
            "tags": spec.tags,
        }

    def _resolve_eligible_sites(self, specs: list[MatchingSpecs]) -> tuple[str, ...]:
        explicit_sites = {spec.site for spec in specs if spec.site is not None}
        if any(spec.site is None for spec in specs):
            if not self._known_sites:
                raise ValueError("known_sites is required for a matching specification without a site")
            explicit_sites.update(self._known_sites)

        if not explicit_sites or any(not site for site in explicit_sites):
            raise ValueError("matching specifications must resolve to non-empty sites")

        return tuple(sorted(explicit_sites))

    @staticmethod
    def _index_key(site: str, job_type: object, architecture: object, has_gpu: bool) -> str:
        return f"index:{site}:{job_type}:{architecture}:{int(has_gpu)}"

    @staticmethod
    def _round_up(value: int, step: int) -> int:
        return ((value + step - 1) // step) * step

    @staticmethod
    def _round_down(value: int, step: int) -> int:
        return (value // step) * step

    @classmethod
    def _round_optional_up(cls, value: int | None, step: int) -> int | None:
        return cls._round_up(int(value), step) if value is not None else None
