#!/usr/bin/env python3

from __future__ import annotations

from matchmaking.models.job import Job as JobModel


class Job(JobModel):
    priority: int = 0

    def to_redis_hash(self) -> dict[str, str]:
        spec = self.matching_specs[0]
        num_cores = spec.cpu.num_cores
        ram_mb = spec.cpu.ram_mb

        payload = {
            "cpu_num_cores_min": str(num_cores.min),
            "cpu_num_cores_max": str(num_cores.max if num_cores.max is not None else num_cores.min),
            "cpu_architecture_microarchitecture_level_min": str(spec.cpu.architecture.microarchitecture_level.min),
        }

        if spec.cpu.architecture.microarchitecture_level.max is not None:
            payload["cpu_architecture_microarchitecture_level_max"] = str(
                spec.cpu.architecture.microarchitecture_level.max
            )

        if spec.site is not None:
            payload["site"] = str(spec.site)

        if ram_mb is not None:
            payload.update(
                {
                    "cpu_ram_mb_request_overhead": str(ram_mb.request.overhead),
                    "cpu_ram_mb_request_per_core": str(ram_mb.request.per_core),
                    "cpu_ram_mb_limit_overhead": str(ram_mb.limit.overhead),
                    "cpu_ram_mb_limit_per_core": str(ram_mb.limit.per_core),
                }
            )

        if spec.gpu is not None:
            payload["gpu_count_min"] = str(spec.gpu.count.min)
            if spec.gpu.count.max is not None:
                payload["gpu_count_max"] = str(spec.gpu.count.max)

        return payload
