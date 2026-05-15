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
            "system_name": str(spec.system.name),
            "cpu_num_cores_min": str(num_cores.min),
            "cpu_num_cores_max": str(num_cores.max if num_cores.max is not None else num_cores.min),
            "cpu_architecture_name": str(spec.cpu.architecture.name),
            "cpu_architecture_microarchitecture_level_min": str(spec.cpu.architecture.microarchitecture_level.min),
        }

        if spec.system.glibc is not None:
            payload["system_glibc"] = str(spec.system.glibc)

        if spec.system.user_namespaces is not None:
            payload["system_user_namespaces"] = "1" if spec.system.user_namespaces else "0"

        if spec.wall_time is not None:
            payload["wall_time"] = str(spec.wall_time)

        if spec.cpu_work is not None:
            payload["cpu_work"] = str(spec.cpu_work)

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

            payload["gpu_ram_mb"] = str(spec.gpu.ram_mb)
            payload["gpu_vendor"] = str(spec.gpu.vendor)
            payload["gpu_compute_capability_min"] = str(spec.gpu.compute_capability.min)

            if spec.gpu.compute_capability.max is not None:
                payload["gpu_compute_capability_max"] = str(spec.gpu.compute_capability.max)

            if spec.gpu.driver_version is not None:
                payload["gpu_driver_version"] = str(spec.gpu.driver_version)

        if spec.io is not None:
            payload["io_scratch_mb"] = str(spec.io.scratch_mb)

        return payload
