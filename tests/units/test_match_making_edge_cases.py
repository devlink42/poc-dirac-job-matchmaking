#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from matchmaking.core.match_making import match_jobs_with_node, valid_job_specs_with_node
from matchmaking.models.job import MatchingSpecs
from matchmaking.models.node import Node

JOB_01 = "tests/examples/jobs/job_01_mcsimulation_any_site.yaml"
JOB_06 = "tests/examples/jobs/job_06_gpu.yaml"
JOB_07 = "tests/examples/jobs/job_07_sprucing_niche.yaml"
NODE_01 = "tests/examples/nodes/node_01_cern_typical.yaml"
NODE_03 = "tests/examples/nodes/node_03_gpu.yaml"


def _load_job_spec(path: str, spec_index: int = 0) -> dict:
    with open(path, "r") as f:
        data = yaml.safe_load(f)

    spec = data["matching_specs"][spec_index]
    spec.setdefault("job_id", "coverage-job")

    return spec


def _load_node_spec(path: str) -> dict:
    with open(path, "r") as f:
        data = yaml.safe_load(f)

    spec = data
    spec.setdefault("node_id", "coverage-node")

    return spec


def _build_models(job_path: str, node_path: str, mutator=None) -> tuple[MatchingSpecs, Node]:
    job_spec = _load_job_spec(job_path)
    node_spec = _load_node_spec(node_path)

    if mutator:
        mutator(job_spec, node_spec)

    try:
        job = MatchingSpecs.model_validate(job_spec)
    except ValidationError:
        raise

    return job, Node.model_validate(node_spec)


def _mutate_gpu_ram(job_spec: dict, node_spec: dict) -> None:
    job_spec["gpu"]["ram-mb"] = 50000


def _mutate_cpu_work(job_spec: dict, node_spec: dict) -> None:
    job_spec["cpu-work"] = 1000000
    node_spec["cpu_work"] = 1000


def _mutate_ram_limit(job_spec: dict, node_spec: dict) -> None:
    job_spec["cpu"]["ram-mb"]["limit"] = {"overhead": 50000, "per-core": 0}


def _mutate_gpu_vendor(job_spec: dict, node_spec: dict) -> None:
    job_spec["gpu"]["vendor"] = "amd"


def _mutate_ram_request(job_spec: dict, node_spec: dict) -> None:
    job_spec["cpu"]["ram-mb"]["request"] = {"overhead": 1000000, "per-core": 0}


def _mutate_microarch_min(job_spec: dict, node_spec: dict) -> None:
    job_spec["cpu"]["architecture"]["microarchitecture-level"] = {"min": 5, "max": None}


def _mutate_microarch_max(job_spec: dict, node_spec: dict) -> None:
    job_spec["cpu"]["architecture"]["microarchitecture-level"] = {"min": 1, "max": 3}


def _mutate_cpu_cores_min(job_spec: dict, node_spec: dict) -> None:
    job_spec["cpu"]["num-cores"] = {"min": 9, "max": 9}


def _mutate_gpu_count_max(job_spec: dict, node_spec: dict) -> None:
    node_spec["gpu"]["count"] = 2


def _mutate_user_namespaces(job_spec: dict, node_spec: dict) -> None:
    node_spec["system"]["user-namespaces"] = False


def _mutate_architecture_name(job_spec: dict, node_spec: dict) -> None:
    job_spec["cpu"]["architecture"]["name"] = "aarch64"


def _mutate_gpu_driver_version(job_spec: dict, node_spec: dict) -> None:
    job_spec["gpu"]["driver-version"] = "999.0"


def _mutate_missing_plain_tags(job_spec: dict, node_spec: dict) -> None:
    job_spec["tags"] = "missing:tag"


def _mutate_io_scratch_too_small(job_spec: dict, node_spec: dict) -> None:
    job_spec["io"] = {"scratch-mb": 1000000, "scratch-iops": 100}
    node_spec["io"] = {"scratch-mb": 1000, "scratch-iops": 1000}


def _mutate_gpu_compute_capability_min(job_spec: dict, node_spec: dict) -> None:
    job_spec["gpu"]["compute-capability"] = {"min": "9.1", "max": None}


def _mutate_gpu_compute_capability_max(job_spec: dict, node_spec: dict) -> None:
    job_spec["gpu"]["compute-capability"] = {"min": "7.0", "max": "7.5"}


def _base_node_spec() -> dict:
    return {
        "node_id": "edge-node",
        "site": "LCG.CERN.cern",
        "system": {"name": "Linux", "glibc": "2.28", "user-namespaces": True},
        "wall-time": 3600,
        "cpu-work": 1000,
        "cpu": {
            "num-nodes": 1,
            "num-cores": 1,
            "ram-mb": 1536,
            "architecture": {"name": "x86_64", "microarchitecture-level": 4},
        },
        "gpu": {
            "count": 1,
            "ram-mb": 8192,
            "vendor": "nvidia",
            "compute-capability": "8.0",
            "driver-version": "510.47.03",
        },
        "tags": ["cvmfs:lhcb", "gpu:nvidia", "os:el9"],
    }


def _base_job_spec() -> dict:
    return {
        "job_id": "edge-job",
        "site": "LCG.CERN.cern",
        "system": {"name": "Linux", "glibc": "2.28", "user-namespaces": True},
        "wall-time": 3600,
        "cpu-work": 1000,
        "cpu": {
            "num-cores": {"min": 1, "max": 1},
            "ram-mb": {
                "request": {"overhead": 1024, "per-core": 512},
                "limit": {"overhead": 1024, "per-core": 512},
            },
            "architecture": {"name": "x86_64", "microarchitecture-level": {"min": 4, "max": 4}},
        },
        "gpu": {
            "count": {"min": 1, "max": 1},
            "ram-mb": 8192,
            "vendor": "nvidia",
            "compute-capability": {"min": "8.0", "max": "8.0"},
            "driver-version": "510.47.03",
        },
        "tags": "cvmfs:lhcb & gpu:nvidia",
    }


@pytest.mark.parametrize(
    "job_path,node_path,mutator",
    [
        (JOB_01, NODE_01, _mutate_cpu_work),
        (JOB_01, NODE_01, _mutate_ram_request),
        (JOB_01, NODE_01, _mutate_missing_plain_tags),
        (JOB_01, NODE_01, _mutate_io_scratch_too_small),
        (JOB_06, NODE_03, _mutate_gpu_ram),
        (JOB_06, NODE_03, _mutate_ram_limit),
        (JOB_06, NODE_03, _mutate_gpu_vendor),
        (JOB_06, NODE_03, _mutate_cpu_cores_min),
        (JOB_06, NODE_03, _mutate_microarch_min),
        (JOB_06, NODE_03, _mutate_microarch_max),
        (JOB_06, NODE_03, _mutate_gpu_count_max),
        (JOB_06, NODE_03, _mutate_architecture_name),
        (JOB_06, NODE_03, _mutate_gpu_driver_version),
        (JOB_06, NODE_03, _mutate_gpu_compute_capability_min),
        (JOB_06, NODE_03, _mutate_gpu_compute_capability_max),
        (JOB_07, NODE_03, _mutate_user_namespaces),
    ],
)
def test_valid_job_with_node_failure_branches(job_path, node_path, mutator):
    try:
        job_specs, node = _build_models(job_path, node_path, mutator=mutator)
    except ValidationError:
        return

    job_id = Path(job_path).stem

    assert not valid_job_specs_with_node(job_id, job_specs, node)


def test_valid_job_with_node_accepts_boundary_equal_values():
    job = MatchingSpecs.model_validate(_base_job_spec())
    node = Node.model_validate(_base_node_spec())

    assert valid_job_specs_with_node("edge-job-0", job, node)


def test_match_jobs_with_node_raises_for_invalid_node(tmp_path):
    job_file = tmp_path / "job.yaml"
    node_file = tmp_path / "node_invalid.yaml"

    with open(job_file, "w") as f:
        yaml.safe_dump({"matching_specs": [_base_job_spec()]}, f)

    invalid_node = _base_node_spec()
    invalid_node.pop("site")
    with open(node_file, "w") as f:
        yaml.safe_dump(invalid_node, f)

    with pytest.raises(ValidationError):
        match_jobs_with_node(str(job_file), str(node_file))


def test_match_jobs_with_node_returns_empty_even_with_mixed_specs(tmp_path):
    job_file = tmp_path / "job_mixed.yaml"
    node_file = tmp_path / "node.yaml"

    valid_job_spec = _base_job_spec()
    invalid_job_spec = _base_job_spec()
    invalid_job_spec["cpu"]["num-cores"] = {"min": 2, "max": 1}

    with open(job_file, "w") as job, open(node_file, "w") as node:
        yaml.safe_dump({"job_id": "mixed-job", "matching_specs": [invalid_job_spec, valid_job_spec]}, job)
        yaml.safe_dump(_base_node_spec(), node)

    assert match_jobs_with_node(str(job_file), str(node_file))[0] == []


@pytest.mark.parametrize("job_content", [{}, {"matching_specs": []}])
def test_match_jobs_with_node_handles_missing_or_empty_matching_specs(tmp_path, job_content):
    job_file = tmp_path / "job_empty.yaml"
    node_file = tmp_path / "node.yaml"

    with open(job_file, "w") as job, open(node_file, "w") as node:
        yaml.safe_dump({"job_id": "edge-empty-job", **job_content}, job)
        yaml.safe_dump(_base_node_spec(), node)

    assert match_jobs_with_node(str(job_file), str(node_file))[0] == []


def test_match_jobs_returns_empty_when_job_specs_are_invalid():
    invalid_job = "tests/examples/jobs/invalid_01_job_min_gt_max.yaml"
    node_01 = "tests/examples/nodes/node_01_cern_typical.yaml"

    assert match_jobs_with_node(invalid_job, node_01)[0] == []
