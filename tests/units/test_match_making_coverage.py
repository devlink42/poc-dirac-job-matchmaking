#!/usr/bin/env python3

from __future__ import annotations

from copy import deepcopy

import pytest
import yaml
from pydantic import TypeAdapter, ValidationError

from matchmaking.core import match_making as vp
from matchmaking.logic import tags
from matchmaking.models.job import Job, MatchingSpecs
from matchmaking.models.node import Node
from matchmaking.models.utils import CustomVersion

JOB_01 = "tests/examples/jobs/job_01_mcsimulation_any_site.yaml"
JOB_06 = "tests/examples/jobs/job_06_gpu.yaml"
JOB_07 = "tests/examples/jobs/job_07_sprucing_niche.yaml"
NODE_01 = "tests/examples/nodes/node_01_cern_typical.yaml"
NODE_03 = "tests/examples/nodes/node_03_gpu.yaml"


def _load_job_spec(path: str, spec_index: int = 0) -> dict:
    with open(path, "r") as f:
        data = yaml.safe_load(f)

    spec = deepcopy(data["matching_specs"][spec_index])
    spec.setdefault("job_id", "coverage-job")

    return spec


def _load_node_spec(path: str) -> dict:
    with open(path, "r") as f:
        data = yaml.safe_load(f)

    spec = deepcopy(data)
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


def _mutate_user_namespaces(job_spec: dict, node_spec: dict) -> None:
    node_spec["system"]["user-namespaces"] = False


def _mutate_cpu_cores_min(job_spec: dict, node_spec: dict) -> None:
    job_spec["cpu"]["num-cores"] = {"min": 9, "max": 9}


def _mutate_ram_limit(job_spec: dict, node_spec: dict) -> None:
    job_spec["cpu"]["ram-mb"]["limit"] = {"overhead": 50000, "per-core": 0}


def _mutate_architecture_name(job_spec: dict, node_spec: dict) -> None:
    job_spec["cpu"]["architecture"]["name"] = "aarch64"


def _mutate_microarch_min(job_spec: dict, node_spec: dict) -> None:
    job_spec["cpu"]["architecture"]["microarchitecture-level"] = {"min": 5, "max": None}


def _mutate_microarch_max(job_spec: dict, node_spec: dict) -> None:
    job_spec["cpu"]["architecture"]["microarchitecture-level"] = {"min": 1, "max": 3}


def _mutate_gpu_count_max(job_spec: dict, node_spec: dict) -> None:
    node_spec["gpu"]["count"] = 2


def _mutate_gpu_ram(job_spec: dict, node_spec: dict) -> None:
    job_spec["gpu"]["ram-mb"] = 50000


def _mutate_gpu_vendor(job_spec: dict, node_spec: dict) -> None:
    job_spec["gpu"]["vendor"] = "amd"


def _mutate_gpu_compute_capability_min(job_spec: dict, node_spec: dict) -> None:
    job_spec["gpu"]["compute-capability"] = {"min": "9.1", "max": None}


def _mutate_gpu_compute_capability_max(job_spec: dict, node_spec: dict) -> None:
    job_spec["gpu"]["compute-capability"] = {"min": "7.0", "max": "7.5"}


def _mutate_gpu_driver_version(job_spec: dict, node_spec: dict) -> None:
    job_spec["gpu"]["driver-version"] = "999.0"


def _mutate_io_scratch(job_spec: dict, node_spec: dict) -> None:
    node_spec["io"] = {"scratch-mb": 1024}


def _mutate_io_lan(job_spec: dict, node_spec: dict) -> None:
    node_spec["io"] = {"scratch-mb": 8192, "lan-mbitps": 100}
    job_spec["io"]["lan-mbitps"] = 200


def _mutate_invalid_tag_expression(job_spec: dict, node_spec: dict) -> None:
    job_spec["tags"] = "cvmfs:lhcb & ("


def _mutate_missing_plain_tags(job_spec: dict, node_spec: dict) -> None:
    job_spec["tags"] = "missing:tag"


def _mutate_cpu_work(job_spec: dict, node_spec: dict) -> None:
    job_spec["cpu-work"] = 1000000
    node_spec["cpu_work"] = 1000


def _mutate_ram_request(job_spec: dict, node_spec: dict) -> None:
    job_spec["cpu"]["ram-mb"]["request"] = {"overhead": 1000000, "per-core": 0}


def _mutate_io_scratch_too_small(job_spec: dict, node_spec: dict) -> None:
    job_spec["io"] = {"scratch-mb": 1000000, "scratch-iops": 100}
    node_spec["io"] = {"scratch-mb": 1000, "scratch-iops": 1000}


def _mutate_tag_eval_error(job_spec: dict, node_spec: dict) -> None:
    job_spec["tags"] = "a & (b | c)"
    job_spec["tags"] = "tag_that_is_missing & tag_a"


@pytest.mark.parametrize(
    "job_path,node_path,mutator",
    [
        (JOB_01, NODE_01, _mutate_missing_plain_tags),
        (JOB_01, NODE_01, _mutate_invalid_tag_expression),
        (JOB_01, NODE_01, _mutate_cpu_work),
        (JOB_01, NODE_01, _mutate_ram_request),
        (JOB_01, NODE_01, _mutate_io_scratch_too_small),
        (JOB_01, NODE_01, _mutate_tag_eval_error),
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
        (JOB_07, NODE_03, _mutate_io_lan),
        (JOB_07, NODE_03, _mutate_io_scratch),
        (JOB_07, NODE_03, _mutate_user_namespaces),
    ],
)
def test_valid_job_with_node_failure_branches(job_path, node_path, mutator):
    try:
        job, node = _build_models(job_path, node_path, mutator=mutator)
    except ValidationError:
        return

    job_id = job_path.split("/")[-1].rstrip(".yaml")
    assert not vp.valid_job_specs_with_node(job_id, job, node)


def test_valid_job_success_with_example_file():
    assert vp.valid_job(JOB_01)


def test_valid_job_failure_paths():
    assert not vp.valid_job("tests/examples/jobs/invalid_05_job_empty_specs.yaml")
    assert not vp.valid_job("tests/examples/jobs/invalid_01_job_min_gt_max.yaml")
    assert not vp.valid_job("tests/examples/jobs/does_not_exist.yaml")


def test_valid_node_success_with_example_file():
    assert vp.valid_node(NODE_01)


def test_valid_node_failure_paths():
    assert not vp.valid_node("tests/examples/nodes/invalid_07_node_negative_cores.yaml")
    assert not vp.valid_node("tests/examples/nodes/does_not_exist.yaml")


def test_evaluate_node_returns_false_for_unsupported_expression_node():
    # evaluate_tag_expression now catches ValueError/SyntaxError and returns False
    assert not tags.evaluate_tag_expression("a + b", {"a", "b"})


def test_valid_node_returns_empty_when_job_specs_are_invalid():
    assert vp.match_jobs_with_node("tests/examples/jobs/invalid_01_job_min_gt_max.yaml", NODE_01)[0] is not None
    assert vp.match_jobs_with_node("tests/examples/jobs/invalid_01_job_min_gt_max.yaml", NODE_01)[0] == []


def test_valid_node_returns_empty_when_node_is_invalid():
    assert vp.match_jobs_with_node(JOB_01, "tests/examples/nodes/invalid_07_node_negative_cores.yaml") is None


def test_invalid_version_with_adapter():
    adapter = TypeAdapter(CustomVersion)

    with pytest.raises(ValidationError, match="Invalid version format"):
        adapter.validate_python("version_invalide")


def test_job_model_validation_no_time_or_work():
    job_data = {
        "owner": "test-owner",
        "group": "lhcb_mc",
        "job_type": "User",
        "submission_time": "2026-01-01T12:00:00Z",
        "matching_specs": [
            {
                "system": {"name": "Linux"},
                "cpu": {
                    "num-cores": {"min": 1, "max": 1},
                    "architecture": {"name": "x86_64", "microarchitecture-level": {"min": 1}},
                },
                "tags": "a",
            }
        ],
    }
    with pytest.raises(ValidationError, match="At least one of 'wall-time' or 'cpu-work' must be provided"):
        Job.model_validate(job_data)
