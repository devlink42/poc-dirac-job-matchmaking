#!/usr/bin/env python3

from __future__ import annotations

import ast
from copy import deepcopy

import pytest
import yaml
from pydantic import TypeAdapter, ValidationError

from src.core import valid_pilot as vp
from src.models.job import MatchingSpecs
from src.models.node import Node
from src.models.utils import CustomVersion

JOB_01 = "tests/examples/jobs/job_01_mcsimulation_any_site.yaml"
JOB_06 = "tests/examples/jobs/job_06_gpu.yaml"
JOB_07 = "tests/examples/jobs/job_07_sprucing_niche.yaml"
PILOT_01 = "tests/examples/nodes/pilot_01_cern_typical.yaml"
PILOT_03 = "tests/examples/nodes/pilot_03_gpu.yaml"


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

    return MatchingSpecs.model_validate(job_spec), Node.model_validate(node_spec)


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


@pytest.mark.parametrize(
    "job_path,node_path,mutator",
    [
        (JOB_01, PILOT_01, _mutate_missing_plain_tags),
        (JOB_01, PILOT_01, _mutate_invalid_tag_expression),
        (JOB_06, PILOT_03, _mutate_gpu_ram),
        (JOB_06, PILOT_03, _mutate_ram_limit),
        (JOB_06, PILOT_03, _mutate_gpu_vendor),
        (JOB_06, PILOT_03, _mutate_cpu_cores_min),
        (JOB_06, PILOT_03, _mutate_microarch_min),
        (JOB_06, PILOT_03, _mutate_microarch_max),
        (JOB_06, PILOT_03, _mutate_gpu_count_max),
        (JOB_06, PILOT_03, _mutate_architecture_name),
        (JOB_06, PILOT_03, _mutate_gpu_driver_version),
        (JOB_06, PILOT_03, _mutate_gpu_compute_capability_min),
        (JOB_06, PILOT_03, _mutate_gpu_compute_capability_max),
        (JOB_07, PILOT_03, _mutate_io_lan),
        (JOB_07, PILOT_03, _mutate_io_scratch),
        (JOB_07, PILOT_03, _mutate_user_namespaces),
    ],
)
def test_valid_job_with_node_failure_branches(job_path, node_path, mutator):
    job, node = _build_models(job_path, node_path, mutator=mutator)
    job_id = job_path.split("/")[-1].rstrip(".yaml")

    assert not vp.valid_job_with_node(job_id, job, node)


def test_valid_job_success_with_example_file():
    assert vp.valid_job(JOB_01)


def test_valid_job_failure_paths():
    assert not vp.valid_job("tests/examples/jobs/invalid_05_empty_specs.yaml")
    assert not vp.valid_job("tests/examples/jobs/invalid_01_min_gt_max.yaml")
    assert not vp.valid_job("tests/examples/jobs/does_not_exist.yaml")


def test_valid_node_success_with_example_file():
    assert vp.valid_node(PILOT_01)


def test_valid_node_failure_paths():
    assert not vp.valid_node("tests/examples/nodes/invalid_07_pilot_negative_cores.yaml")
    assert not vp.valid_node("tests/examples/nodes/does_not_exist.yaml")


def test_evaluate_node_raises_for_unsupported_expression_node():
    unsupported_node = ast.parse("a + b", mode="eval").body

    with pytest.raises(ValueError):
        vp._evaluate_node(unsupported_node, "a + b")


def test_valid_pilot_returns_empty_when_job_specs_are_invalid():
    assert vp.valid_pilot("tests/examples/jobs/invalid_01_min_gt_max.yaml", PILOT_01)[0] == []


def test_valid_pilot_returns_empty_when_node_is_invalid():
    assert vp.valid_pilot(JOB_01, "tests/examples/nodes/invalid_07_pilot_negative_cores.yaml") == []


def test_invalid_version_with_adapter():
    adapter = TypeAdapter(CustomVersion)

    with pytest.raises(ValidationError, match="Invalid version format"):
        adapter.validate_python("version_invalide")
