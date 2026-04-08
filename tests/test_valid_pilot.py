#!/usr/bin/env python3

from src.core.valid_pilot import valid_job_with_node, valid_pilot
from src.models.job import Job
from src.models.node import Node


def test_valid_job_with_node_success():
    # Setup node
    node_data = {
        "node_id": "test-node",
        "system":                                                                                                   {
            "name": "Linux", "glibc": 2.31, "user-namespaces": True},
        "wall-time":                                                                                                3600,
        "cpu-work":                                                                                                 1000,
        "cpu":                                                                                                      {
            "num-nodes": 1,
            "num-cores": 8,
            "ram-mb": 8192,
            "architecture": {
                "name": "x86_64",
                "microarchitecture-level": 3
            }
        },
        "gpu": {"count": 0},
        "tags": ["cvmfs:lhcb", "os:el9"]
    }

    node = Node.model_validate(node_data)

    # Setup job
    job_data = {
        "job_id": "test-job",
        "system": {"name": "Linux", "glibc": 2.28, "user-namespaces": True},
        "wall-time": 1800,
        "cpu-work": 500,
        "cpu": {
            "num-cores": {"min": 1, "max": 8},
            "ram-mb": {
                "request": {"overhead": 1024, "per-core": 128},
                "limit": {"overhead": 2048, "per-core": 256}
            },
            "architecture": {
                "name": "x86_64",
                "microarchitecture-level": {"min": 2, "max": 4}
            }
        },
        "tags": "cvmfs:lhcb"
    }

    job = Job.model_validate(job_data)

    assert valid_job_with_node(job, node) is True


def test_valid_job_with_node_system_mismatch():
    node = Node.model_validate(
        {
            "node_id":                                                                                            "node",
            "system":                                                                                          {
                "name": "Linux", "glibc": 2.31, "user-namespaces": True},
            "wall-time": 100,
            "cpu-work": 100,
            "cpu": {
                "num-nodes": 1,
                "num-cores": 1,
                "ram-mb": 1024,
                "architecture": {"name": "x86_64", "microarchitecture-level": 1}
            },
            "gpu": {"count": 0},
            "tags": []
        }
    )

    # OS mismatch
    job = Job.model_validate(
        {
            "job_id":      "job",
            "system": {"name": "Darwin", "glibc": 2.31, "user-namespaces": True},
            "wall-time": 100,
            "cpu-work": 100,
            "cpu": {
                "num-cores": {"min": 1, "max": 1},
                "architecture": {"name": "x86_64", "microarchitecture-level": {"min": 1}}
            },
            "tags": ""
        }
    )

    assert valid_job_with_node(job, node) is False

    # GLIBC too old on node
    job = Job.model_validate(
        {
            "job_id": "job",
            "system": {"name": "Linux", "glibc": 2.35, "user-namespaces": True},
            "wall-time": 100,
            "cpu-work": 100,
            "cpu": {
                "num-cores": {"min": 1, "max": 1},
                "architecture": {"name": "x86_64", "microarchitecture-level": {"min": 1}}
            },
            "tags": ""
        }
    )

    assert valid_job_with_node(job, node) is False


def test_valid_job_with_node_cpu_arch_mismatch():
    node = Node.model_validate(
        {
            "node_id": "node",
            "system": {"name": "Linux", "glibc": 2.31, "user-namespaces": True},
            "wall-time": 100,
            "cpu-work": 100,
            "cpu": {
                "num-nodes": 1,
                "num-cores": 1,
                "ram-mb": 1024,
                "architecture": {"name": "x86_64", "microarchitecture-level": 1}
            },
            "gpu": {"count": 0},
            "tags": []
        }
    )

    # Arch level too high for node
    job = Job.model_validate(
        {
            "job_id": "job",
            "system": {"name": "Linux", "glibc": 2.28, "user-namespaces": True},
            "wall-time": 100,
            "cpu-work": 100,
            "cpu": {
                "num-cores": {"min": 1, "max": 1},
                "architecture": {"name": "x86_64", "microarchitecture-level": {"min": 3}}
            },
            "tags": ""
        }
    )

    assert valid_job_with_node(job, node) is False


def test_valid_job_with_node_ram_fail():
    node = Node.model_validate(
        {
            "node_id": "node",
            "system": {
                "name": "Linux", "glibc": 2.31, "user-namespaces": True},
            "wall-time": 100,
            "cpu-work": 100,
            "cpu": {
                "num-nodes": 1, "num-cores": 2, "ram-mb": 1000,
                "architecture": {"name": "x86_64", "microarchitecture-level": 1}
            },
            "gpu": {"count": 0},
            "tags": []
        }
    )

    # RAM required: overhead(500) + per_core(300)*2 = 1100 > node RAM (1000)
    job = Job.model_validate(
        {
            "job_id": "job",
            "system": {"name": "Linux", "glibc": 2.28, "user-namespaces": True},
            "wall-time": 100,
            "cpu-work": 100,
            "cpu": {
                "num-cores": {"min": 1, "max": 1},
                "ram-mb": {
                    "request": {"overhead": 500, "per-core": 300},
                    "limit": {"overhead": 1000, "per-core": 500}
                },
                "architecture": {
                    "name": "x86_64",
                    "microarchitecture-level": {"min": 1}
                }
            },
            "tags": ""}
    )

    assert valid_job_with_node(job, node) is False


def test_valid_job_with_node_tags():
    node = Node.model_validate(
        {
            "node_id": "node",
            "system": {
                "name": "Linux", "glibc": 2.31, "user-namespaces": True
            },
            "wall-time": 100,
            "cpu-work": 100,
            "cpu": {
                "num-nodes": 1,
                "num-cores": 1,
                "ram-mb": 1024,
                "architecture": {"name": "x86_64", "microarchitecture-level": 1}
            },
            "gpu": {"count": 0},
            "tags": ["tag1", "tag2"]
        }
    )

    # Missing tag
    job = Job.model_validate(
        {
            "job_id": "job",
            "system":
                {"name": "Linux", "glibc": 2.28, "user-namespaces": True},
            "wall-time": 100,
            "cpu-work": 100,
            "cpu": {
                "num-cores": {"min": 1, "max": 1},
                "architecture": {"name": "x86_64", "microarchitecture-level": {"min": 1}}},
            "tags": "tag1 tag3"
        }
    )

    assert valid_job_with_node(job, node) is False

    # Matching tags
    job = Job.model_validate(
        {
            "job_id": "job",
            "system": {"name": "Linux", "glibc": 2.28, "user-namespaces": True},
            "wall-time": 100,
            "cpu-work": 100,
            "cpu": {
                "num-cores": {"min": 1, "max": 1},
                "architecture": {"name": "x86_64", "microarchitecture-level": {"min": 1}}},
            "tags": "tag1 tag2"
        }
    )

    assert valid_job_with_node(job, node) is True


def test_valid_pilot_from_files():
    matches = valid_pilot(
        "tests/examples/jobs/job_01_mcsimulation_any_site.yaml",
        "tests/examples/nodes/pilot_01_cern_typical.yaml"
    )

    assert len(matches) == 1
