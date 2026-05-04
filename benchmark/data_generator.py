#!/usr/bin/env python3
"""Data generation module for benchmarking the matchmaking system."""

from __future__ import annotations

import random
from datetime import datetime, timezone
from typing import Iterator

from matchmaking.models.job import Architecture, ComputeMemory, Cpu, Job, MatchingSpecs, System
from matchmaking.models.node import Architecture as NodeArchitecture
from matchmaking.models.node import Cpu as NodeCpu
from matchmaking.models.node import Gpu as NodeGpu
from matchmaking.models.node import Node
from matchmaking.models.node import System as NodeSystem
from matchmaking.models.utils import (
    ArchitectureName,
    CustomVersion,
    JobGroup,
    JobType,
    Range,
    ResourceSpec,
    StrictRange,
    SystemName,
)

# Standard RNG is sufficient for benchmark data; SystemRandom (os.urandom) is
# reserved for cryptographic use and adds unnecessary syscall overhead here.
_rng = random.Random()  # noqa: S311

# Module-level constants avoid re-allocating identical lists on every call.
_SITES = [
    "LCG.CERN.cern",
    "LCG.IN2P3.fr",
    "LCG.RAL.uk",
    "LCG.GRIDKA.de",
    "LCG.CNAF.it",
    "LCG.NCBJ.pl",
    "LCG.CSCS.ch",
    "LCG.Beijing.cn",
]
_TAG_POOL = [f"tag:{i:03d}" for i in range(200)]
_CPU_WORK_OPTIONS = [259200, 345600, 1080000, 21600]
_RARE_JOB_TYPES = [JobType.USER, JobType.SPRUCING, JobType.MERGE, JobType.LBAPI]
_RARE_OWNERS = ["sharmar", "jdoe", "asmith"]


def generate_mock_job(job_id: str) -> Job:
    """Generate a mock Job object based on hypothetical LHCb distributions.

    Args:
        job_id: A unique identifier for the generated job.

    Returns:
        A populated Job model.
    """
    roll = _rng.random()
    if roll < 0.60:
        job_type = JobType.MCSIMULATION
    elif roll < 0.81:
        job_type = JobType.MCFASTSIMULATION
    elif roll < 0.97:
        job_type = JobType.WGPRODUCTION
    else:
        job_type = _rng.choice(_RARE_JOB_TYPES)

    roll = _rng.random()
    if roll < 0.80:
        owner, group = "lbprods", JobGroup.LHCB_MC
    elif roll < 0.98:
        owner, group = "lbprods", JobGroup.LHCB_DATA
    else:
        owner, group = _rng.choice(_RARE_OWNERS), JobGroup.LHCB_USER

    tags = ["cvmfs:lhcb", "os:el9"]
    if _rng.random() < 0.3:
        tags.extend(_rng.sample(_TAG_POOL, _rng.randint(1, 3)))
    tag_expr = " & ".join(tags)
    if _rng.random() < 0.1:
        tag_expr += " & (feature:A | feature:B)"

    cpu_work = _rng.choice(_CPU_WORK_OPTIONS)

    return Job(
        job_id=job_id,
        owner=owner,
        group=group,
        job_type=job_type,
        submission_time=datetime.now(tz=timezone.utc),
        matching_specs=[
            MatchingSpecs(
                **{
                    "site": _rng.choice(_SITES),
                    "system": System(name=SystemName.LINUX),
                    "wall-time": cpu_work + 3600,
                    "cpu-work": cpu_work // 100,
                    "cpu": Cpu(
                        **{
                            "num-cores": StrictRange(min=1, max=_rng.choice([1, 2, 4, 8])),
                            "ram-mb": ComputeMemory(
                                request=ResourceSpec(overhead=2000),
                                limit=ResourceSpec(overhead=4000),
                            ),
                            "architecture": Architecture(
                                **{
                                    "name": ArchitectureName.x86_64,
                                    "microarchitecture-level": Range(min=_rng.randint(1, 2), max=None),
                                }
                            ),
                        }
                    ),
                    "tags": tag_expr,
                }
            )
        ],
    )


def generate_mock_node(node_id: str) -> Node:
    """Generate a mock Node (Pilot) object.

    Args:
        node_id: A unique identifier for the generated node.

    Returns:
        A populated Node model.
    """
    node_tags = ["cvmfs:lhcb", "os:el9", "production", "tier1"]
    if _rng.random() < 0.5:
        node_tags.extend(_rng.sample(_TAG_POOL, _rng.randint(10, 20)))
    if _rng.random() < 0.2:
        node_tags.append("feature:A")

    return Node(
        **{
            "node_id": node_id,
            "site": _rng.choice(_SITES),
            "system": NodeSystem(
                **{"name": SystemName.LINUX, "glibc": CustomVersion(version="2.17"), "user-namespaces": True}
            ),
            "wall-time": 86400 * 7,
            "cpu-work": 1000000,
            "cpu": NodeCpu(
                **{
                    "num-nodes": 1,
                    "num-cores": 64,
                    "ram-mb": 128000,
                    "architecture": NodeArchitecture(
                        **{"name": ArchitectureName.x86_64, "microarchitecture-level": _rng.randint(2, 4)}
                    ),
                }
            ),
            "gpu": NodeGpu(count=0),
            "tags": node_tags,
        }
    )


def node_generator(count: int) -> Iterator[Node]:
    """Memory-efficient generator for mock nodes."""
    for i in range(count):
        yield generate_mock_node(f"node-{i}")
