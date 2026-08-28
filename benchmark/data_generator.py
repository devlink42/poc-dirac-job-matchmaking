#!/usr/bin/env python3
"""Data generation module for benchmarking the matchmaking system."""

from __future__ import annotations

import random
from collections.abc import Iterator
from datetime import UTC, datetime

from matchmaking.models.job import Architecture, ComputeMemory, Cpu, Job, MatchingSpecs, System
from matchmaking.models.node import Architecture as NodeArchitecture
from matchmaking.models.node import Cpu as NodeCpu
from matchmaking.models.node import Gpu as NodeGpu
from matchmaking.models.node import Node
from matchmaking.models.node import System as NodeSystem
from matchmaking.models.utils import (
    ArchitectureName,
    CustomVersion,
    Range,
    ResourceSpec,
    StrictRange,
    SystemName,
    Type,
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
_CPU_WORK_OPTIONS = [259200, 345600, 1080000, 21600]
_RARE_JOB_TYPES = [
    Type.USER,
    Type.SPRUCING,
    Type.MERGE,
    Type.MCRECONSTRUCTION,
    Type.APMERGE,
    Type.APPOSTPROC,
    Type.MCMERGE,
    Type.LBAPI,
]
_OWNERS = ["sharmar", "jdoe", "asmith"]

_BASE_TAGS = ("cvmfs:lhcb", "os:el9")
_TAG_CAPABILITIES = (
    "cvmfs:lhcbdev",
    "os:el10",
    "os:alma9",
    "os:alma10",
    "os:ubuntu22",
    "os:ubuntu24",
    "os:ubuntu26",
    "gpu:nvidia",
    "gpu:amd",
    "gpu:intel",
    "~diracx:banned:LCG.NIPNE-07.ro",
    "~diracx:banned:LCG.GRIDKA.de",
    "~diracx:banned:LCG.NCBJ.pl",
)

# Exactly 15 stable requirement profiles. Each one expresses 5 to 7
# alternatives; a compatible node needs one of the alternatives in addition to
# the base requirements. Keeping this catalogue bounded makes aggregation
# experiments meaningful and reproducible.
_TAG_EXPRESSION_ALTERNATIVES = tuple(
    tuple(
        _TAG_CAPABILITIES[(profile_index + tag_index) % len(_TAG_CAPABILITIES)]
        for tag_index in range(5 + profile_index % 3)
    )
    for profile_index in range(len(_TAG_CAPABILITIES))
)
_tag_expression_count = len(_TAG_EXPRESSION_ALTERNATIVES)


def set_seed(seed: int) -> None:
    """Set the seed for the random number generator.

    Args:
        seed: The seed value to use.
    """
    _rng.seed(seed)


def _generate_job_tag_expression() -> str:
    """Generate one job tag expression from the bounded profile catalogue."""
    alternatives = _rng.choice(_TAG_EXPRESSION_ALTERNATIVES[:_tag_expression_count])
    base_requirements = " & ".join(_BASE_TAGS)
    alternative_requirements = " | ".join(alternatives)

    return f"{base_requirements} & ({alternative_requirements})"


def generate_mock_job(job_id: str) -> Job:
    """Generate a mock Job object based on hypothetical LHCb distributions.

    Args:
        job_id: A unique identifier for the generated job.

    Returns:
        A populated Job model.
    """
    roll = _rng.random()
    if roll < 0.80:
        owner, group = "lbprods", "lhcb_mc"
    elif roll < 0.98:
        owner, group = "lbprods", "lhcb_data"
    else:
        owner, group = _rng.choice(_OWNERS), "lhcb_user"

    roll = _rng.random()
    if roll < 0.60:
        job_type = Type.MCSIMULATION
    elif roll < 0.81:
        job_type = Type.MCFASTSIMULATION
    elif roll < 0.97:
        job_type = Type.WGPRODUCTION
    else:
        job_type = _rng.choice(_RARE_JOB_TYPES)

    roll = _rng.random()
    if roll < 0.85:
        site = None
    else:
        site = _rng.choice(_SITES)

    cpu_work = _rng.choice(_CPU_WORK_OPTIONS)

    tag_expr = _generate_job_tag_expression()

    return Job(
        job_id=job_id,
        submit_time=datetime.now(tz=UTC),
        owner=owner,
        group=group,
        type=job_type,
        matching_specs=[
            MatchingSpecs(
                **{
                    "site": site,
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
    node_tags = [*_BASE_TAGS, *_rng.sample(_TAG_CAPABILITIES, _rng.randint(3, 5))]

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


def job_generator(count: int) -> Iterator[Job]:
    """Memory-efficient generator for mock jobs."""
    for i in range(count):
        yield generate_mock_job(f"job-{i}")


def node_generator(count: int) -> Iterator[Node]:
    """Memory-efficient generator for mock nodes."""
    for i in range(count):
        yield generate_mock_node(f"node-{i}")
