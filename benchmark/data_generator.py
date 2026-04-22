#!/usr/bin/env python3
"""Data generation module for benchmarking the matchmaking system."""

from __future__ import annotations

import random
from datetime import datetime, timezone
from typing import Iterator

from src.models.job import Architecture, ComputeMemory, Cpu, Job, MatchingSpecs, System
from src.models.node import Architecture as NodeArchitecture
from src.models.node import Cpu as NodeCpu
from src.models.node import Gpu as NodeGpu
from src.models.node import Node
from src.models.node import System as NodeSystem
from src.models.utils import (
    ArchitectureName,
    CustomVersion,
    JobGroup,
    JobType,
    Range,
    ResourceSpec,
    StrictRange,
    SystemName,
)

secure_random = random.SystemRandom()


def generate_mock_job(job_id: str) -> Job:
    """Generate a mock Job object based on hypothetical LHCb distributions.

    Args:
        job_id (str): A unique identifier for the generated job.

    Returns:
        Job: A populated Job model.
    """
    job_type_roll = secure_random.random()
    if job_type_roll < 0.60:
        job_type = JobType.MCSIMULATION
    elif job_type_roll < 0.81:
        job_type = JobType.MCFASTSIMULATION
    elif job_type_roll < 0.97:
        job_type = JobType.WGPRODUCTION
    else:
        job_type = secure_random.choice([JobType.USER, JobType.SPRUCING, JobType.MERGE, JobType.LBAPI])

    owner_roll = secure_random.random()
    if owner_roll < 0.80:
        owner = "lbprods"
        group = JobGroup.LHCB_MC
    elif owner_roll < 0.98:
        owner = "lbprods"
        group = JobGroup.LHCB_DATA
    else:
        owner = secure_random.choice(["sharmar", "jdoe", "asmith"])
        group = JobGroup.LHCB_USER

    sites = [
        "LCG.CERN.cern",
        "LCG.IN2P3.fr",
        "LCG.RAL.uk",
        "LCG.GRIDKA.de",
        "LCG.CNAF.it",
        "LCG.NCBJ.pl",
        "LCG.CSCS.ch",
        "LCG.Beijing.cn",
    ]

    cpu_work_options = [259200, 345600, 1080000, 21600]
    cpu_work = secure_random.choice(cpu_work_options)

    return Job(
        job_id=job_id,
        owner=owner,
        group=group,
        job_type=job_type,
        submission_time=datetime.now(tz=timezone.utc),
        matching_specs=[
            MatchingSpecs(
                **{
                    "site": secure_random.choice(sites),
                    "system": System(name=SystemName.LINUX),
                    "wall-time": cpu_work + 3600,
                    "cpu-work": cpu_work // 100,
                    "cpu": Cpu(
                        **{
                            "num-cores": StrictRange(min=1, max=secure_random.choice([1, 2, 4, 8])),
                            "ram-mb": ComputeMemory(
                                request=ResourceSpec(overhead=2000), limit=ResourceSpec(overhead=4000)
                            ),
                            "architecture": Architecture(
                                **{
                                    "name": ArchitectureName.x86_64,
                                    "microarchitecture-level": Range(min=secure_random.randint(1, 2), max=None),
                                }
                            ),
                        }
                    ),
                    "tags": secure_random.choice(["cvmfs:lhcb", "cvmfs:lhcb & os:el9", "os:el9", ""]),
                }
            )
        ],
    )


def job_generator(count: int) -> Iterator[Job]:
    """Memory-efficient generator for mock jobs."""
    for i in range(count):
        yield generate_mock_job(f"job-{i}")


def generate_mock_node(node_id: str) -> Node:
    """Generate a mock Node (Pilot) object.

    Args:
        node_id (str): A unique identifier for the generated node.

    Returns:
        Node: A populated Node model.
    """
    return Node(
        **{
            "node_id": node_id,
            "site": secure_random.choice(
                [
                    "LCG.CERN.cern",
                    "LCG.IN2P3.fr",
                    "LCG.RAL.uk",
                    "LCG.GRIDKA.de",
                    "LCG.CNAF.it",
                    "LCG.NCBJ.pl",
                    "LCG.CSCS.ch",
                    "LCG.Beijing.cn",
                ]
            ),
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
                        **{"name": ArchitectureName.x86_64, "microarchitecture-level": secure_random.randint(2, 4)}
                    ),
                }
            ),
            "gpu": NodeGpu(count=0),
            "tags": ["cvmfs:lhcb", "os:el9", "production", "tier1"],
        }
    )
