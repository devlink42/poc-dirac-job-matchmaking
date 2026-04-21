#!/usr/bin/env python3
"""Data generation module for benchmarking the matchmaking system."""

from __future__ import annotations

import random
from datetime import datetime

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
    sites = ["LCG.CERN.ch", "LCG.IN2P3.fr", "LCG.RAL.uk", None]

    return Job(
        job_id=job_id,
        owner="lhcb-user",
        group=JobGroup.LHCB_MC,
        job_type=JobType.MCSIMULATION,
        submission_time=datetime.now(tz=datetime.now().astimezone().tzinfo),
        matching_specs=[
            MatchingSpecs(
                **{
                    "site": secure_random.choice(sites),
                    "system": System(name=SystemName.LINUX),
                    "wall-time": secure_random.randint(3600, 86400),
                    "cpu-work": secure_random.randint(100, 1000),
                    "cpu": Cpu(
                        **{
                            "num-cores": StrictRange(min=1, max=8),
                            "ram-mb": ComputeMemory(
                                request=ResourceSpec(overhead=2000), limit=ResourceSpec(overhead=4000)
                            ),
                            "architecture": Architecture(
                                **{"name": ArchitectureName.x86_64, "microarchitecture-level": Range(min=2, max=4)}
                            ),
                        }
                    ),
                    "tags": "",
                }
            )
        ],
    )


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
            "site": secure_random.choice(["LCG.CERN.ch", "LCG.IN2P3.fr", "LCG.RAL.uk"]),
            "system": NodeSystem(
                **{"name": SystemName.LINUX, "glibc": CustomVersion(version="2.17"), "user-namespaces": True}
            ),
            "wall-time": 86400,
            "cpu-work": 2000,
            "cpu": NodeCpu(
                **{
                    "num-nodes": 1,
                    "num-cores": 8,
                    "ram-mb": 16000,
                    "architecture": NodeArchitecture(**{"name": ArchitectureName.x86_64, "microarchitecture-level": 3}),
                }
            ),
            "gpu": NodeGpu(count=0),
            "tags": ["production", "tier1"],
        }
    )
