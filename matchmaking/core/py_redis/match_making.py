#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path

from pydantic import ValidationError

from matchmaking.config.logger import logger
from matchmaking.core.match_making import valid_job_specs_with_node
from matchmaking.models.job import Job
from matchmaking.models.node import Node


def match_job_with_node_redis(jobs: list[Job], node: str) -> tuple[list[Job], Node]:
    """Match jobs with a specific node using Redis data structures.

    Args:
        jobs (list[Job]): List of job objects to match.
        node (str): Path to the node specification file.

    Returns:
        tuple[list[Job], Node]: Tuple containing list of matched jobs and the node object.
    """
    try:
        node_obj = Node.load_from_yaml(node)
        logger.info("Node file %s is VALID.", node)
    except ValidationError as e:
        logger.error("Invalid node specification: %s", e)
        raise

    if not node_obj.node_id:
        node_obj.node_id = Path(node).stem
        logger.warning("Node ID not specified in %s, using filename as default: %s", node, node_obj.node_id)

    jobs_match = []

    try:
        for job in jobs:
            for i, job_spec in enumerate(job.matching_specs):
                if valid_job_specs_with_node(f"{job.job_id}-{i}", job_spec, node_obj):
                    jobs_match.append(job)
                    logger.info("Job %s-%s matches node %s.", job.job_id, i, node_obj.node_id)
    except ValidationError as e:
        logger.error("Invalid job specification: %s", e)
        return [], node_obj

    return jobs_match, node_obj
