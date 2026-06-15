#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path

from pydantic import ValidationError

from matchmaking.config.logger import logger
from matchmaking.models.job import Job
from matchmaking.models.node import Node


def load_node(node: str | Node) -> Node:
    if isinstance(node, Node):
        return node

    try:
        node_obj = Node.load_from_yaml(node)
        logger.info(f"Node file {node} is VALID.")
    except ValidationError as e:
        logger.error(f"Invalid node specification: {e}")

        raise

    if not node_obj.node_id:
        node_obj.node_id = Path(node).stem
        logger.warning(f"Node ID not specified in {node}, using filename as default: {node_obj.node_id}")

    return node_obj


def load_job(job: str | Job) -> Job:
    if isinstance(job, Job):
        return job

    try:
        job_obj = Job.load_from_yaml(job)
        logger.info(f"Job file {job} is VALID.")
    except ValidationError as e:
        logger.error(f"Invalid job specification: {e}")

        raise

    if not job_obj.job_id:
        job_obj.job_id = Path(job).stem
        logger.warning(f"Job ID not specified in {job}, using filename as default: {job_obj.job_id}")

    return job_obj
