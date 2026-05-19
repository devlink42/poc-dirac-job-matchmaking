#!/usr/bin/env python3

from __future__ import annotations

import redis
from pydantic import ValidationError

from matchmaking.config.logger import logger
from matchmaking.core.match_making import valid_job_specs_with_node
from matchmaking.models.job import Job
from matchmaking.models.node import Node


def match_job_with_redis_nodes(job_json: str, redis_client: redis.Redis) -> list[Node]:
    """Find all compatible nodes from Redis for a given job JSON.
    Mimics pure python logic by pulling all nodes and testing them.
    """
    try:
        job_obj = Job.model_validate_json(job_json)
    except ValidationError as e:
        logger.error("Invalid job specification: %s", e)
        raise

    matched_nodes = []

    # In a pure python/redis approach, we fetch all nodes and test locally
    nodes_data = redis_client.hgetall("py_redis:nodes")

    for _, node_json in nodes_data.items():
        try:
            node_obj = Node.model_validate_json(node_json)
        except ValidationError:
            continue

        # Match each spec in the job against this node
        for i, job_spec in enumerate(job_obj.matching_specs):
            if valid_job_specs_with_node(f"{job_obj.job_id}-{i}", job_spec, node_obj):
                matched_nodes.append(node_obj)
                break

    return matched_nodes


def fetch_and_match(job_id: str, redis_client: redis.Redis) -> list[Node]:
    job_json = redis_client.hget("py_redis:jobs", job_id)
    if not job_json:
        logger.error("Job %s not found in Redis.", job_id)
        return []

    return match_job_with_redis_nodes(job_json, redis_client)
