#!/usr/bin/env python3

from __future__ import annotations

import yaml
from pydantic import ValidationError

from matchmaking.config.logger import logger
from matchmaking.logic.tags import evaluate_tag_expression
from matchmaking.models.job import Job
from matchmaking.models.node import Node


def _eval_tag_expression(expr: str, node_tags: set[str]) -> bool:
    """Evaluate a simple boolean expression of tags against a set of node tags.

    Supported syntax examples:
      - "a & b"
      - "a | (b & c)"
      - "~a"
      - Operators: '&' for AND, '|' for OR, '~' for NOT, parentheses for grouping
    """
    return evaluate_tag_expression(expr, node_tags)


def valid_job_with_node(job: Job, node: Node) -> bool:
    # Site check
    if job.site and job.site != node.site:
        logger.warning(f"Job {job.job_id} requires to be on site {job.site}, skipping...")
        return False

    # System check
    if job.system.name != node.system.name:
        logger.warning(f"Job {job.job_id} requires to be on system {job.system.name}, skipping...")
        return False

    if job.system.glibc and node.system.glibc < job.system.glibc:
        logger.warning(f"Job {job.job_id} requires glibc >= {job.system.glibc}, skipping...")
        return False

    if job.system.user_namespaces and not node.system.user_namespaces:
        logger.warning(f"Job {job.job_id} requires user namespaces to be {job.system.user_namespaces}, skipping...")
        return False

    # Wall time check
    if job.wall_time and node.wall_time < job.wall_time:
        logger.warning(f"Job {job.job_id} requires {job.wall_time} wall time, skipping...")
        return False

    # CPU work check
    if job.cpu_work and node.cpu_work < job.cpu_work:
        logger.warning(f"Job {job.job_id} requires {job.cpu_work} CPU work, skipping...")
        return False

    # CPU Cores check
    if node.cpu.num_cores < job.cpu.num_cores.min:
        logger.warning(f"Job {job.job_id} requires at least {job.cpu.num_cores.min} CPU cores, skipping...")
        return False

    # RAM check
    if job.cpu.ram_mb and (required_ram_request := job.cpu.ram_mb.request.overhead):
        if job.cpu.ram_mb.request.per_core:
            required_ram_request += job.cpu.ram_mb.request.per_core * job.cpu.num_cores.max

        if node.cpu.ram_mb < required_ram_request:
            logger.warning(f"Job {job.job_id} requires at least {required_ram_request} MB RAM, skipping...")
            return False

        if ram_limit := job.cpu.ram_mb.limit.overhead:
            if job.cpu.ram_mb.limit.per_core:
                ram_limit += job.cpu.ram_mb.limit.per_core * job.cpu.num_cores.max

            if node.cpu.ram_mb < ram_limit:
                logger.warning(f"Job {job.job_id} requires at least {ram_limit} MB RAM, skipping...")
                return False

    # Architecture check
    if job.cpu.architecture.name != node.cpu.architecture.name:
        logger.warning(f"Job {job.job_id} requires architecture {job.cpu.architecture.name}, skipping...")
        return False

    if node.cpu.architecture.microarchitecture_level < job.cpu.architecture.microarchitecture_level.min:
        logger.warning(
            f"Job {job.job_id} requires at least microarchitecture level "
            f"{job.cpu.architecture.microarchitecture_level.min}, skipping..."
        )
        return False

    if (
        job.cpu.architecture.microarchitecture_level.max
        and node.cpu.architecture.microarchitecture_level > job.cpu.architecture.microarchitecture_level.max
    ):
        logger.warning(
            f"Job {job.job_id} requires at most microarchitecture level "
            f"{job.cpu.architecture.microarchitecture_level.max}, skipping..."
        )
        return False

    # GPU check (if required)
    if job.gpu:
        if node.gpu.count < job.gpu.count.min:
            logger.warning(f"Job {job.job_id} requires at least {job.gpu.count.min} GPUs, skipping...")
            return False

        if job.gpu.count.max and node.gpu.count > job.gpu.count.max:
            logger.warning(f"Job {job.job_id} requires at most {job.gpu.count.max} GPUs, skipping...")
            return False

        if node.gpu.count > 0:
            if job.gpu.ram_mb and node.gpu.ram_mb and node.gpu.ram_mb < job.gpu.ram_mb:
                logger.warning(f"Job {job.job_id} requires at least {job.gpu.ram_mb} MB GPU RAM, skipping...")
                return False

            if job.gpu.vendor != node.gpu.vendor:
                logger.warning(f"Job {job.job_id} requires GPU vendor {job.gpu.vendor}, skipping...")
                return False

            if (
                job.gpu.compute_capability.min
                and node.gpu.compute_capability
                and node.gpu.compute_capability < job.gpu.compute_capability.min
            ):
                logger.warning(
                    f"Job {job.job_id} requires at least compute capability "
                    f"{job.gpu.compute_capability.min}, skipping..."
                )
                return False

            if (
                job.gpu.compute_capability.max
                and node.gpu.compute_capability
                and node.gpu.compute_capability > job.gpu.compute_capability.max
            ):
                logger.warning(
                    f"Job {job.job_id} requires at most compute capability "
                    f"{job.gpu.compute_capability.max}, skipping..."
                )
                return False

            if job.gpu.driver_version and node.gpu.driver_version and node.gpu.driver_version < job.gpu.driver_version:
                logger.warning(
                    f"Job {job.job_id} requires at least driver version {job.gpu.driver_version}, skipping..."
                )
                return False

    # IO check
    if job.io and node.io:
        if node.io.scratch_mb < job.io.scratch_mb:
            logger.warning(f"Job {job.job_id} requires at least {job.io.scratch_mb} MB scratch space, skipping...")
            return False

    # Tags check (all job tags must be present in node tags)
    if job.tags:
        node_tags = set(node.tags)
        logger.debug(f"Node {node.node_id} has tags: {node_tags}")
        logger.debug(f"Job {job.job_id} has tags: {job.tags}")

        if any(op in job.tags for op in ("&", "|", "~", "(", ")")):
            if not _eval_tag_expression(job.tags, node_tags):
                logger.warning(f"Job {job.job_id} has invalid tag expression, skipping...")
                return False
        else:
            job_tags = set(tag.strip() for tag in job.tags.split())
            logger.debug(f"Job {job.job_id} has tags: {job_tags}")
            if not (job_tags <= node_tags):
                logger.warning(f"Job {job.job_id} has missing tags, skipping...")
                return False

    return True


def match_jobs_with_node(job: str, node: str) -> list[Job]:
    """Validate a job against a node configuration.

    Args:
        job (str): Path to the job YAML file.
        node (str): Path to the node YAML file.

    Returns:
        list[Job]: List of matching jobs if validation is successful, otherwise an empty list.
    """
    with open(job, "r") as job_file, open(node, "r") as node_file:
        yaml_job = yaml.safe_load(job_file)
        yaml_node = yaml.safe_load(node_file)

    # Ensure node has a node_id for validation if missing
    if "node_id" not in yaml_node:
        yaml_node["node_id"] = "unknown-node-id"

    try:
        node_obj = Node.model_validate(yaml_node)
    except ValidationError as e:
        logger.error(f"Invalid node specification: {e}")
        return []

    jobs = yaml_job.get("matching_specs", [])
    jobs_match = []

    for job_spec in jobs:
        try:
            # Add a dummy job_id if not present for validation
            if "job_id" not in job_spec:
                job_spec["job_id"] = "unknown-job-id"

            job_obj = Job.model_validate(job_spec)

            if valid_job_with_node(job_obj, node_obj):
                jobs_match.append(job_obj)
        except ValidationError as e:
            logger.error(f"Invalid job specification: {e}")
            continue

    return jobs_match
