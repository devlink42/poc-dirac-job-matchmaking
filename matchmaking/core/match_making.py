#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path
from typing import Any

from pydantic import ValidationError

from matchmaking.config.logger import logger
from matchmaking.logic.tags import evaluate_tag_expression
from matchmaking.models.job import Job, MatchingSpecs
from matchmaking.models.node import Node


def valid_job(job: str) -> bool:
    """Validate a job against a set of requirements."""
    try:
        job_obj = Job.load_from_yaml(job)
        job_id = job_obj.job_id or Path(job).stem
        logger.info("Job %s is VALID.", job_id)

        return True
    except Exception as e:
        logger.error("Error validating job specification: %s", e)
        return False


def valid_node(node: str) -> bool:
    """Validate a node against a set of requirements."""
    try:
        Node.load_from_yaml(node)
        logger.info("Node file %s is VALID.", node)

        return True
    except Exception as e:
        logger.error("Error validating node: %s", e)
        return False


def valid_job_specs_with_node(job_id: str | Any, job_specs: MatchingSpecs, node: Node) -> bool:
    """Determine whether a given job is compatible with a specific node based on
    various requirements and constraints.

    Evaluate the compatibility of a job with a node by verifying criteria such
    as site, system requirements, CPU capabilities, memory requirements, GPU
    attributes, IO bandwidth, and specific tags.

    Args:
        job_id (str | Any): The unique identifier of the job.
        job_specs (Job): The job object containing the requirements for execution.
        node (Node): The node object representing the computational resource.

    Returns:
        bool: True if the job is compatible with the node, otherwise False.
    """
    # Site check
    if job_specs.site and job_specs.site != node.site:
        logger.warning("Job %s requires to be on site %s, skipping...", job_id, job_specs.site)
        return False

    # System check
    if job_specs.system.name != node.system.name:
        logger.warning("Job %s requires to be on system %s, skipping...", job_id, job_specs.system.name)
        return False

    if job_specs.system.glibc and node.system.glibc < job_specs.system.glibc:
        logger.warning("Job %s requires glibc >= %s, skipping...", job_id, job_specs.system.glibc)
        return False

    if job_specs.system.user_namespaces and not node.system.user_namespaces:
        logger.warning(
            "Job %s requires user namespaces to be %s, skipping...",
            job_id,
            job_specs.system.user_namespaces,
        )
        return False

    # Wall time check
    if job_specs.wall_time and node.wall_time < job_specs.wall_time:
        logger.warning("Job %s requires %s wall time, skipping...", job_id, job_specs.wall_time)
        return False

    # CPU work check
    if job_specs.cpu_work and node.cpu_work < job_specs.cpu_work:
        logger.warning("Job %s requires %s CPU work, skipping...", job_id, job_specs.cpu_work)
        return False

    # CPU Cores check
    if node.cpu.num_cores < job_specs.cpu.num_cores.min:
        logger.warning("Job %s requires at least %s CPU cores, skipping...", job_id, job_specs.cpu.num_cores.min)
        return False

    # RAM check
    if job_specs.cpu.ram_mb and (required_ram_request := job_specs.cpu.ram_mb.request.overhead):
        if job_specs.cpu.ram_mb.request.per_core:
            required_ram_request += job_specs.cpu.ram_mb.request.per_core * job_specs.cpu.num_cores.max

        if node.cpu.ram_mb < required_ram_request:
            logger.warning("Job %s requires at least %s MB RAM, skipping...", job_id, required_ram_request)
            return False

        if ram_limit := job_specs.cpu.ram_mb.limit.overhead:
            if job_specs.cpu.ram_mb.limit.per_core:
                ram_limit += job_specs.cpu.ram_mb.limit.per_core * job_specs.cpu.num_cores.max

            if node.cpu.ram_mb < ram_limit:
                logger.warning("Job %s requires at least %s MB RAM, skipping...", job_id, ram_limit)
                return False

    # Architecture check
    if job_specs.cpu.architecture.name != node.cpu.architecture.name:
        logger.warning("Job %s requires architecture %s, skipping...", job_id, job_specs.cpu.architecture.name)
        return False

    if node.cpu.architecture.microarchitecture_level < job_specs.cpu.architecture.microarchitecture_level.min:
        logger.warning(
            "Job %s requires at least microarchitecture level %s, skipping...",
            job_id,
            job_specs.cpu.architecture.microarchitecture_level.min,
        )
        return False

    if (
        job_specs.cpu.architecture.microarchitecture_level.max
        and node.cpu.architecture.microarchitecture_level > job_specs.cpu.architecture.microarchitecture_level.max
    ):
        logger.warning(
            "Job %s requires at most microarchitecture level %s, skipping...",
            job_id,
            job_specs.cpu.architecture.microarchitecture_level.max,
        )
        return False

    # GPU check (if required)
    if job_specs.gpu:
        if node.gpu.count < job_specs.gpu.count.min:
            logger.warning("Job %s requires at least %s GPUs, skipping...", job_id, job_specs.gpu.count.min)
            return False

        if job_specs.gpu.count.max and node.gpu.count > job_specs.gpu.count.max:
            logger.warning("Job %s requires at most %s GPUs, skipping...", job_id, job_specs.gpu.count.max)
            return False

        if node.gpu.count > 0:
            if job_specs.gpu.ram_mb and node.gpu.ram_mb and node.gpu.ram_mb < job_specs.gpu.ram_mb:
                logger.warning("Job %s requires at least %s MB GPU RAM, skipping...", job_id, job_specs.gpu.ram_mb)
                return False

            if job_specs.gpu.vendor != node.gpu.vendor:
                logger.warning("Job %s requires GPU vendor %s, skipping...", job_id, job_specs.gpu.vendor)
                return False

            if (
                job_specs.gpu.compute_capability.min
                and node.gpu.compute_capability
                and node.gpu.compute_capability < job_specs.gpu.compute_capability.min
            ):
                logger.warning(
                    "Job %s requires at least compute capability %s, skipping...",
                    job_id,
                    job_specs.gpu.compute_capability.min,
                )
                return False

            if (
                job_specs.gpu.compute_capability.max
                and node.gpu.compute_capability
                and node.gpu.compute_capability > job_specs.gpu.compute_capability.max
            ):
                logger.warning(
                    "Job %s requires at most compute capability %s, skipping...",
                    job_id,
                    job_specs.gpu.compute_capability.max,
                )
                return False

            if (
                job_specs.gpu.driver_version
                and node.gpu.driver_version
                and node.gpu.driver_version < job_specs.gpu.driver_version
            ):
                logger.warning(
                    "Job %s requires at least driver version %s, skipping...",
                    job_id,
                    job_specs.gpu.driver_version,
                )
                return False

    # IO check
    if job_specs.io and node.io:
        if node.io.scratch_mb < job_specs.io.scratch_mb:
            logger.warning("Job %s requires at least %s MB scratch space, skipping...", job_id, job_specs.io.scratch_mb)
            return False

    # Tags check (all job tags must be present in node tags)
    if job_specs.tags:
        node_tags = set(node.tags)
        logger.debug("Node %s has tags: %s", node.node_id, node_tags)
        logger.debug("Job %s has tags: %s", job_id, job_specs.tags)

        if any(op in job_specs.tags for op in ("&", "|", "~", "(", ")")):
            if not evaluate_tag_expression(job_specs.tags, node_tags):
                logger.warning("Job %s has invalid tag expression, skipping...", job_id)
                return False
        else:
            job_tags = set(tag.strip() for tag in job_specs.tags.split())
            logger.debug("Job %s has tags: %s", job_id, job_tags)
            if not (job_tags <= node_tags):
                logger.warning("Job %s has missing tags, skipping...", job_id)
                return False

    return True


def match_jobs_with_node(job: str, node: str) -> tuple[list[Job], Node]:
    """Validate a job against a node configuration.

    Args:
        job (str): Path to the job YAML file.
        node (str): Path to the node YAML file.

    Returns:
        tuple[list[Job], Node]: A tuple containing a list of matching jobs and the node object.
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
        job_obj = Job.load_from_yaml(job)
        logger.info("Job file %s is VALID.", job)

        if not job_obj.job_id:
            job_obj.job_id = Path(job).stem
            logger.warning("Job ID not specified in %s, using filename as default: %s", job, job_obj.job_id)

        for i, job_spec in enumerate(job_obj.matching_specs):
            if valid_job_specs_with_node(f"{job_obj.job_id}-{i}", job_spec, node_obj):
                jobs_match.append(job_obj)
                logger.info("Job %s-%s matches node %s.", job_obj.job_id, i, node_obj.node_id)
    except ValidationError as e:
        logger.error("Invalid job specification: %s", e)
        return [], node_obj

    return jobs_match, node_obj
