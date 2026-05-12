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
        logger.info(f"Job {job_id} is VALID.")

        return True
    except Exception as e:
        logger.error(f"Error validating job specification: {e}")
        return False


def valid_node(node: str) -> bool:
    """Validate a node against a set of requirements."""
    try:
        Node.load_from_yaml(node)
        logger.info(f"Node file {node} is VALID.")

        return True
    except Exception as e:
        logger.error(f"Error validating node: {e}")
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
        logger.warning(f"Job {job_id} requires to be on site {job_specs.site}, skipping...")
        return False

    # System check
    if job_specs.system.name != node.system.name:
        logger.warning(f"Job {job_id} requires to be on system {job_specs.system.name}, skipping...")
        return False

    if job_specs.system.glibc and node.system.glibc < job_specs.system.glibc:
        logger.warning(f"Job {job_id} requires glibc >= {job_specs.system.glibc}, skipping...")
        return False

    if job_specs.system.user_namespaces and not node.system.user_namespaces:
        logger.warning(f"Job {job_id} requires user namespaces to be {job_specs.system.user_namespaces}, skipping...")
        return False

    # Wall time check
    if job_specs.wall_time and node.wall_time < job_specs.wall_time:
        logger.warning(f"Job {job_id} requires {job_specs.wall_time} wall time, skipping...")
        return False

    # CPU work check
    if job_specs.cpu_work and node.cpu_work < job_specs.cpu_work:
        logger.warning(f"Job {job_id} requires {job_specs.cpu_work} CPU work, skipping...")
        return False

    # CPU Cores check
    if node.cpu.num_cores < job_specs.cpu.num_cores.min:
        logger.warning(f"Job {job_id} requires at least {job_specs.cpu.num_cores.min} CPU cores, skipping...")
        return False

    # RAM check
    if job_specs.cpu.ram_mb:
        required_ram_request = job_specs.cpu.ram_mb.request.overhead
        if job_specs.cpu.ram_mb.request.per_core:
            required_ram_request += job_specs.cpu.ram_mb.request.per_core * job_specs.cpu.num_cores.min

        if node.cpu.ram_mb < required_ram_request:
            logger.warning(f"Job {job_id} requires at least {required_ram_request} MB RAM, skipping...")
            return False

        ram_limit = job_specs.cpu.ram_mb.limit.overhead
        if job_specs.cpu.ram_mb.limit.per_core:
            ram_limit += job_specs.cpu.ram_mb.limit.per_core * job_specs.cpu.num_cores.min

        if node.cpu.ram_mb < ram_limit:
            logger.warning(f"Job {job_id} requires at least {ram_limit} MB RAM, skipping...")
            return False

    # Architecture check
    if job_specs.cpu.architecture.name != node.cpu.architecture.name:
        logger.warning(f"Job {job_id} requires architecture {job_specs.cpu.architecture.name}, skipping...")
        return False

    if node.cpu.architecture.microarchitecture_level < job_specs.cpu.architecture.microarchitecture_level.min:
        logger.warning(
            f"Job {job_id} requires at least microarchitecture level "
            f"{job_specs.cpu.architecture.microarchitecture_level.min}, skipping..."
        )
        return False

    if (
        job_specs.cpu.architecture.microarchitecture_level.max
        and node.cpu.architecture.microarchitecture_level > job_specs.cpu.architecture.microarchitecture_level.max
    ):
        logger.warning(
            f"Job {job_id} requires at most microarchitecture level "
            f"{job_specs.cpu.architecture.microarchitecture_level.max}, skipping..."
        )
        return False

    # GPU check (if required)
    if job_specs.gpu:
        if node.gpu.count < job_specs.gpu.count.min:
            logger.warning(f"Job {job_id} requires at least {job_specs.gpu.count.min} GPUs, skipping...")
            return False

        if job_specs.gpu.count.max and node.gpu.count > job_specs.gpu.count.max:
            logger.warning(f"Job {job_id} requires at most {job_specs.gpu.count.max} GPUs, skipping...")
            return False

        if node.gpu.count > 0:
            if job_specs.gpu.ram_mb and node.gpu.ram_mb and node.gpu.ram_mb < job_specs.gpu.ram_mb:
                logger.warning(f"Job {job_id} requires at least {job_specs.gpu.ram_mb} MB GPU RAM, skipping...")
                return False

            if job_specs.gpu.vendor != node.gpu.vendor:
                logger.warning(f"Job {job_id} requires GPU vendor {job_specs.gpu.vendor}, skipping...")
                return False

            if (
                job_specs.gpu.compute_capability.min
                and node.gpu.compute_capability
                and node.gpu.compute_capability < job_specs.gpu.compute_capability.min
            ):
                logger.warning(
                    f"Job {job_id} requires at least compute capability {job_specs.gpu.compute_capability.min}, "
                    f"skipping..."
                )
                return False

            if (
                job_specs.gpu.compute_capability.max
                and node.gpu.compute_capability
                and node.gpu.compute_capability > job_specs.gpu.compute_capability.max
            ):
                logger.warning(
                    f"Job {job_id} requires at most compute capability {job_specs.gpu.compute_capability.max}, "
                    f"skipping..."
                )
                return False

            if (
                job_specs.gpu.driver_version
                and node.gpu.driver_version
                and node.gpu.driver_version < job_specs.gpu.driver_version
            ):
                logger.warning(
                    f"Job {job_id} requires at least driver version {job_specs.gpu.driver_version}, skipping..."
                )
                return False

    # IO check
    if job_specs.io and node.io:
        if node.io.scratch_mb < job_specs.io.scratch_mb:
            logger.warning(f"Job {job_id} requires at least {job_specs.io.scratch_mb} MB scratch space, skipping...")
            return False

    # Tags check (all job tags must be present in node tags)
    if job_specs.tags:
        node_tags = set(node.tags)
        logger.debug(f"Node {node.node_id} has tags: {node_tags}")
        logger.debug(f"Job {job_id} has tags: {job_specs.tags}")

        if not evaluate_tag_expression(job_specs.tags, node_tags):
            logger.warning(f"Job {job_id} has invalid tag expression, skipping...")
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
        logger.info(f"Node file {node} is VALID.")
    except ValidationError as e:
        logger.error(f"Invalid node specification: {e}")
        raise

    if not node_obj.node_id:
        node_obj.node_id = Path(node).stem
        logger.warning(f"Node ID not specified in {node}, using filename as default: {node_obj.node_id}")

    jobs_match = []

    try:
        job_obj = Job.load_from_yaml(job)
        logger.info(f"Job file {job} is VALID.")

        if not job_obj.job_id:
            job_obj.job_id = Path(job).stem
            logger.warning(f"Job ID not specified in {job}, using filename as default: {job_obj.job_id}")

        for i, job_spec in enumerate(job_obj.matching_specs):
            if valid_job_specs_with_node(f"{job_obj.job_id}-{i}", job_spec, node_obj):
                jobs_match.append(job_obj)
                logger.info(f"Job {job_obj.job_id}-{i} matches node {node_obj.node_id}.")
    except ValidationError as e:
        logger.error(f"Invalid job specification: {e}")
        return [], node_obj

    return jobs_match, node_obj
