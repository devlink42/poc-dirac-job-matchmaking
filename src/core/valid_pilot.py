#!/usr/bin/env python3

from __future__ import annotations

import argparse
import ast
import logging
import re
import sys

import yaml
from pydantic import ValidationError

from src.models.job import Job
from src.models.node import Node

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)


def _eval_tag_expression(expr: str, node_tags: set[str]) -> bool:
    """Evaluate a simple boolean expression of tags against a set of node tags.

    Supported syntax examples:
      - "a & b"
      - "a | (b & c)"
      - "not a"
      - Operators: '&' for AND, '|' for OR, 'not' for NOT, parentheses for grouping
    """

    def repl_token(m: re.Match[str]) -> str:
        token = m.group(0)
        if token in {"and", "or", "not"}:
            return token

        return "True" if token in node_tags else "False"

    expr_norm = expr.replace("&", " and ").replace("|", " or ").replace("~", " not ")
    logger.debug(f"Evaluating tag expression: {expr_norm}")
    expr_bool = re.sub(r"[A-Za-z0-9:_+-]+", repl_token, expr_norm)
    logger.debug(f"Normalized expression: {expr_bool}")

    def evaluate_node(node):
        if isinstance(node, ast.Constant):  # True or False
            logger.debug(f"Evaluating constant: {node.value}")
            return bool(node.value)
        elif isinstance(node, ast.BoolOp):  # 'and' / 'or'
            if isinstance(node.op, ast.And):
                logger.debug(f"Evaluating AND: {node.values}")
                return all(evaluate_node(val) for val in node.values)
            elif isinstance(node.op, ast.Or):
                logger.debug(f"Evaluating OR: {node.values}")
                return any(evaluate_node(val) for val in node.values)
        elif isinstance(node, ast.UnaryOp):  # 'not'
            if isinstance(node.op, ast.Not):
                logger.debug(f"Evaluating NOT: {node.operand}")
                return not evaluate_node(node.operand)

        raise ValueError(f"Operation not supported: {expr_norm} =>")

    try:
        tree = ast.parse(expr_bool, mode="eval")
        logger.debug(f"Parsed expression: {tree.body}")
        return evaluate_node(tree.body)
    except Exception:
        logger.warning(f"Error evaluating expression: {expr_norm}")
        return False


def valid_job_with_node(job: Job, node: Node) -> bool:
    # Site check
    if job.site and job.site != node.site:
        logger.warning(f"Job {job.job_id} is not on site {job.site}, skipping...")
        return False

    # System check
    if job.system.name != node.system.name:
        logger.warning(f"Job {job.job_id} is not on system {job.system.name}, skipping...")
        return False

    if job.system.glibc and node.system.glibc < job.system.glibc:
        logger.warning(f"Job {job.job_id} requires glibc >= {job.system.glibc}, skipping...")
        return False

    if job.system.user_namespaces and job.system.user_namespaces != node.system.user_namespaces:
        logger.warning(f"Job {job.job_id} requires user namespaces to be {job.system.user_namespaces}, skipping...")
        return False

    # CPU work
    if node.cpu_work < job.cpu_work:
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
                logger.warning(f"Job {job.job_id} requires at most {ram_limit} MB RAM, skipping...")
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

        if node.io.lan_mbitps and job.io.lan_mbitps and node.io.lan_mbitps < job.io.lan_mbitps:
            logger.warning(f"Job {job.job_id} requires at least {job.io.lan_mbitps} Mbit/s LAN bandwidth, skipping...")
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


def valid_pilot(job: str, pilot: str) -> list[Job]:
    """Validate a job against a node/pilot configuration.

    Args:
        job (str): Path to the job YAML file.
        pilot (str): Path to the node/pilot YAML file.

    Returns:
        list[Job]: List of matching jobs if validation is successful, otherwise an empty list.
    """
    with open(job, "r") as job_file, open(pilot, "r") as pilot_file:
        yaml_job = yaml.safe_load(job_file)
        yaml_node = yaml.safe_load(pilot_file)

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


def main():
    parser = argparse.ArgumentParser(description="Matchmaking and validation for DIRAC jobs and pilots.")
    parser.add_argument("job", nargs="?", help="Path to the job YAML file")
    parser.add_argument("node_pilot", nargs="?", help="Path to the node/pilot YAML file")
    parser.add_argument("--validate-job", "-VJ", action="store_true", help="Only validate the job file")
    parser.add_argument(
        "--validate-node",
        "-VN",
        "--validate-pilot",
        "-VP",
        action="store_true",
        help="Only validate the node/pilot file",
    )

    args = parser.parse_args()

    if args.validate_job:
        if not args.job:
            logger.error("Error: --validate-job requires a job file path.")
            sys.exit(1)
        try:
            with open(args.job, "r") as f:
                content = yaml.safe_load(f)

            jobs = content.get("matching_specs", [])
            if not jobs:
                print(f"No matching_specs found in {args.job}")
                sys.exit(1)

            print(f"Validating {len(jobs)} job(s) from {args.job}...")
            for i, job_spec in enumerate(jobs):
                if "job_id" not in job_spec:
                    job_spec["job_id"] = f"job-{i}"

                Job.model_validate(job_spec)
                print(f"  - Job {job_spec.get('job_id')} is VALID.")

            print("Validation successful.")
        except Exception as e:
            logger.error(f"Error validating job: {e}")
            sys.exit(1)

    elif args.validate_node:
        # If node_path is not provided, check if job_path was used instead
        node_path = args.node_pilot or args.job
        if not node_path:
            logger.error("Error: --validate-node/--validate-pilot requires a node file path.")
            sys.exit(1)

        try:
            with open(node_path, "r") as f:
                content = yaml.safe_load(f)

            if "node_id" not in content:
                content["node_id"] = "unknown-node"

            Node.model_validate(content)
            print(f"Node file {node_path} is VALID.")
        except Exception as e:
            logger.error(f"Error validating node: {e}")
            sys.exit(1)

    elif args.job and args.node_pilot:
        try:
            matched_jobs = valid_pilot(args.job, args.node_pilot)

            if matched_jobs:
                print(f"Match found! {len(matched_jobs)} job(s) can run on this node:")

                for job in matched_jobs:
                    print(f"  - Job ID: {job.job_id}")
            else:
                print("No jobs from the job file can run on this node.")
        except Exception as e:
            logger.error(f"Error during matchmaking: {e}")
            sys.exit(1)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
