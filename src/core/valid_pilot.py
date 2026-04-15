#!/usr/bin/env python3

from __future__ import annotations

import argparse
import sys

import yaml
from pydantic import ValidationError

from src.models.job import Job
from src.models.node import Node


def valid_job_with_node(job: Job, node: Node) -> bool:
    # Site check
    if job.site and job.site != node.site:
        return False

    # System check
    if job.system.name != node.system.name:
        return False

    if job.system.glibc is not None and node.system.glibc < job.system.glibc:
        return False

    if job.system.user_namespaces is not None and job.system.user_namespaces and not node.system.user_namespaces:
        return False

    # Architecture check
    if job.cpu.architecture.name != node.cpu.architecture.name:
        return False

    if node.cpu.architecture.microarchitecture_level < job.cpu.architecture.microarchitecture_level.min:
        return False

    if (
        job.cpu.architecture.microarchitecture_level.max is not None
        and node.cpu.architecture.microarchitecture_level > job.cpu.architecture.microarchitecture_level.max
    ):
        return False

    # CPU Cores check
    if node.cpu.num_cores < job.cpu.num_cores.min:
        return False

    # RAM check
    if job.cpu.ram_mb:
        if required_ram_request := job.cpu.ram_mb.request.overhead:
            if job.cpu.ram_mb.request.per_core:
                required_ram_request += job.cpu.ram_mb.request.per_core * job.cpu.num_cores.max

            if node.cpu.ram_mb < required_ram_request:
                return False

    # CPU work
    if node.cpu_work < job.cpu_work:
        return False

    # GPU check (if required)
    if job.gpu:
        if node.gpu.count < job.gpu.count.min:
            return False
        if job.gpu.count.max is not None and node.gpu.count > job.gpu.count.max:
            return False

        if node.gpu.count > 0:
            if job.gpu.compute_capability.min and node.gpu.compute_capability:
                if node.gpu.compute_capability < job.gpu.compute_capability.min:
                    return False

            if job.gpu.compute_capability.max and node.gpu.compute_capability:
                if node.gpu.compute_capability > job.gpu.compute_capability.max:
                    return False

            if job.gpu.driver_version and node.gpu.driver_version:
                if node.gpu.driver_version < job.gpu.driver_version:
                    return False

    # Tags check (all job tags must be present in node tags)
    if job.tags:
        job_tags = set(tag.strip() for tag in job.tags.replace(",", " ").split())
        node_tags = set(node.tags)

        if not job_tags.issubset(node_tags):
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
        print(f"Invalid node specification: {e}")
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
            print(f"Invalid job specification: {e}")
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
            print("Error: --validate-job requires a job file path.")
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
            print(f"Error validating job: {e}")
            sys.exit(1)

    elif args.validate_node:
        # If node_path is not provided, check if job_path was used instead
        node_path = args.node_pilot or args.job
        if not node_path:
            print("Error: --validate-node/--validate-pilot requires a node file path.")
            sys.exit(1)

        try:
            with open(node_path, "r") as f:
                content = yaml.safe_load(f)

            if "node_id" not in content:
                content["node_id"] = "unknown-node"

            Node.model_validate(content)
            print(f"Node file {node_path} is VALID.")
        except Exception as e:
            print(f"Error validating node: {e}")
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
            print(f"Error during matchmaking: {e}")
            sys.exit(1)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
