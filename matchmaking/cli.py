from __future__ import annotations

import argparse
import sys

import yaml

from matchmaking.config.logger import configure_logger, logger
from matchmaking.core.match_making import match_jobs_with_node
from matchmaking.models.job import Job
from matchmaking.models.node import Node


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
    # Force INFO logging level to show job/node validation details
    configure_logger("INFO")

    if args.validate_job:
        if not args.job:
            logger.error("Error: --validate-job requires a job file path.")
            sys.exit(1)
        try:
            with open(args.job, "r") as f:
                content = yaml.safe_load(f)

            jobs = content.get("matching_specs", [])
            if not jobs:
                logger.error(f"No matching_specs found in {args.job}")
                sys.exit(1)

            logger.info(f"Validating {len(jobs)} job(s) from {args.job}...")
            for i, job_spec in enumerate(jobs):
                if "job_id" not in job_spec:
                    job_spec["job_id"] = f"job-{i}"

                Job.model_validate(job_spec)
                logger.info(f"  - Job {job_spec.get('job_id')} is VALID.")

            logger.info("Validation successful.")
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
            logger.info(f"Node file {node_path} is VALID.")
        except Exception as e:
            logger.error(f"Error validating node: {e}")
            sys.exit(1)

    elif args.job and args.node_pilot:
        try:
            matched_jobs = match_jobs_with_node(args.job, args.node_pilot)

            if matched_jobs:
                logger.info(f"Match found! {len(matched_jobs)} job(s) can run on this node:")

                for job in matched_jobs:
                    logger.info(f"  - Job ID: {job.job_id}")
            else:
                logger.warning("No jobs from the job file can run on this node.")
        except Exception as e:
            logger.error(f"Error during matchmaking: {e}")
            sys.exit(1)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
