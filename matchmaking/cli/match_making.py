from __future__ import annotations

import argparse
import sys

from matchmaking.config.logger import configure_logger, logger
from matchmaking.core.match_making import match_jobs_with_node, valid_job, valid_node


def main():
    parser = argparse.ArgumentParser(description="Matchmaking and validation for DIRAC jobs and pilots.")
    parser.add_argument("job", nargs="?", help="Path to the job YAML file")
    parser.add_argument("node", nargs="?", help="Path to the node YAML file")
    parser.add_argument("--validate-job", "-VJ", action="store_true", help="Only validate the job file")
    parser.add_argument(
        "--validate-node",
        "-VN",
        action="store_true",
        help="Only validate the node file",
    )

    args = parser.parse_args()
    # Force INFO logging level to show job/node validation details
    configure_logger("INFO")

    if args.validate_job:
        if not args.job:
            logger.error("Error: --validate-job requires a job file path.")
            sys.exit(1)

        valid_job(args.job)
    elif args.validate_node:
        # If node_path is not provided, check if job_path was used instead
        node_path = args.node or args.job
        if not node_path:
            logger.error("Error: --validate-node requires a node file path.")
            sys.exit(1)

        valid_node(node_path)
    elif args.job and args.node:
        try:
            matched_jobs, _ = match_jobs_with_node(args.job, args.node)

            if matched_jobs:
                logger.info(f"Match found! {len(matched_jobs)} job(s) can run on this node:")

                for job in matched_jobs:
                    logger.info(f"  - Job ID: {job.job_id}")
            else:
                logger.info("No jobs from the job file can run on this node.")
        except Exception as e:
            logger.error(f"Error during matchmaking: {e}")
            sys.exit(1)
    else:
        parser.print_help()


if __name__ == "__main__":  # pragma: no cover
    main()
