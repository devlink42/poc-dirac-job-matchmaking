#!/usr/bin/env python3

from __future__ import annotations

import argparse
import sys

from matchmaking.config.logger import configure_logger, logger
from matchmaking.core.match_making import match_jobs_with_node
from matchmaking.core.scheduler import select_job
from matchmaking.models.config import SchedulingConfig


def main():
    parser = argparse.ArgumentParser(description="Job scheduler for the cluster.")
    parser.add_argument("node_pilot", nargs="?", help="Path to the node/pilot YAML file")
    parser.add_argument("job", nargs="?", help="Path to the job YAML file")
    parser.add_argument("config", nargs="?", help="Path to the configuration file")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging verbosity level.",
    )

    args = parser.parse_args()

    configure_logger(args.log_level)

    if args.node_pilot and args.job and args.config:
        try:
            if valid_jobs_node := match_jobs_with_node(args.job, args.node_pilot):
                jobs, node = valid_jobs_node

                if jobs:
                    allowed_job = select_job(
                        node,
                        jobs,
                        SchedulingConfig.load_from_yaml(args.config),
                    )

                    if allowed_job:
                        logger.info("Job %s selected for execution on %s.", allowed_job.job_id, node.site)
                    else:
                        logger.info("No allowed job from the job file can run on this node.")
                else:
                    logger.info("No valid jobs from the job file can run on this node.")
        except Exception as e:
            logger.error("Error during matchmaking: %s", e)
            sys.exit(1)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
