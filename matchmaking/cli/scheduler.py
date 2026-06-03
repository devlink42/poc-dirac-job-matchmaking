#!/usr/bin/env python3

from __future__ import annotations

import argparse
import sys

from matchmaking.config.logger import configure_logger, logger
from matchmaking.core.scheduler import select_job
from matchmaking.models.node import Node


def main():
    """Run the scheduler CLI.

    This function parses command line arguments and invokes the job selection
    logic for a given node.
    """
    parser = argparse.ArgumentParser(description="Job scheduler for the cluster.")
    parser.add_argument("node", nargs="?", help="Path to the node YAML file")
    parser.add_argument("job", nargs="?", help="Path to the job YAML file")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging verbosity level.",
    )

    args = parser.parse_args()

    configure_logger(args.log_level)

    if not args.node or not args.job:
        parser.print_help()
        return

    try:
        node_obj = Node.load_from_yaml(args.node)
        allowed_job = select_job(node_obj)

        if allowed_job:
            logger.info("Job %s selected for execution on %s.", allowed_job.job_id, node_obj.site)
        else:
            logger.info("No allowed job can run on this node.")
    except Exception as e:
        logger.error("Error during matchmaking: %s", e)
        sys.exit(1)


if __name__ == "__main__":  # pragma: no cover
    main()
