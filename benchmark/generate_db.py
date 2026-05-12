#!/usr/bin/env python3
"""Pre-generate benchmark data into a SQLite database.

Run once before benchmarking so no data is generated at test runtime:
    pixi run python -m benchmark.generate_db --num-jobs 10000000 --num-nodes 50000

The database is then consumed by locustfile.py via --db-path.
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from benchmark.data_generator import job_generator, node_generator
from matchmaking.config.logger import configure_logger, logger

_DEFAULT_DB = "benchmark/benchmark.db"
_BATCH_SIZE = 1000


def _create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS jobs (
            id      INTEGER PRIMARY KEY,
            job_id  TEXT NOT NULL,
            data    TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS nodes (
            id      INTEGER PRIMARY KEY,
            node_id TEXT NOT NULL,
            data    TEXT NOT NULL
        );
    """)


def _populate(conn: sqlite3.Connection, num_jobs: int, num_nodes: int) -> None:
    batch: list[tuple[str, str]] = []

    logger.info("Generating %s jobs...", num_jobs)

    for i, job in enumerate(job_generator(num_jobs), 1):
        batch.append((job.job_id, job.model_dump_json()))
        if len(batch) == _BATCH_SIZE:
            conn.executemany("INSERT INTO jobs (job_id, data) VALUES (?, ?)", batch)
            batch.clear()

        if i % 10000 == 0:
            logger.info("  %s/%s jobs written", i, num_jobs)

    if batch:
        conn.executemany("INSERT INTO jobs (job_id, data) VALUES (?, ?)", batch)

    logger.info("Generating %s nodes...", num_nodes)
    conn.executemany(
        "INSERT INTO nodes (node_id, data) VALUES (?, ?)",
        ((n.node_id, n.model_dump_json()) for n in node_generator(num_nodes)),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Pre-generate benchmark data into a SQLite database.")
    parser.add_argument("--num-jobs", type=int, default=100000, help="Number of jobs to generate")
    parser.add_argument("--num-nodes", type=int, default=1000, help="Number of nodes to generate")
    parser.add_argument("--output", type=str, default=_DEFAULT_DB, help="Output database path")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite an existing database")
    args = parser.parse_args()

    configure_logger("INFO")

    db_path = Path(args.output)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    if db_path.exists():
        if not args.overwrite:
            logger.error("%s already exists. Use --overwrite to replace it.", db_path)
            raise SystemExit(1)

        db_path.unlink()

    conn = sqlite3.connect(db_path)
    try:
        _create_schema(conn)
        _populate(conn, args.num_jobs, args.num_nodes)
        conn.commit()
        size_kb = db_path.stat().st_size / 1024
        logger.info("Done. Database written to %s (%.0s KB).", db_path, size_kb)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
