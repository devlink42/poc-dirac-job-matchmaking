#!/usr/bin/env python3

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from sys import executable

PROJECT_ROOT = Path(__file__).parent.parent.absolute()


def run_cli(*args):
    env = os.environ.copy()
    # Use proper path separator for Windows/Linux
    current_pythonpath = env.get("PYTHONPATH", "")
    if current_pythonpath:
        env["PYTHONPATH"] = f"{str(PROJECT_ROOT)}{os.pathsep}{current_pythonpath}"
    else:
        env["PYTHONPATH"] = str(PROJECT_ROOT)

    script_path = str(PROJECT_ROOT / "src" / "core" / "valid_pilot.py")

    processed_args = []
    for arg in args:
        if isinstance(arg, str) and not arg.startswith("-"):
            potential_path = PROJECT_ROOT / arg
            if potential_path.exists():
                processed_args.append(str(potential_path))
            else:
                processed_args.append(arg)
        else:
            processed_args.append(str(arg))

    result = subprocess.run(  # noqa: S603
        [executable, script_path] + processed_args, capture_output=True, text=True, env=env
    )

    return result


def test_cli_match_success():
    result = run_cli(
        "tests/examples/jobs/job_01_mcsimulation_any_site.yaml", "tests/examples/nodes/pilot_01_cern_typical.yaml"
    )

    assert result.returncode == 0
    assert "Match found!" in result.stdout
    assert "Job ID: unknown-job-id" in result.stdout


def test_cli_validate_job():
    result = run_cli("tests/examples/jobs/job_01_mcsimulation_any_site.yaml", "--validate-job")

    assert result.returncode == 0
    assert "Validating 1 job(s)" in result.stdout
    assert "Job job-0 is VALID" in result.stdout


def test_cli_validate_node():
    result = run_cli("tests/examples/nodes/pilot_01_cern_typical.yaml", "--validate-node")

    assert result.returncode == 0
    assert "is VALID" in result.stdout


def test_cli_invalid_job_file():
    result = run_cli("tests/examples/jobs/invalid_01_min_gt_max.yaml", "--validate-job")

    assert result.returncode == 1
    assert "Error validating job" in result.stdout
    assert "max must be greater than or equal to min" in result.stdout


def test_cli_validate_job_requires_file_path():
    result = run_cli("--validate-job")

    assert result.returncode == 1
    assert "--validate-job requires a job file path" in result.stdout


def test_cli_validate_node_requires_file_path():
    result = run_cli("--validate-node")

    assert result.returncode == 1
    assert "--validate-node/--validate-pilot requires a node file path" in result.stdout


def test_cli_validate_job_missing_file_path():
    result = run_cli("tests/examples/jobs/does_not_exist.yaml", "--validate-job")

    assert result.returncode == 1
    assert "Error validating job" in result.stdout
    assert "No such file or directory" in result.stdout


def test_cli_help():
    result = run_cli("--help")

    assert result.returncode == 0
    assert "Matchmaking and validation for DIRAC jobs and pilots" in result.stdout
