import os
import subprocess
import sys


def run_cli(*args):
    env = os.environ.copy()
    env["PYTHONPATH"] = env.get("PYTHONPATH", "") + ":" + os.getcwd()
    result = subprocess.run(
        [sys.executable, "src/core/valid_pilot.py"] + list(args),
        capture_output=True,
        text=True,
        env=env
    )

    return result


def test_cli_match_success():
    result = run_cli("examples/jobs/job_01_mcsimulation_any_site.yaml", "examples/nodes/pilot_01_cern_typical.yaml")

    assert result.returncode == 0
    assert "Match found!" in result.stdout
    assert "Job ID: unknown-job-id" in result.stdout


def test_cli_validate_job():
    result = run_cli("examples/jobs/job_01_mcsimulation_any_site.yaml", "--validate-job")

    assert result.returncode == 0
    assert "Validating 1 job(s)" in result.stdout
    assert "Job job-0 is VALID" in result.stdout


def test_cli_validate_node():
    result = run_cli("examples/nodes/pilot_01_cern_typical.yaml", "--validate-node")

    assert result.returncode == 0
    assert "is VALID" in result.stdout


def test_cli_invalid_job_file():
    result = run_cli("examples/jobs/invalid_01_min_gt_max.yaml", "--validate-job")

    assert result.returncode == 1
    assert "Error validating job" in result.stdout
    assert "max must be greater than or equal to min" in result.stdout


def test_cli_help():
    result = run_cli("--help")

    assert result.returncode == 0
    assert "Matchmaking and validation for DIRAC jobs and pilots" in result.stdout
