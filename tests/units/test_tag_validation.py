from __future__ import annotations

import ast
from datetime import datetime, timezone
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from matchmaking.logic.tags import evaluate_tag_expression, validate_tag_expression
from matchmaking.models.job import Job
from matchmaking.models.utils import JobGroup, JobType, SystemName

BASE_JOB_DATA = {
    "job_id": "test-job",
    "owner": "test-owner",
    "group": JobGroup.LHCB_MC,
    "job_type": JobType.USER,
    "submission_time": datetime.now(tz=timezone.utc),
    "matching_specs": [
        {
            "system": {"name": SystemName.LINUX},
            "wall-time": 3600,
            "cpu": {
                "num-cores": {"min": 1, "max": 1},
                "architecture": {"name": "x86_64", "microarchitecture-level": {"min": 1}},
            },
            "tags": "a",
        }
    ],
}


def test_invalid_tag_expression():
    job_data = BASE_JOB_DATA.copy()
    job_data["matching_specs"][0]["tags"] = "a & (b | )"  # Invalid expression

    with pytest.raises(ValidationError) as excinfo:
        Job.model_validate(job_data)

    assert "Invalid tag expression" in str(excinfo.value)


def test_empty_tag_expression():
    job_data = BASE_JOB_DATA.copy()
    job_data["matching_specs"][0]["tags"] = ""
    # Should not raise, empty tags are allowed
    Job.model_validate(job_data)
    # Also test the function directly
    validate_tag_expression("")


def test_unsupported_ast_node_in_tags():
    job_data = BASE_JOB_DATA.copy()
    job_data["matching_specs"][0]["tags"] = "a if b else c"  # Conditional expression not supported

    with pytest.raises(ValidationError) as excinfo:
        Job.model_validate(job_data)

    assert "Invalid tag expression syntax" in str(excinfo.value)


def test_valid_tag_expression():
    job_data = BASE_JOB_DATA.copy()
    job_data["matching_specs"][0]["tags"] = "a & (b | c)"
    # Should not raise
    Job.model_validate(job_data)


def test_unsupported_operator_in_tags():
    job_data = BASE_JOB_DATA.copy()
    job_data["matching_specs"][0]["tags"] = "a + b"  # '+' is not supported

    with pytest.raises(ValidationError) as excinfo:
        Job.model_validate(job_data)

    assert "Invalid tag expression" in str(excinfo.value)
    assert "Unsupported operation" in str(excinfo.value)


def test_unexpected_name_in_tags():

    # Using mock to bypass the repl_token logic and reach lines 70-73
    with patch("ast.parse") as mock_parse:
        mock_node = ast.Expression(body=ast.Name(id="unexpected", ctx=ast.Load()))
        mock_parse.return_value = mock_node
        with pytest.raises(ValueError, match="Unexpected name: unexpected"):
            validate_tag_expression("dummy")


def test_unsupported_unary_operator_in_tags():

    with patch("ast.parse") as mock_parse:
        # Create a tree with an unsupported unary operator (e.g., UAdd '+')
        mock_node = ast.Expression(body=ast.UnaryOp(op=ast.UAdd(), operand=ast.Constant(value=True)))
        mock_parse.return_value = mock_node
        with pytest.raises(ValueError, match="Unsupported unary operator: UAdd"):
            validate_tag_expression("dummy")


def test_unsupported_constant_type_in_tags():

    # This is a bit hacky but it targets the specific lines in tags.py
    with patch("ast.parse") as mock_parse:
        # Create a tree with a constant string instead of bool
        mock_node = ast.Expression(body=ast.Constant(value="not-a-bool"))
        mock_parse.return_value = mock_node
        with pytest.raises(ValueError, match="Unsupported constant type: str"):
            validate_tag_expression("dummy")


def test_evaluate_node_returns_false_for_unsupported_expression_node():
    # evaluate_tag_expression now catches ValueError/SyntaxError and returns False
    assert not evaluate_tag_expression("a + b", {"a", "b"})
