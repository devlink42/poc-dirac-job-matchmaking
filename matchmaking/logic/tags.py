#!/usr/bin/env python3

from __future__ import annotations

import ast
import re
from functools import lru_cache

from matchmaking.config.logger import logger

TOKEN = {"and", "or", "not"}
REGEX_TAG = r"[A-Za-z0-9][A-Za-z0-9:_.\-]*"
_TAG_NAME_PREFIX = "_tag_"
_TAG_EXPRESSION_CACHE_SIZE = 8192


def normalize_tag_expression(expr: str) -> str:
    """Normalize a tag expression by replacing shorthand operators with Python keywords.

    Args:
        expr: The tag expression to normalize (e.g., 'a & b').

    Returns:
        The normalized expression (e.g., 'a and b').
    """
    return expr.replace("&", " and ").replace("|", " or ").replace("~", " not ")


@lru_cache(maxsize=_TAG_EXPRESSION_CACHE_SIZE)
def _compile_tag_expression(expr: str) -> tuple[ast.expr, tuple[str, ...]] | None:
    """Compile and validate a tag expression once for repeated evaluation."""
    if not expr:
        return None

    expr_norm = normalize_tag_expression(expr).strip()
    tags: list[str] = []

    def repl_token(match: re.Match[str]) -> str:
        token = match.group(0)
        if token in TOKEN:
            return token

        tag_name = f"{_TAG_NAME_PREFIX}{len(tags)}"
        tags.append(token)
        return tag_name

    parsed_expression = re.sub(REGEX_TAG, repl_token, expr_norm)

    try:
        tree = ast.parse(parsed_expression, mode="eval")
    except SyntaxError as e:
        logger.error("Syntax error in tag expression '%s': %s", expr, e)
        raise ValueError(f"Invalid tag expression syntax: {e}") from e

    valid_tag_names = {f"{_TAG_NAME_PREFIX}{index}" for index in range(len(tags))}

    def check_tree_node(node: ast.AST) -> None:
        if isinstance(node, ast.Expression):
            check_tree_node(node.body)
        elif isinstance(node, ast.BoolOp):
            for value in node.values:
                check_tree_node(value)
        elif isinstance(node, ast.UnaryOp):
            if not isinstance(node.op, ast.Not):
                raise ValueError(f"Unsupported unary operator: {type(node.op).__name__}")
            check_tree_node(node.operand)
        elif isinstance(node, ast.Name):
            if node.id not in valid_tag_names:
                raise ValueError(f"Unexpected name: {node.id}")
        elif isinstance(node, ast.Constant):
            if not isinstance(node.value, bool):
                raise ValueError(f"Unsupported constant type: {type(node.value).__name__}")
        else:
            raise ValueError(f"Unsupported operation in tag expression: {type(node).__name__}")

    try:
        check_tree_node(tree)
    except ValueError as e:
        logger.error("Validation failed for tag expression '%s': %s", expr, e)
        raise ValueError(f"Invalid tag expression: {e}") from e

    return tree.body, tuple(tags)


def validate_tag_expression(expr: str) -> None:
    """Validate the syntax and safety of a tag expression.

    Args:
        expr: The tag expression to validate.

    Raises:
        ValueError: If the expression is syntactically invalid or contains unsupported operations.
    """
    _compile_tag_expression(expr)


def evaluate_tag_expression(expr: str, node_tags: set[str]) -> bool:
    """Evaluate a boolean expression of tags against a set of node tags.

    Supported syntax examples:
      - "a & b"
      - "a | (b & c)"
      - "~a"
      - Operators: '&' for AND, '|' for OR, '~' for NOT, parentheses for grouping

    Args:
        expr: The tag expression to evaluate.
        node_tags: The set of tags available on the node.

    Returns:
        True if the expression evaluates to True, False otherwise.
    """

    def evaluate_node(node: ast.AST, tags: tuple[str, ...]) -> bool:
        if isinstance(node, ast.Constant):
            return bool(node.value)
        if isinstance(node, ast.Name):
            tag_index = int(node.id.removeprefix(_TAG_NAME_PREFIX))
            return tags[tag_index] in node_tags
        if isinstance(node, ast.BoolOp):
            if isinstance(node.op, ast.And):
                return all(evaluate_node(value, tags) for value in node.values)
            if isinstance(node.op, ast.Or):
                return any(evaluate_node(value, tags) for value in node.values)
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
            return not evaluate_node(node.operand, tags)

        logger.error("Unsupported operation during evaluation: %s", type(node).__name__)
        raise ValueError(f"Unsupported operation during evaluation: {type(node).__name__}")

    try:
        compiled_expression = _compile_tag_expression(expr)
        if compiled_expression is None:
            return False

        tree, tags = compiled_expression
        return evaluate_node(tree, tags)
    except ValueError as e:
        logger.error("Error evaluating tag expression '%s': %s", expr, e)

        return False
