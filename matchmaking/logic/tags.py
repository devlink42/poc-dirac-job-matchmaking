#!/usr/bin/env python3

from __future__ import annotations

import ast
import re

from matchmaking.config.logger import logger


def normalize_tag_expression(expr: str) -> str:
    """Normalize a tag expression by replacing shorthand operators with Python keywords.

    Args:
        expr (str): The tag expression to normalize (e.g., 'a & b').

    Returns:
        str: The normalized expression (e.g., 'a and b').
    """
    return expr.replace("&", " and ").replace("|", " or ").replace("~", " not ")


def validate_tag_expression(expr: str) -> None:
    """Validate the syntax and safety of a tag expression.

    Args:
        expr (str): The tag expression to validate.

    Raises:
        ValueError: If the expression is syntactically invalid or contains unsupported operations.
    """
    if not expr:
        logger.debug("Empty tag expression provided")
        return

    logger.debug(f"Validating tag expression: {expr}")
    expr_norm = normalize_tag_expression(expr)
    logger.debug(f"Normalized expression for validation: {expr_norm}")

    def repl_token(m: re.Match[str]) -> str:
        token = m.group(0)
        if token in {"and", "or", "not"}:
            return token

        return "True"

    expr_dummy = re.sub(r"[A-Za-z0-9][A-Za-z0-9:_.\-]*", repl_token, expr_norm)
    logger.debug(f"Dummy expression for AST parsing: {expr_dummy}")

    try:
        tree = ast.parse(expr_dummy, mode="eval")
    except SyntaxError as e:
        logger.error(f"Syntax error in tag expression '{expr}': {e}")
        raise ValueError(f"Invalid tag expression syntax: {e}") from e

    def check_tree_node(node: ast.AST) -> None:
        logger.debug(f"Checking AST node: {type(node).__name__}")
        if isinstance(node, ast.Expression):
            check_tree_node(node.body)
        elif isinstance(node, ast.BoolOp):
            logger.debug(f"Checking boolean operation: {type(node.op).__name__}")
            for value in node.values:
                check_tree_node(value)
        elif isinstance(node, ast.UnaryOp):
            logger.debug(f"Checking unary operation: {type(node.op).__name__}")
            if not isinstance(node.op, ast.Not):
                logger.error(f"Unsupported unary operator: {type(node.op).__name__}")
                raise ValueError(f"Unsupported unary operator: {type(node.op).__name__}")
            check_tree_node(node.operand)
        elif isinstance(node, ast.Name):
            logger.debug(f"Checking name: {node.id}")
            if node.id not in ("True", "False", "and", "or", "not"):
                logger.error(f"Unexpected name in expression: {node.id}")
                raise ValueError(f"Unexpected name: {node.id}")
        elif isinstance(node, ast.Constant):
            logger.debug(f"Checking constant: {node.value}")
            if not isinstance(node.value, bool):
                logger.error(f"Unsupported constant type: {type(node.value).__name__}")
                raise ValueError(f"Unsupported constant type: {type(node.value).__name__}")
        else:
            logger.error(f"Unsupported AST node type: {type(node).__name__}")
            raise ValueError(f"Unsupported operation in tag expression: {type(node).__name__}")

    try:
        check_tree_node(tree)
        logger.debug("Tag expression validation successful")
    except ValueError as e:
        logger.error(f"Validation failed for tag expression '{expr}': {e}")
        raise ValueError(f"Invalid tag expression: {e}") from e


def evaluate_tag_expression(expr: str, node_tags: set[str]) -> bool:
    """Evaluate a tag expression against a set of node tags.

    Args:
        expr (str): The tag expression to evaluate.
        node_tags (set[str]): The set of tags available on the node.

    Returns:
        bool: True if the expression evaluates to True, False otherwise.
    """
    expr_norm = normalize_tag_expression(expr)
    logger.debug(f"Evaluating tag expression: {expr_norm}")

    def repl_token(m: re.Match[str]) -> str:
        token = m.group(0)
        if token in {"and", "or", "not"}:
            return token

        return "True" if token in node_tags else "False"

    expr_bool = re.sub(r"[A-Za-z0-9][A-Za-z0-9:_.\-]*", repl_token, expr_norm)
    logger.debug(f"Normalized expression: {expr_bool}")

    def evaluate_node(node: ast.AST) -> bool:
        logger.debug(f"Evaluating AST node: {type(node).__name__}")
        if isinstance(node, ast.Constant):
            val = bool(node.value)
            logger.debug(f"Constant evaluated to: {val}")
            return val
        if isinstance(node, ast.BoolOp):
            if isinstance(node.op, ast.And):
                logger.debug("Evaluating AND operation")
                return all(evaluate_node(val) for val in node.values)
            if isinstance(node.op, ast.Or):
                logger.debug("Evaluating OR operation")
                return any(evaluate_node(val) for val in node.values)
        if isinstance(node, ast.UnaryOp):
            if isinstance(node.op, ast.Not):
                logger.debug("Evaluating NOT operation")
                return not evaluate_node(node.operand)

        logger.error(f"Unsupported operation during evaluation: {type(node).__name__}")
        raise ValueError(f"Unsupported operation during evaluation: {type(node).__name__}")

    try:
        tree = ast.parse(expr_bool, mode="eval")
        result = evaluate_node(tree.body)
        logger.debug(f"Tag expression evaluation result: {result}")
        return result
    except (SyntaxError, ValueError) as e:
        logger.error(f"Error evaluating tag expression '{expr}': {e}")
        return False
