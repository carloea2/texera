import os
import sys
import json
import ast
from loguru import logger
import pytest

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.port_optimizer import optimize_udf

def normalize_ast(node):
    """Normalize AST by removing docstrings and type annotations."""
    if isinstance(node, ast.AST):
        for field, old_value in ast.iter_fields(node):
            if isinstance(old_value, list):
                new_values = []
                for value in old_value:
                    if isinstance(value, ast.Expr) and isinstance(value.value, ast.Str):
                        continue  # Skip docstrings
                    if isinstance(value, ast.AnnAssign):
                        # Convert AnnAssign to Assign
                        new_values.append(ast.Assign(
                            targets=[value.target],
                            value=value.value,
                            lineno=value.lineno,
                            col_offset=value.col_offset
                        ))
                    else:
                        new_values.append(normalize_ast(value))
                setattr(node, field, new_values)
            elif isinstance(old_value, ast.AST):
                setattr(node, field, normalize_ast(old_value))
    return node

@pytest.mark.parametrize("test_dir", [
    os.path.join(os.path.dirname(__file__), 'test1'),
    os.path.join(os.path.dirname(__file__), 'test2'),
])
def test_port_optimizer(test_dir):
    with open(os.path.join(test_dir, 'input.py'), 'r') as f:
        input_code = f.read()
    with open(os.path.join(test_dir, 'expected_output.py'), 'r') as f:
        expected_code = f.read()
    with open(os.path.join(test_dir, 'port_config.json'), 'r') as f:
        port_config = json.load(f)
    port_config = {int(k): v for k, v in port_config.items()}
    optimized_code = optimize_udf(input_code, port_config)
    logger.info(f"Optimized code for {test_dir}:")
    logger.info(optimized_code)
    logger.info(f"Expected code for {test_dir}:")
    logger.info(expected_code)
    optimized_ast = ast.parse(optimized_code)
    expected_ast = ast.parse(expected_code)
    normalized_optimized = normalize_ast(optimized_ast)
    normalized_expected = normalize_ast(expected_ast)
    assert ast.dump(normalized_optimized, include_attributes=False) == ast.dump(normalized_expected, include_attributes=False), \
        f"Optimized code AST does not match expected output AST for {test_dir}" 