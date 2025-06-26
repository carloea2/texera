import ast
import subprocess
import json
import tempfile
import os
from typing import List, Dict, Any, Optional
import astor
from loguru import logger
import argparse

class PortOptimizer:
    def __init__(self):
        self.disabled_ports: List[int] = []

    def set_disabled_ports(self, port_config: Dict[int, bool]) -> None:
        """Set the disabled ports based on port configuration."""
        self.disabled_ports = {port for port, enabled in port_config.items() if not enabled}
        logger.debug(f"Disabled ports: {self.disabled_ports}")

    def _is_yield_to_disabled_port(self, node: ast.Yield) -> bool:
        """Check if a yield statement is sending to a disabled port."""
        logger.debug(f"Checking yield node: {ast.dump(node)}")
        if isinstance(node.value, ast.Tuple) and len(node.value.elts) == 2:
            port_value = node.value.elts[1]
            logger.debug(f"Port value: {ast.dump(port_value)}")
            if isinstance(port_value, ast.Num):
                is_disabled = port_value.n in self.disabled_ports
                if is_disabled:
                    logger.debug(f"Found disabled yield to port {port_value.n}")
                return is_disabled
        return False

    def _find_live_vars_and_yields(self, func_body):
        """Find variables used in yields and the index of the last yield."""
        live_vars = set()
        last_yield_idx = -1
        for idx, stmt in enumerate(func_body):
            # Recursively search for yield in this statement
            for node in ast.walk(stmt):
                if isinstance(node, ast.Yield):
                    last_yield_idx = idx
                    # Collect all variable names used in the yield
                    for n in ast.walk(node):
                        if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load):
                            live_vars.add(n.id)
        return live_vars, last_yield_idx

    def _remove_dead_code(self, func_node):
        """Remove assignments and statements after the last yield, and remove assignments not used in any yield, including in else branches."""
        if not isinstance(func_node, ast.FunctionDef):
            return func_node
        body = func_node.body
        live_vars, last_yield_idx = self._find_live_vars_and_yields(body)
        if last_yield_idx == -1:
            # No yields, remove all
            func_node.body = []
            return func_node
        # Only keep code up to and including the last yield
        new_body = body[:last_yield_idx+1]
        # Walk backwards to remove dead assignments
        current_live = set(live_vars)
        pruned_body = []
        for stmt in reversed(new_body):
            if isinstance(stmt, ast.Assign):
                assigned = set()
                for t in stmt.targets:
                    if isinstance(t, ast.Name):
                        assigned.add(t.id)
                    elif isinstance(t, ast.Subscript) and isinstance(t.value, ast.Name):
                        assigned.add(t.value.id)
                # Only keep if assigned variable is live and used in a yield after this assignment
                if assigned & current_live:
                    pruned_body.append(stmt)
                    current_live |= assigned
                # else: skip dead assignment
            elif isinstance(stmt, ast.AugAssign):
                target = stmt.target
                if isinstance(target, ast.Name) and target.id in current_live:
                    pruned_body.append(stmt)
                    current_live.add(target.id)
                elif isinstance(target, ast.Subscript) and isinstance(target.value, ast.Name) and target.value.id in current_live:
                    pruned_body.append(stmt)
                    current_live.add(target.value.id)
                # else: skip dead augassign
            elif isinstance(stmt, ast.If):
                # Recursively prune dead code in if/else branches
                stmt.body = self._prune_dead_code_block(stmt.body, current_live)
                stmt.orelse = self._prune_dead_code_block(stmt.orelse, current_live)
                # Only keep if either branch is not empty
                if stmt.body or stmt.orelse:
                    pruned_body.append(stmt)
            else:
                pruned_body.append(stmt)
        pruned_body = list(reversed(pruned_body))
        func_node.body = pruned_body
        return func_node

    def _prune_dead_code_block(self, block, live_vars):
        """Helper to prune dead assignments in a block, given live vars after the block."""
        pruned = []
        current_live = set(live_vars)
        for stmt in reversed(block):
            if isinstance(stmt, ast.Assign):
                assigned = set()
                for t in stmt.targets:
                    if isinstance(t, ast.Name):
                        assigned.add(t.id)
                    elif isinstance(t, ast.Subscript) and isinstance(t.value, ast.Name):
                        assigned.add(t.value.id)
                if assigned & current_live:
                    pruned.append(stmt)
                    current_live |= assigned
            elif isinstance(stmt, ast.AugAssign):
                target = stmt.target
                if isinstance(target, ast.Name) and target.id in current_live:
                    pruned.append(stmt)
                    current_live.add(target.id)
                elif isinstance(target, ast.Subscript) and isinstance(target.value, ast.Name) and target.value.id in current_live:
                    pruned.append(stmt)
                    current_live.add(target.value.id)
            elif isinstance(stmt, ast.If):
                stmt.body = self._prune_dead_code_block(stmt.body, current_live)
                stmt.orelse = self._prune_dead_code_block(stmt.orelse, current_live)
                if stmt.body or stmt.orelse:
                    pruned.append(stmt)
            else:
                pruned.append(stmt)
        return list(reversed(pruned))

    def _process_node(self, node):
        """Process a single AST node."""
        logger.debug(f"Processing node: {type(node).__name__}")
        logger.debug(f"Node details: {ast.dump(node)}")
        
        if isinstance(node, ast.If):
            logger.debug(f"If node test: {ast.dump(node.test)}")
            node.test = self._process_node(node.test)
            new_body = []
            for stmt in node.body:
                processed = self._process_node(stmt)
                if processed is not None:
                    new_body.append(processed)
            node.body = new_body
            new_orelse = []
            for stmt in node.orelse:
                processed = self._process_node(stmt)
                if processed is not None:
                    new_orelse.append(processed)
            # Remove empty else branch
            node.orelse = new_orelse if new_orelse else []
            # If both body and else are empty, remove the if entirely
            if not node.body and not node.orelse:
                return None
            # If only the body is empty, invert the condition and use the else as the new body
            if not node.body and node.orelse:
                return ast.If(
                    test=ast.UnaryOp(op=ast.Not(), operand=node.test),
                    body=node.orelse,
                    orelse=[]
                )
            return node
            
        elif isinstance(node, ast.Expr):
            logger.debug(f"Processing Expr node: {ast.dump(node)}")
            if isinstance(node.value, ast.Yield):
                logger.debug(f"Found yield in Expr: {ast.dump(node.value)}")
                if self._is_yield_to_disabled_port(node.value):
                    logger.debug(f"Removing yield statement: {ast.dump(node)}")
                    return None
            return node
            
        elif isinstance(node, ast.AugAssign):
            logger.debug(f"Processing AugAssign: {ast.dump(node)}")
            node.target = self._process_node(node.target)
            node.value = self._process_node(node.value)
            return node
            
        elif isinstance(node, ast.Assign):
            logger.debug(f"Processing Assign: {ast.dump(node)}")
            node.targets = [self._process_node(t) for t in node.targets]
            node.value = self._process_node(node.value)
            return node
            
        elif isinstance(node, ast.Compare):
            logger.debug(f"Processing Compare: {ast.dump(node)}")
            node.left = self._process_node(node.left)
            node.comparators = [self._process_node(c) for c in node.comparators]
            return node
            
        elif isinstance(node, ast.Subscript):
            logger.debug(f"Processing Subscript: {ast.dump(node)}")
            node.value = self._process_node(node.value)
            node.slice = self._process_node(node.slice)
            return node
            
        elif isinstance(node, ast.Name):
            logger.debug(f"Processing Name: {ast.dump(node)}")
            return node
            
        elif isinstance(node, ast.Constant):
            logger.debug(f"Processing Constant: {ast.dump(node)}")
            return node
            
        elif isinstance(node, ast.Tuple):
            logger.debug(f"Processing Tuple: {ast.dump(node)}")
            node.elts = [self._process_node(e) for e in node.elts]
            return node
            
        elif isinstance(node, ast.Module):
            logger.debug(f"Processing Module: {ast.dump(node)}")
            node.body = [self._process_node(b) for b in node.body]
            return node
            
        elif isinstance(node, ast.ClassDef):
            logger.debug(f"Processing ClassDef: {ast.dump(node)}")
            node.body = [self._process_node(b) for b in node.body if self._process_node(b) is not None]
            if not node.body:
                return None
            return node
            
        elif isinstance(node, ast.FunctionDef):
            logger.debug(f"Processing FunctionDef: {ast.dump(node)}")
            node.body = [self._process_node(b) for b in node.body if self._process_node(b) is not None]
            if not node.body:
                return None
            # Dead code removal pass
            node = self._remove_dead_code(node)
            return node
            
        else:
            logger.debug(f"Unhandled node type: {type(node).__name__}")
            return node

    def _unparse(self, node, indent_level=0):
        """Convert an AST node back to source code with proper indentation."""
        logger.debug(f"Unparsing node: {type(node).__name__}")
        logger.debug(f"Node details: {ast.dump(node)}")
        
        indent = "    " * indent_level
        
        if isinstance(node, ast.Module):
            return "\n".join(self._unparse(stmt, 0) for stmt in node.body)
            
        elif isinstance(node, ast.ClassDef):
            class_def = f"class {node.name}:"
            body = "\n".join(self._unparse(stmt, indent_level + 1) for stmt in node.body)
            return f"{class_def}\n{body}"
            
        elif isinstance(node, ast.FunctionDef):
            args = []
            for arg in node.args.args:
                if hasattr(arg, 'annotation') and arg.annotation:
                    args.append(f"{arg.arg}: {self._unparse(arg.annotation, 0)}")
                else:
                    args.append(arg.arg)
            func_def = f"{indent}def {node.name}({', '.join(args)}):"
            body = "\n".join(self._unparse(stmt, indent_level + 1) for stmt in node.body)
            return f"{func_def}\n{body}"
            
        elif isinstance(node, ast.If):
            test = self._unparse(node.test, 0)
            if_line = f"{indent}if {test}:"
            body = "\n".join(self._unparse(stmt, indent_level + 1) for stmt in node.body)
            result = f"{if_line}\n{body}"
            
            if node.orelse:
                else_body = "\n".join(self._unparse(stmt, indent_level + 1) for stmt in node.orelse)
                result += f"\n{indent}else:\n{else_body}"
            
            return result
            
        elif isinstance(node, ast.Compare):
            left = self._unparse(node.left, 0)
            op_map = {ast.Gt: ">", ast.Lt: "<", ast.Eq: "==", ast.NotEq: "!=",
                     ast.GtE: ">=", ast.LtE: "<="}
            ops = [op_map[type(op)] for op in node.ops]
            comparators = [self._unparse(c, 0) for c in node.comparators]
            return f"{left} {' '.join(f'{op} {c}' for op, c in zip(ops, comparators))}"
            
        elif isinstance(node, ast.BinOp):
            op_map = {ast.Add: "+", ast.Sub: "-", ast.Mult: "*", ast.Div: "/",
                     ast.FloorDiv: "//", ast.Mod: "%", ast.Pow: "**"}
            left = self._unparse(node.left, 0)
            right = self._unparse(node.right, 0)
            op = op_map[type(node.op)]
            return f"{left} {op} {right}"
            
        elif isinstance(node, ast.AugAssign):
            target = self._unparse(node.target, 0)
            value = self._unparse(node.value, 0)
            op_map = {ast.Add: "+=", ast.Sub: "-=", ast.Mult: "*=", ast.Div: "/=",
                     ast.FloorDiv: "//=", ast.Mod: "%=", ast.Pow: "**="}
            op = op_map[type(node.op)]
            return f"{indent}{target} {op} {value}"
            
        elif isinstance(node, ast.Assign):
            targets = [self._unparse(t, 0) for t in node.targets]
            value = self._unparse(node.value, 0)
            return f"{indent}{' = '.join(targets)} = {value}"
            
        elif isinstance(node, ast.Expr):
            return f"{indent}{self._unparse(node.value, 0)}"
            
        elif isinstance(node, ast.Yield):
            value = self._unparse(node.value, 0)
            return f"yield {value}"
            
        elif isinstance(node, ast.Tuple):
            elts = [self._unparse(e, 0) for e in node.elts]
            return f"({', '.join(elts)})"
            
        elif isinstance(node, ast.Name):
            return node.id
            
        elif isinstance(node, ast.Constant):
            if isinstance(node.value, str):
                # Use double quotes for string literals
                return f'"{node.value}"'
            return str(node.value)
            
        elif isinstance(node, ast.Subscript):
            value = self._unparse(node.value, 0)
            slice_value = self._unparse(node.slice, 0)
            return f"{value}[{slice_value}]"
            
        else:
            logger.debug(f"Unhandled node type in unparse: {type(node).__name__}")
            return str(node)

    def optimize_code(self, code_str: str) -> str:
        """Optimize the code by removing logic related to disabled ports."""
        try:
            tree = ast.parse(code_str)
            logger.debug("Parsed AST:")
            logger.debug(ast.dump(tree, indent=2))
            
            # First pass: process and remove disabled port code
            new_body = []
            for node in tree.body:
                processed = self._process_node(node)
                if processed is not None:
                    new_body.append(processed)
            tree.body = new_body
            
            # Log the optimized AST before unparsing
            logger.debug("Optimized AST before unparsing:")
            logger.debug(ast.dump(tree, indent=2))
            
            optimized_code = self._unparse(tree)
            print(optimized_code)
            print("--------------------------------")
            
            # Second pass: use autoflake to remove unused code
            logger.info("Starting autoflake analysis for unused code")
            with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
                f.write(optimized_code)
                temp_path = f.name
                logger.debug(f"Created temporary file for autoflake: {temp_path}")
            
            try:
                # Run autoflake as a subprocess
                logger.debug("Running autoflake subprocess")
                result = subprocess.run(['autoflake', '--remove-all-unused-imports', '--remove-unused-variables', '--in-place', temp_path], 
                                     capture_output=True, text=True)
                logger.debug(f"Autoflake return code: {result.returncode}")
                logger.debug(f"Autoflake stdout: {result.stdout}")
                logger.debug(f"Autoflake stderr: {result.stderr}")
                
                if result.returncode == 0:
                    # Read the optimized file
                    with open(temp_path, 'r') as f:
                        optimized_code = f.read()
                    logger.info("Successfully removed unused code")
                else:
                    logger.warning("Autoflake analysis failed")
            finally:
                # Clean up the temporary file
                os.unlink(temp_path)
                logger.debug("Cleaned up temporary file")
            
            return optimized_code
        except Exception as e:
            logger.error(f"Error processing code: {str(e)}")
            raise ValueError(f"Error processing code: {str(e)}") 

def optimize_udf(code_str: str, port_config: Dict[int, bool]) -> str:
    """Main function to optimize UDF code based on port configuration."""
    optimizer = PortOptimizer()
    optimizer.set_disabled_ports(port_config)
    return optimizer.optimize_code(code_str)

def main():
    parser = argparse.ArgumentParser(description="Optimize a Python UDF file based on port config.")
    parser.add_argument("--input", "-i", required=True, help="Path to the input Python file.")
    parser.add_argument("--config", "-c", required=True, help="Path to the port config JSON file.")
    args = parser.parse_args()

    with open(args.input, "r") as f:
        code_str = f.read()
    with open(args.config, "r") as f:
        port_config = json.load(f)
    port_config = {int(k): v for k, v in port_config.items()}
    optimized = optimize_udf(code_str, port_config)
    print(optimized)

if __name__ == "__main__":
    main() 