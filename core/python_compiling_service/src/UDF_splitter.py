import ast
import astor
from collections import defaultdict, deque
import re

# Import the loop transformer
try:
    from .loop_transformer import transform_code as transform_loop_code
except ImportError:
    # Fallback for when running as script
    import sys
    import os
    sys.path.append(os.path.dirname(__file__))
    from loop_transformer import transform_code as transform_loop_code


def preprocess_code(code_snippet: str) -> str:
    """
    Preprocess code by removing docstrings, comments, and empty lines.
    Ensures that empty function/class/module bodies have a 'pass' statement.
    Args:
        code_snippet (str): A string containing source code
    Returns:
        str: Cleaned code without docstrings, comments, or empty lines, and valid Python structure
    """
    # Step 1: Remove docstrings using AST
    class DocstringRemover(ast.NodeTransformer):
        def visit_FunctionDef(self, node):
            self.generic_visit(node)
            if (node.body and isinstance(node.body[0], ast.Expr) and
                isinstance(node.body[0].value, ast.Str)):
                node.body = node.body[1:]
            return node
        def visit_AsyncFunctionDef(self, node):
            self.generic_visit(node)
            if (node.body and isinstance(node.body[0], ast.Expr) and
                isinstance(node.body[0].value, ast.Str)):
                node.body = node.body[1:]
            return node
        def visit_ClassDef(self, node):
            self.generic_visit(node)
            if (node.body and isinstance(node.body[0], ast.Expr) and
                isinstance(node.body[0].value, ast.Str)):
                node.body = node.body[1:]
            return node
        def visit_Module(self, node):
            self.generic_visit(node)
            if (node.body and isinstance(node.body[0], ast.Expr) and
                isinstance(node.body[0].value, ast.Str)):
                node.body = node.body[1:]
            return node

    try:
        tree = ast.parse(code_snippet)
        tree = DocstringRemover().visit(tree)
        code_wo_docstrings = astor.to_source(tree)
    except Exception:
        # If AST parsing fails, fallback to original code
        code_wo_docstrings = code_snippet

    # Step 2: Remove comments and empty lines, but preserve indentation
    lines = code_wo_docstrings.split('\n')
    cleaned_lines = []
    for line in lines:
        stripped_line = line.lstrip()
        # Skip empty lines
        if not stripped_line:
            continue
        # Skip comment-only lines
        if stripped_line.startswith('#'):
            continue
        # Remove inline comments (everything after #), but keep indentation
        if '#' in line:
            comment_pos = line.find('#')
            code_part = line[:comment_pos].rstrip()
            if not code_part.strip():
                continue
            cleaned_lines.append(code_part)
        else:
            cleaned_lines.append(line.rstrip())
    cleaned_code = '\n'.join(cleaned_lines)

    # Step 3: Ensure all function/class/module bodies are not empty (insert 'pass' if needed)
    class EmptyBodyFixer(ast.NodeTransformer):
        def visit_FunctionDef(self, node):
            self.generic_visit(node)
            if not node.body:
                node.body = [ast.Pass()]
            return node
        def visit_AsyncFunctionDef(self, node):
            self.generic_visit(node)
            if not node.body:
                node.body = [ast.Pass()]
            return node
        def visit_ClassDef(self, node):
            self.generic_visit(node)
            if not node.body:
                node.body = [ast.Pass()]
            return node
        def visit_Module(self, node):
            self.generic_visit(node)
            if not node.body:
                node.body = [ast.Pass()]
            return node

    try:
        tree = ast.parse(cleaned_code)
        tree = EmptyBodyFixer().visit(tree)
        fixed_code = astor.to_source(tree)
        return fixed_code
    except Exception:
        # If AST parsing fails, fallback to cleaned code
        return cleaned_code


def SSA(code_snippet: str) -> str:
    """
    Convert a function definition to Static Single Assignment (SSA) format.
    
    Args:
        code_snippet (str): A string containing a function definition (already cleaned)
        
    Returns:
        str: The function converted to SSA format
    """
    try:
        # Parse the code into an AST (code is already cleaned)
        tree = ast.parse(code_snippet)
        
        # Transform the AST to SSA format
        ssa_transformer = SSATransformer()
        ssa_tree = ssa_transformer.visit(tree)
        
        # Flatten lists in function body (for tuple assignments)
        for node in ast.walk(ssa_tree):
            if isinstance(node, ast.FunctionDef):
                new_body = []
                for stmt in node.body:
                    if isinstance(stmt, list):
                        new_body.extend(stmt)
                    else:
                        new_body.append(stmt)
                node.body = new_body
        
        # Convert back to source code using astor
        return astor.to_source(ssa_tree)
    
    except Exception as e:
        raise ValueError(f"Failed to convert code to SSA format: {e}")


class SSATransformer(ast.NodeTransformer):
    """AST transformer to convert code to SSA format."""
    
    def __init__(self):
        self.variable_counter = {}
        self.scope_stack = []
    
    def visit_FunctionDef(self, node):
        """Visit function definition and process its body."""
        # Initialize variable counter for this function
        self.variable_counter = {}
        self.scope_stack.append(set())
        
        # Process function arguments (they start with version 0)
        for arg in node.args.args:
            self.variable_counter[arg.arg] = 0
            self.scope_stack[-1].add(arg.arg)
        
        # Process the function body
        node.body = [self.visit(stmt) for stmt in node.body]
        
        self.scope_stack.pop()
        return node
    
    def visit_Assign(self, node):
        """Visit assignment statements and rename variables."""
        # Handle tuple assignments
        if len(node.targets) == 1 and isinstance(node.targets[0], ast.Tuple):
            return self._handle_tuple_assignment(node)
        
        # Handle normal assignments
        node.value = self.visit(node.value)
        new_targets = []
        for target in node.targets:
            if isinstance(target, ast.Name):
                var_name = target.id
                if var_name not in self.variable_counter:
                    self.variable_counter[var_name] = 0
                else:
                    self.variable_counter[var_name] += 1
                new_target = ast.Name(
                    id=f"{var_name}{self.variable_counter[var_name] if self.variable_counter[var_name] > 0 else ''}",
                    ctx=target.ctx
                )
                new_targets.append(new_target)
                if self.scope_stack:
                    self.scope_stack[-1].add(var_name)
            else:
                new_targets.append(self.visit(target))
        node.targets = new_targets
        return node
    
    def visit_AugAssign(self, node):
        """Visit augmented assignment statements (+=, -=, &=, etc.) and convert to SSA."""
        # Convert augmented assignment to regular assignment
        # e.g., x += 1 becomes x = x + 1
        
        # First, visit the value to get the latest versions
        node.value = self.visit(node.value)
        
        if isinstance(node.target, ast.Name):
            var_name = node.target.id
            
            # Get the latest version of the variable for the right side
            latest_version = self.variable_counter.get(var_name, 0)
            if latest_version > 0:
                current_var = ast.Name(id=f"{var_name}{latest_version}", ctx=ast.Load())
            else:
                current_var = ast.Name(id=var_name, ctx=ast.Load())
            
            # Create the binary operation
            bin_op = ast.BinOp(
                left=current_var,
                op=node.op,
                right=node.value
            )
            
            # Increment the variable counter
            if var_name not in self.variable_counter:
                self.variable_counter[var_name] = 0
            else:
                self.variable_counter[var_name] += 1
            
            # Create the new assignment
            new_target = ast.Name(
                id=f"{var_name}{self.variable_counter[var_name] if self.variable_counter[var_name] > 0 else ''}",
                ctx=ast.Store()
            )
            
            if self.scope_stack:
                self.scope_stack[-1].add(var_name)
            
            return ast.Assign(
                targets=[new_target],
                value=bin_op
            )
        
        return node
    
    def _handle_tuple_assignment(self, node):
        """Handle tuple assignments like 'a, b = b, a'."""
        targets = node.targets[0].elts
        values = node.value.elts if isinstance(node.value, ast.Tuple) else [node.value]
        
        # Create temporary variables for all right-hand side values
        temp_assignments = []
        temp_vars = []
        
        for i, value in enumerate(values):
            temp_name = f"_tmp_{i}"
            temp_vars.append(temp_name)
            temp_assign = ast.Assign(
                targets=[ast.Name(id=temp_name, ctx=ast.Store())],
                value=self.visit(value)
            )
            temp_assignments.append(temp_assign)
        
        # Create assignments from temporaries to targets
        target_assignments = []
        for i, target in enumerate(targets):
            if isinstance(target, ast.Name):
                var_name = target.id
                if var_name not in self.variable_counter:
                    self.variable_counter[var_name] = 0
                else:
                    self.variable_counter[var_name] += 1
                new_target = ast.Name(
                    id=f"{var_name}{self.variable_counter[var_name] if self.variable_counter[var_name] > 0 else ''}",
                    ctx=ast.Store()
                )
                if self.scope_stack:
                    self.scope_stack[-1].add(var_name)
                
                # Assign from corresponding temporary
                if i < len(temp_vars):
                    target_assign = ast.Assign(
                        targets=[new_target],
                        value=ast.Name(id=temp_vars[i], ctx=ast.Load())
                    )
                    target_assignments.append(target_assign)
        
        # Return all assignments as a list
        return temp_assignments + target_assignments
    
    def visit_Name(self, node):
        """Visit name nodes and update references to use the latest version."""
        if isinstance(node.ctx, ast.Load):  # Only rename when reading the variable
            var_name = node.id
            
            # Check if this variable has been assigned in the current scope
            if self.scope_stack and var_name in self.scope_stack[-1]:
                # Use the latest version of the variable
                latest_version = self.variable_counter.get(var_name, 0)
                if latest_version > 0:
                    return ast.Name(
                        id=f"{var_name}{latest_version}",
                        ctx=node.ctx
                    )
                else:
                    return ast.Name(
                        id=var_name,
                        ctx=node.ctx
                    )
        
        return node
    
    def visit_Return(self, node):
        """Visit return statements and update variable references."""
        if node.value:
            node.value = self.visit(node.value)
        return node
    
    def visit_BinOp(self, node):
        """Visit binary operations and update variable references."""
        node.left = self.visit(node.left)
        node.right = self.visit(node.right)
        return node
    
    def visit_UnaryOp(self, node):
        """Visit unary operations and update variable references."""
        node.operand = self.visit(node.operand)
        return node
    
    def visit_Call(self, node):
        """Visit function calls and update variable references."""
        node.func = self.visit(node.func)
        node.args = [self.visit(arg) for arg in node.args]
        node.keywords = [self.visit(keyword) for keyword in node.keywords]
        return node
    
    def visit_keyword(self, node):
        """Visit keyword arguments and update variable references."""
        node.value = self.visit(node.value)
        return node


class VariableDependencyGraph:
    """
    A class to analyze variable dependencies in SSA form code.
    
    Each vertex is a tuple (variable, line_number).
    Edges represent dependencies:
    1. If the same variable appears on different lines, there's an edge from lower to higher line number
    2. If X = Y (assignment), then there's an edge from Y to X
    """
    
    def __init__(self, ssa_code_snippet: str):
        """
        Initialize the dependency graph from SSA form code.
        
        Args:
            ssa_code_snippet (str): Code snippet in SSA format
        """
        self.ssa_code = ssa_code_snippet
        self.vertices = set()  # Set of all vertices as (variable, line_number) tuples
        self.edges = defaultdict(set)  # Adjacency list: vertex -> set of dependent vertices
        self.reverse_edges = defaultdict(set)  # Reverse adjacency list: vertex -> set of vertices that depend on it
        self.variable_versions = defaultdict(list)  # variable_name -> list of versions
        self.variable_lines = {}  # variable -> line number where defined
        self.variable_usage_lines = defaultdict(list)  # variable -> list of line numbers where used
        self.variable_types = {}  # variable -> inferred type
        
        self._build_graph()
    
    def _build_graph(self):
        """Build the dependency graph by parsing the SSA code."""
        try:
            tree = ast.parse(self.ssa_code)
            visitor = DependencyVisitor()
            visitor.visit(tree)
            
            # Extract information from the visitor
            self.variable_lines = visitor.variable_lines
            self.variable_usage_lines = visitor.variable_usage_lines
            self.variable_versions = visitor.variable_versions
            
            # Build vertices as (variable, line_number) tuples
            self.vertices = set()
            for variable, line_num in self.variable_lines.items():
                self.vertices.add((variable, line_num))
            
            # Add usage lines as vertices too
            for variable, usage_lines in self.variable_usage_lines.items():
                for line_num in usage_lines:
                    self.vertices.add((variable, line_num))
            
            # Build edges based on the visitor's dependency information
            self._build_edges_from_visitor(visitor)
            
            # Build reverse edges
            for vertex, dependents in self.edges.items():
                for dependent in dependents:
                    self.reverse_edges[dependent].add(vertex)
                    
        except Exception as e:
            raise ValueError(f"Failed to build dependency graph: {e}")
    
    def _build_edges_from_visitor(self, visitor):
        """Build edges based on the visitor's dependency information."""
        # Rule 1: Same variable edges (lower line number to higher line number)
        variable_line_mapping = defaultdict(list)
        for vertex in self.vertices:
            variable, line_num = vertex
            variable_line_mapping[variable].append((line_num, vertex))
        
        for variable, line_vertices in variable_line_mapping.items():
            # Sort by line number
            line_vertices.sort(key=lambda x: x[0])
            
            # Add edges from lower to higher line numbers (only for same variable)
            for i in range(len(line_vertices) - 1):
                current_vertex = line_vertices[i][1]
                next_vertex = line_vertices[i + 1][1]
                self.edges[current_vertex].add(next_vertex)
        
        # Rule 2: Assignment dependencies (only actual assignments, not variable versions)
        for target_var, dependencies in visitor.dependencies.items():
            target_line = visitor.variable_lines.get(target_var)
            if target_line is not None:
                target_vertex = (target_var, target_line)
                
                for dep_var in dependencies:
                    # Find the latest usage line of dep_var before or at the assignment
                    dep_lines = [l for l in visitor.variable_usage_lines.get(dep_var, []) if l <= target_line]
                    if dep_lines:
                        latest_line = max(dep_lines)
                        dep_vertex = (dep_var, latest_line)
                        if dep_var != target_var:
                            self.edges[dep_vertex].add(target_vertex)
        
        # Store type information for size estimation
        self.variable_types = visitor.variable_types
    
    def get_vertices(self):
        """Get all vertices (variable, line_number) in the graph."""
        return list(self.vertices)
    
    def get_edges(self):
        """Get all edges in the graph as a list of tuples ((from_var, from_line), (to_var, to_line))."""
        edges = []
        for from_vertex, to_vertices in self.edges.items():
            for to_vertex in to_vertices:
                edges.append((from_vertex, to_vertex))
        return edges
    
    def get_dependents(self, vertex):
        """Get all vertices that depend on the given vertex."""
        return list(self.edges.get(vertex, set()))
    
    def get_dependencies(self, vertex):
        """Get all vertices that the given vertex depends 
        on."""
        return list(self.reverse_edges.get(vertex, set()))
    
    def get_variable_versions(self, variable_name):
        """Get all versions of a variable."""
        return self.variable_versions.get(variable_name, [])
    
    def get_variable_line(self, variable):
        """Get the line number where a variable is defined."""
        return self.variable_lines.get(variable, None)
    
    def get_variable_usage_lines(self, variable):
        """Get all line numbers where a variable is used."""
        return self.variable_usage_lines.get(variable, [])
    
    def get_vertices_by_variable(self, variable):
        """Get all vertices for a specific variable."""
        return [vertex for vertex in self.vertices if vertex[0] == variable]
    
    def find_valid_cuts(self):
        """
        Find valid cut points in the dependency graph.
        A valid cut must only cut through temporal (blue) edges, not assignment (red) edges.
        
        Returns:
            list: List of valid cut points, each containing line number and the edge being cut
        """
        valid_cuts = []
        
        # Group vertices by line number
        line_groups = defaultdict(list)
        for vertex in self.vertices:
            var, line = vertex
            line_groups[line].append(vertex)
        
        # Check each line as a potential cut point
        for line_num in sorted(line_groups.keys()):
            if line_num == 1:  # Skip function definition line
                continue
                
            # Get all edges that cross this line
            crossing_edges = self._get_edges_crossing_line(line_num)
            
            # Check if all crossing edges are temporal (same variable)
            if self._are_all_temporal_edges(crossing_edges):
                valid_cuts.append({
                    'line_number': line_num,
                    'crossing_edges': crossing_edges,
                    'description': f"Valid cut at line {line_num} - cuts {len(crossing_edges)} temporal edge(s)"
                })
        
        return valid_cuts
    
    def _get_edges_crossing_line(self, line_num):
        """
        Get all edges that cross the given line number.
        An edge crosses a line if one vertex is before the line and one is at or after the line.
        """
        crossing_edges = []
        
        for from_vertex, to_vertices in self.edges.items():
            from_var, from_line = from_vertex
            for to_vertex in to_vertices:
                to_var, to_line = to_vertex
                
                # Check if edge crosses the line
                if from_line < line_num and to_line >= line_num:
                    crossing_edges.append((from_vertex, to_vertex))
        
        return crossing_edges
    
    def _are_all_temporal_edges(self, edges):
        """
        Check if all edges are temporal (same variable).
        Temporal edges connect the same variable at different lines.
        """
        for from_vertex, to_vertex in edges:
            from_var, from_line = from_vertex
            to_var, to_line = to_vertex
            
            # If variables are different, it's not a temporal edge
            if from_var != to_var:
                return False
        
        return True
    
    def rank_cuts_by_variable_size(self, valid_cuts):
        """
        Rank valid cuts by the size of variables being cut through and argument usage heuristic.
        Smaller variables are preferred for cuts, and cuts at L-1 are favored if an argument
        is first used at line L.
        
        Args:
            valid_cuts (list): List of valid cut points
            
        Returns:
            list: Ranked list of cuts (smallest variable size first, with argument usage bonus)
        """
        ranked_cuts = []
        
        # Find first usage line for each argument
        argument_first_usage = {}
        for vertex in self.vertices:
            var_name, line_num = vertex
            # Check if this is an argument (has type annotation)
            if var_name in self.variable_types:
                if var_name not in argument_first_usage or line_num < argument_first_usage[var_name]:
                    argument_first_usage[var_name] = line_num
        
        for cut in valid_cuts:
            # Calculate the total size of variables being cut through
            total_size = 0
            cut_variables = set()
            
            for from_vertex, to_vertex in cut['crossing_edges']:
                from_var, from_line = from_vertex
                to_var, to_line = to_vertex
                
                # Since these are temporal edges, from_var == to_var
                var_name = from_var
                cut_variables.add(var_name)
                
                # Estimate variable size based on type
                var_size = self._estimate_variable_size(var_name)
                total_size += var_size
            
            # Apply argument usage heuristic
            heuristic_bonus = 0
            cut_line = cut['line_number']
            
            # Check if cutting at this line is favorable for any argument
            for arg_name, first_usage_line in argument_first_usage.items():
                if cut_line == first_usage_line - 1:
                    # This is a favorable cut - reduce the rank score
                    heuristic_bonus = -50000  # Large bonus to favor this cut
            
            # Create ranked cut entry
            ranked_cut = {
                'line_number': cut['line_number'],
                'crossing_edges': cut['crossing_edges'],
                'description': cut['description'],
                'cut_variables': list(cut_variables),
                'total_variable_size': total_size,
                'average_variable_size': total_size / len(cut_variables) if cut_variables else 0,
                'rank_score': total_size + heuristic_bonus,  # Lower is better
                'heuristic_bonus': heuristic_bonus
            }
            
            ranked_cuts.append(ranked_cut)
        
        # Sort by rank score (smallest first)
        ranked_cuts.sort(key=lambda x: x['rank_score'])
        
        return ranked_cuts
    
    def _estimate_variable_size(self, var_name):
        """
        Estimate the size of a variable based on type information.
        DataFrames and complex data structures are much larger than simple types.
        
        Args:
            var_name (str): Variable name
            
        Returns:
            int: Estimated size (bytes)
        """
        # Type-based size estimation
        type_sizes = {
            # Basic types
            'int': 8,
            'float': 8,
            'str': 20,
            'bool': 1,
            'numeric': 8,  # For arithmetic operations
            
            # Collection types
            'list': 1000,
            'dict': 5000,
            'tuple': 1000,
            'set': 5000,
            
            # DataFrame types (very large)
            'DataFrame': 100000,  # 100KB for DataFrames
            'Series': 10000,      # 10KB for Series
            
            # Unknown type
            'unknown': 8
        }
        
        # Get the type for this variable
        var_type = self.variable_types.get(var_name, 'unknown')
        
        # Return the size based on type
        return type_sizes.get(var_type, type_sizes['unknown'])
    
    def has_cycle(self):
        """Check if the dependency graph has cycles using DFS."""
        visited = set()
        rec_stack = set()
        
        def dfs(vertex):
            visited.add(vertex)
            rec_stack.add(vertex)
            
            for neighbor in self.edges.get(vertex, set()):
                if neighbor not in visited:
                    if dfs(neighbor):
                        return True
                elif neighbor in rec_stack:
                    return True
            
            rec_stack.remove(vertex)
            return False
        
        for vertex in self.vertices:
            if vertex not in visited:
                if dfs(vertex):
                    return True
        return False
    
    def get_topological_order(self):
        """Get topological ordering of vertices (if no cycles)."""
        if self.has_cycle():
            raise ValueError("Cannot get topological order: graph contains cycles")
        
        in_degree = defaultdict(int)
        for vertex in self.vertices:
            in_degree[vertex] = len(self.get_dependencies(vertex))
        
        queue = deque([vertex for vertex in self.vertices if in_degree[vertex] == 0])
        result = []
        
        while queue:
            vertex = queue.popleft()
            result.append(vertex)
            
            for dependent in self.get_dependents(vertex):
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)
        
        return result
    
    def visualize(self):
        """Return a string representation of the graph."""
        result = "Variable Dependency Graph:\n"
        result += "=" * 30 + "\n"
        
        for vertex in sorted(self.vertices):
            variable, line_num = vertex
            dependents = self.get_dependents(vertex)
            dependencies = self.get_dependencies(vertex)
            
            result += f"Vertex: ({variable}, line {line_num})\n"
            result += f"  Dependencies: {dependencies}\n"
            result += f"  Dependents: {dependents}\n"
            result += "-" * 20 + "\n"
        
        return result
    
    def visualize_text(self):
        """Return a visual ASCII representation of the dependency graph."""
        if not self.vertices:
            return "Empty graph"
        
        # Sort vertices for consistent output (by variable name, then line number)
        sorted_vertices = sorted(self.vertices, key=lambda x: (x[0], x[1]))
        
        # Create the visual representation
        result = []
        result.append("Variable Dependency Graph (Visual)")
        result.append("=" * 50)
        result.append("")
        
        # Create a mapping of vertex to index for easier reference
        vertex_to_index = {vertex: i for i, vertex in enumerate(sorted_vertices)}
        
        # Create adjacency matrix
        matrix = []
        for i, vertex in enumerate(sorted_vertices):
            row = []
            for j, other_vertex in enumerate(sorted_vertices):
                if other_vertex in self.edges.get(vertex, set()):
                    row.append("1")
                else:
                    row.append("0")
            matrix.append(row)
        
        # Print header with vertex names
        header = "    " + " ".join(f"({v},{l})" for v, l in sorted_vertices)
        result.append(header)
        result.append("    " + "-" * (len(sorted_vertices) * 12))
        
        # Print matrix rows
        for i, vertex in enumerate(sorted_vertices):
            var, line = vertex
            row_str = f"({var},{line}) |"
            for j, cell in enumerate(matrix[i]):
                row_str += f" {cell:>10}"
            result.append(row_str)
        
        result.append("")
        result.append("Legend: 1 = dependency exists, 0 = no dependency")
        result.append("        Row → Column: Row depends on Column")
        result.append("")
        
        # Add vertex information
        result.append("Vertices (Variable, Line Number):")
        result.append("-" * 35)
        for vertex in sorted_vertices:
            var, line = vertex
            dependents = self.get_dependents(vertex)
            dependencies = self.get_dependencies(vertex)
            result.append(f"({var}, {line}): deps={dependencies}, dependents={dependents}")
        
        result.append("")
        
        # Add dependency chains visualization
        result.append("Dependency Chains:")
        result.append("-" * 20)
        
        # Find root nodes (no dependencies)
        root_nodes = [v for v in sorted_vertices if not self.get_dependencies(v)]
        
        if root_nodes:
            result.append(f"Root nodes (no dependencies): {root_nodes}")
        else:
            result.append("No root nodes found (possible cycles)")
        
        # Find leaf nodes (no dependents)
        leaf_nodes = [v for v in sorted_vertices if not self.get_dependents(v)]
        if leaf_nodes:
            result.append(f"Leaf nodes (no dependents): {leaf_nodes}")
        
        result.append("")
        
        # Show some example dependency paths
        result.append("Example Dependency Paths:")
        result.append("-" * 25)
        
        # Find paths from roots to leaves
        for root in root_nodes[:3]:  # Limit to first 3 roots
            paths = self._find_paths_to_leaves(root, leaf_nodes)
            for path in paths[:2]:  # Limit to first 2 paths per root
                path_str = " → ".join(f"({v},{l})" for v, l in path)
                result.append(f"({root[0]},{root[1]}) → ... → ({path[-1][0]},{path[-1][1]}): {path_str}")
        
        result.append("")
        
        # Add variable version information
        result.append("Variable Versions:")
        result.append("-" * 18)
        for base_var in sorted(set(v for v, l in sorted_vertices)):
            versions = self.get_variable_versions(base_var)
            if len(versions) > 1:
                result.append(f"{base_var}: {versions}")
        
        return "\n".join(result)
    
    def generate_dot(self, filename="dependency_graph.dot"):
        """Generate a DOT file for Graphviz visualization."""
        dot_content = []
        dot_content.append("digraph VariableDependencyGraph {")
        dot_content.append("    rankdir=TB;")
        dot_content.append("    node [shape=box, style=filled, fontname=\"Arial\"];")
        dot_content.append("    edge [fontname=\"Arial\", fontsize=10];")
        dot_content.append("")
        
        # Group vertices by line number
        line_groups = defaultdict(list)
        for vertex in sorted(self.vertices, key=lambda x: (x[0], x[1])):
            var, line = vertex
            line_groups[line].append(vertex)
        
        # Create subgraphs for each line to ensure same rank
        for line_num in sorted(line_groups.keys()):
            dot_content.append(f"    subgraph cluster_line_{line_num} {{")
            dot_content.append(f"        rank=same;")
            dot_content.append(f"        label=\"Line {line_num}\";")
            dot_content.append(f"        style=invis;")
            dot_content.append("")
            
            # Add nodes for this line
            for vertex in line_groups[line_num]:
                var, line = vertex
                # Color nodes based on variable type
                if var in ['X', 'Y']:  # Function arguments
                    color = "lightblue"
                elif var.endswith('1'):  # SSA versions
                    color = "lightgreen"
                else:
                    color = "lightyellow"
                
                dot_content.append(f'        "{var}_{line}" [label="{var}\\n(line {line})", fillcolor="{color}"];')
            
            dot_content.append("    }")
            dot_content.append("")
        
        # Add edges with different styles for different types
        for from_vertex, to_vertices in self.edges.items():
            from_var, from_line = from_vertex
            for to_vertex in to_vertices:
                to_var, to_line = to_vertex
                
                # Different edge styles for different dependency types
                if from_var == to_var:
                    # Same variable dependency (temporal)
                    edge_style = '[color=blue, style=dashed]'
                    dot_content.append(f'    "{from_var}_{from_line}" -> "{to_var}_{to_line}" {edge_style};')
                else:
                    # Assignment dependency - source should point to target
                    # Check if this is an assignment edge
                    is_assignment = to_line in self.variable_lines and self.variable_lines.get(to_var) == to_line
                    
                    if is_assignment:
                        # For assignment: source points to target (source -> target)
                        # This shows: source -> target (assignment direction)
                        edge_style = '[color=red, style=solid]'
                        dot_content.append(f'    "{from_var}_{from_line}" -> "{to_var}_{to_line}" {edge_style};')
                    else:
                        # For other dependencies, keep normal direction
                        edge_style = '[color=red, style=solid]'
                        dot_content.append(f'    "{from_var}_{from_line}" -> "{to_var}_{to_line}" {edge_style};')
        
        dot_content.append("}")
        
        # Write to file
        with open(filename, 'w') as f:
            f.write('\n'.join(dot_content))
        
        return filename
    
    def draw_graph(self, output_format="png", filename="dependency_graph"):
        """Draw the graph using Graphviz and save as image."""
        try:
            import graphviz
            
            # Create the graph
            dot = graphviz.Digraph(comment='Variable Dependency Graph')
            dot.attr(rankdir='TB')
            dot.attr('node', shape='box', style='filled', fontname='Arial')
            dot.attr('edge', fontname='Arial', fontsize='10')
            
            # Group vertices by line number
            line_groups = defaultdict(list)
            for vertex in sorted(self.vertices, key=lambda x: (x[0], x[1])):
                var, line = vertex
                line_groups[line].append(vertex)
            
            # Create subgraphs for each line to ensure same rank
            for line_num in sorted(line_groups.keys()):
                with dot.subgraph(name=f'cluster_line_{line_num}') as subgraph:
                    subgraph.attr(rank='same')
                    subgraph.attr(label=f'Line {line_num}')
                    subgraph.attr(style='invis')  # Hide subgraph borders
                    
                    # Add nodes for this line
                    for vertex in line_groups[line_num]:
                        var, line = vertex
                        node_id = f"{var}_{line}"
                        
                        # Color nodes based on variable type
                        if var in ['X', 'Y']:  # Function arguments
                            color = "lightblue"
                        elif var.endswith('1'):  # SSA versions
                            color = "lightgreen"
                        else:
                            color = "lightyellow"
                        
                        subgraph.node(node_id, f"{var}\n(line {line})", fillcolor=color)
            
            # Add edges with different styles for different types
            for from_vertex, to_vertices in self.edges.items():
                from_var, from_line = from_vertex
                for to_vertex in to_vertices:
                    to_var, to_line = to_vertex
                    
                    # Different edge styles for different dependency types
                    if from_var == to_var:
                        # Same variable dependency (temporal) - keep top to bottom
                        dot.edge(f"{from_var}_{from_line}", f"{to_var}_{to_line}", 
                               color='blue', style='dashed')
                    else:
                        # Assignment dependency - source should point to target
                        # Check if this is an assignment edge
                        is_assignment = to_line in self.variable_lines and self.variable_lines.get(to_var) == to_line
                        
                        if is_assignment:
                            # For assignment: source points to target (source -> target)
                            # This shows: source -> target (assignment direction)
                            dot.edge(f"{from_var}_{from_line}", f"{to_var}_{to_line}", 
                                   color='red', style='solid')
                        else:
                            # For other dependencies, keep normal direction
                            dot.edge(f"{from_var}_{from_line}", f"{to_var}_{to_line}", 
                                   color='red', style='solid')
            
            # Save the graph
            output_file = f"{filename}.{output_format}"
            dot.render(filename, format=output_format, cleanup=True)
            print(f"Graph saved as: {output_file}")
            return output_file
            
        except ImportError:
            print("Graphviz not available. Generating DOT file instead.")
            return self.generate_dot(f"{filename}.dot")
    
    def _find_paths_to_leaves(self, start, leaf_nodes):
        """Find all paths from start node to any leaf node."""
        paths = []
        
        def dfs(current, path, visited):
            path.append(current)
            visited.add(current)
            
            if current in leaf_nodes:
                paths.append(path[:])
            else:
                for neighbor in self.get_dependents(current):
                    if neighbor not in visited:
                        dfs(neighbor, path, visited)
            
            path.pop()
            visited.remove(current)
        
        dfs(start, [], set())
        return paths
    
    def _get_base_variable_name(self, var_name):
        """Extract the base variable name from a versioned variable name."""
        match = re.match(r'^([a-zA-Z_][a-zA-Z0-9_]*)(\d*)$', var_name)
        if match:
            return match.group(1)
        return var_name


class DependencyVisitor(ast.NodeVisitor):
    """AST visitor to extract variable dependencies from SSA code."""
    
    def __init__(self):
        self.all_variables = set()
        self.dependencies = defaultdict(set)
        self.variable_versions = defaultdict(list)
        self.variable_lines = {}  # variable -> line number where defined
        self.variable_usage_lines = defaultdict(list)  # variable -> list of line numbers where used
        self.variable_types = {}  # variable -> inferred type
        self.current_assignment_target = None
    
    def visit_FunctionDef(self, node):
        """Visit function definition and process arguments."""
        # Add function arguments as vertices
        for arg in node.args.args:
            self.all_variables.add(arg.arg)
            self.variable_versions[arg.arg].append(arg.arg)
            # Function arguments are defined at the function definition line
            self.variable_lines[arg.arg] = node.lineno
            
            # Extract type annotation if available
            if arg.annotation:
                type_str = astor.to_source(arg.annotation).strip()
                self.variable_types[arg.arg] = self._parse_type_annotation(type_str)
        
        # Process function body
        self.generic_visit(node)
    
    def visit_Assign(self, node):
        """Visit assignment statements to extract dependencies."""
        # First, visit the value to collect all variables used
        used_variables = set()
        self._collect_variables(node.value, used_variables, node.lineno)
        
        # Then process the targets
        for target in node.targets:
            if isinstance(target, ast.Name):
                target_var = target.id
                self.all_variables.add(target_var)
                
                # Record the line where this variable is defined
                self.variable_lines[target_var] = node.lineno
                
                # Extract base variable name and version
                base_name = self._get_base_variable_name(target_var)
                self.variable_versions[base_name].append(target_var)
                
                # Infer type from the assignment
                inferred_type = self._infer_type_from_assignment(node.value)
                if inferred_type:
                    self.variable_types[target_var] = inferred_type
                
                # Add dependencies: target depends on all used variables
                for used_var in used_variables:
                    self.dependencies[target_var].add(used_var)
    
    def _parse_type_annotation(self, type_str):
        """Parse type annotation string to extract type information."""
        type_str = type_str.lower()
        
        # DataFrame types
        if 'dataframe' in type_str or 'pd.dataframe' in type_str:
            return 'DataFrame'
        elif 'series' in type_str or 'pd.series' in type_str:
            return 'Series'
        
        # Basic types
        elif 'int' in type_str:
            return 'int'
        elif 'float' in type_str:
            return 'float'
        elif 'str' in type_str or 'string' in type_str:
            return 'str'
        elif 'bool' in type_str:
            return 'bool'
        elif 'list' in type_str:
            return 'list'
        elif 'dict' in type_str:
            return 'dict'
        elif 'tuple' in type_str:
            return 'tuple'
        elif 'set' in type_str:
            return 'set'
        
        # Default to unknown
        return 'unknown'
    
    def _infer_type_from_assignment(self, value_node):
        """Infer the type of a variable from its assignment."""
        if isinstance(value_node, ast.Call):
            # Function call - check if it's a DataFrame operation
            if isinstance(value_node.func, ast.Attribute):
                # Method call like df1['activity']
                if isinstance(value_node.func.value, ast.Name):
                    var_name = value_node.func.value.id
                    if var_name in self.variable_types and self.variable_types[var_name] == 'DataFrame':
                        return 'Series'  # DataFrame column access returns Series
            elif isinstance(value_node.func, ast.Name):
                func_name = value_node.func.id
                if func_name in ['pd.DataFrame', 'DataFrame']:
                    return 'DataFrame'
                elif func_name in ['pd.Series', 'Series']:
                    return 'Series'
        
        elif isinstance(value_node, ast.Subscript):
            # Subscript operation like df1['activity']
            if isinstance(value_node.value, ast.Name):
                var_name = value_node.value.id
                if var_name in self.variable_types and self.variable_types[var_name] == 'DataFrame':
                    return 'Series'  # DataFrame column access returns Series
        
        elif isinstance(value_node, ast.Compare):
            # Comparison operation - result is boolean
            return 'bool'
        
        elif isinstance(value_node, ast.BinOp):
            # Binary operation - infer from operands
            return 'numeric'  # Usually numeric for arithmetic operations
        
        elif isinstance(value_node, ast.Constant):
            # Constant value
            if isinstance(value_node.value, str):
                return 'str'
            elif isinstance(value_node.value, (int, float)):
                return 'numeric'
            elif isinstance(value_node.value, bool):
                return 'bool'
        
        return 'unknown'
    
    def visit_Name(self, node):
        """Visit name nodes to collect variable references."""
        if isinstance(node.ctx, ast.Load):
            self.all_variables.add(node.id)
            base_name = self._get_base_variable_name(node.id)
            if node.id not in self.variable_versions[base_name]:
                self.variable_versions[base_name].append(node.id)
            
            # Record the line where this variable is used
            self.variable_usage_lines[node.id].append(node.lineno)
    
    def _collect_variables(self, node, variables, line_number):
        """Recursively collect all variable names from an AST node."""
        if isinstance(node, ast.Name):
            variables.add(node.id)
            # Record usage line number
            self.variable_usage_lines[node.id].append(line_number)
        elif isinstance(node, ast.BinOp):
            self._collect_variables(node.left, variables, line_number)
            self._collect_variables(node.right, variables, line_number)
        elif isinstance(node, ast.UnaryOp):
            self._collect_variables(node.operand, variables, line_number)
        elif isinstance(node, ast.Call):
            self._collect_variables(node.func, variables, line_number)
            for arg in node.args:
                self._collect_variables(arg, variables, line_number)
            for keyword in node.keywords:
                self._collect_variables(keyword.value, variables, line_number)
        elif isinstance(node, ast.List):
            for elt in node.elts:
                self._collect_variables(elt, variables, line_number)
        elif isinstance(node, ast.Tuple):
            for elt in node.elts:
                self._collect_variables(elt, variables, line_number)
        elif isinstance(node, ast.Subscript):
            # Handle subscript operations like df1['activity']
            self._collect_variables(node.value, variables, line_number)
            self._collect_variables(node.slice, variables, line_number)
        elif isinstance(node, ast.Attribute):
            # Handle attribute access like df1.columns
            self._collect_variables(node.value, variables, line_number)
        elif isinstance(node, ast.Compare):
            # Handle comparison operations like !=, ==
            self._collect_variables(node.left, variables, line_number)
            for comparator in node.comparators:
                self._collect_variables(comparator, variables, line_number)
        elif isinstance(node, ast.Constant):
            # Handle constants (strings, numbers, etc.)
            pass  # Constants don't contribute to variable dependencies
        elif isinstance(node, ast.Str):
            # Handle string literals (for backward compatibility)
            pass
        elif isinstance(node, ast.Num):
            # Handle numeric literals (for backward compatibility)
            pass
        elif isinstance(node, ast.Index):
            # Handle index operations in subscripts
            self._collect_variables(node.value, variables, line_number)
        elif isinstance(node, ast.Slice):
            # Handle slice operations
            if node.lower:
                self._collect_variables(node.lower, variables, line_number)
            if node.upper:
                self._collect_variables(node.upper, variables, line_number)
            if node.step:
                self._collect_variables(node.step, variables, line_number)
        else:
            # For any other node type, try to visit all children
            for child in ast.iter_child_nodes(node):
                self._collect_variables(child, variables, line_number)
    
    def _get_base_variable_name(self, var_name):
        """Extract the base variable name from a versioned variable name."""
        # Handle cases like "X1", "Y2", etc.
        match = re.match(r'^([a-zA-Z_][a-zA-Z0-9_]*)(\d*)$', var_name)
        if match:
            return match.group(1)
        return var_name


def infer_types_from_code(code: str) -> dict:
    """
    Infer argument and variable types from the function code.
    
    Args:
        code (str): Function code string
        
    Returns:
        dict: Dictionary mapping variable names to their inferred types
    """
    try:
        # Parse the code
        tree = ast.parse(code)
        
        # Find the function definition
        func_def = None
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                func_def = node
                break
        
        if not func_def:
            return {}
        
        type_info = {}
        
        # Extract argument types from type annotations
        for arg in func_def.args.args:
            if arg.annotation:
                type_str = astor.to_source(arg.annotation).strip()
                inferred_type = _parse_type_annotation(type_str)
                type_info[arg.arg] = inferred_type
        
        # Analyze function body to infer variable types from assignments
        for stmt in func_def.body:
            if isinstance(stmt, ast.Assign):
                # Get the type of the right-hand side
                rhs_type = _infer_type_from_expression(stmt.value, type_info)
                
                # Assign this type to all targets
                for target in stmt.targets:
                    if isinstance(target, ast.Name):
                        type_info[target.id] = rhs_type
                    elif isinstance(target, ast.Tuple):
                        # Handle tuple unpacking
                        if isinstance(stmt.value, ast.Tuple):
                            for i, elt in enumerate(target.elts):
                                if isinstance(elt, ast.Name) and i < len(stmt.value.elts):
                                    elt_type = _infer_type_from_expression(stmt.value.elts[i], type_info)
                                    type_info[elt.id] = elt_type
        
        return type_info
        
    except Exception as e:
        print(f"Warning: Type inference failed: {e}")
        return {}


def _parse_type_annotation(type_str: str) -> str:
    """Parse type annotation string to extract type information."""
    type_str = type_str.lower()
    
    # DataFrame types
    if 'dataframe' in type_str or 'pd.dataframe' in type_str:
        return 'DataFrame'
    elif 'series' in type_str or 'pd.series' in type_str:
        return 'Series'
    
    # Basic types
    elif 'int' in type_str:
        return 'int'
    elif 'float' in type_str:
        return 'float'
    elif 'str' in type_str or 'string' in type_str:
        return 'str'
    elif 'bool' in type_str:
        return 'bool'
    elif 'list' in type_str:
        return 'list'
    elif 'dict' in type_str:
        return 'dict'
    elif 'tuple' in type_str:
        return 'tuple'
    elif 'set' in type_str:
        return 'set'
    
    # Default to unknown
    return 'unknown'


def _infer_type_from_expression(expr, type_info: dict) -> str:
    """Infer the type of an expression based on its structure and context."""
    if isinstance(expr, ast.Call):
        # Function call
        if isinstance(expr.func, ast.Name):
            func_name = expr.func.id
            if func_name in ['pd.DataFrame', 'DataFrame']:
                return 'DataFrame'
            elif func_name in ['pd.Series', 'Series']:
                return 'Series'
        elif isinstance(expr.func, ast.Attribute):
            # Method call like df1['activity']
            if isinstance(expr.func.value, ast.Name):
                var_name = expr.func.value.id
                if var_name in type_info and type_info[var_name] == 'DataFrame':
                    return 'Series'  # DataFrame column access returns Series
    
    elif isinstance(expr, ast.Subscript):
        # Subscript operation like df1['activity']
        if isinstance(expr.value, ast.Name):
            var_name = expr.value.id
            if var_name in type_info and type_info[var_name] == 'DataFrame':
                return 'Series'  # DataFrame column access returns Series
    
    elif isinstance(expr, ast.Compare):
        # Comparison operation - result is boolean
        return 'bool'
    
    elif isinstance(expr, ast.BinOp):
        # Binary operation - infer from operands
        left_type = _infer_type_from_expression(expr.left, type_info)
        right_type = _infer_type_from_expression(expr.right, type_info)
        
        # If both operands are numeric, result is numeric
        if left_type in ['int', 'float', 'numeric'] and right_type in ['int', 'float', 'numeric']:
            if left_type == 'float' or right_type == 'float':
                return 'float'
            else:
                return 'int'
        
        # String concatenation
        if left_type == 'str' and right_type == 'str':
            return 'str'
        
        return 'unknown'
    
    elif isinstance(expr, ast.Constant):
        # Constant value
        if isinstance(expr.value, str):
            return 'str'
        elif isinstance(expr.value, (int, float)):
            return 'numeric'
        elif isinstance(expr.value, bool):
            return 'bool'
    
    elif isinstance(expr, ast.Name):
        # Variable reference - use known type if available
        return type_info.get(expr.id, 'unknown')
    
    elif isinstance(expr, ast.Attribute):
        # Attribute access like df1.columns
        if isinstance(expr.value, ast.Name):
            var_name = expr.value.id
            if var_name in type_info:
                base_type = type_info[var_name]
                if base_type == 'DataFrame':
                    if expr.attr in ['columns', 'index', 'shape']:
                        return 'list'
                    elif expr.attr in ['dtypes', 'info']:
                        return 'dict'
    
    return 'unknown'


def compile_baseline(code: str) -> dict:
    """
    Compile code in baseline mode - creates a single process_tables method.
    
    Args:
        code (str): A string containing a function definition with "#baseline" as first line
        
    Returns:
        dict: Contains the baseline operator class and related information
    """
    # Remove the "#baseline" line
    lines = code.split('\n')
    lines = lines[1:]  # Skip the first line which is "#baseline"
    
    # Separate import statements from function definition
    import_statements = []
    function_code = []
    
    for line in lines:
        stripped_line = line.strip()
        if stripped_line.startswith('import ') or stripped_line.startswith('from '):
            import_statements.append(line)
        else:
            function_code.append(line)
    
    # Reconstruct the function code without imports
    function_code_str = '\n'.join(function_code)
    
    # Parse the function to extract its definition
    try:
        tree = ast.parse(function_code_str)
        func_def = tree.body[0]
        if not isinstance(func_def, ast.FunctionDef):
            raise ValueError("Input must be a function definition")
        
        # Get function arguments with type annotations
        args_with_types = []
        for arg in func_def.args.args:
            if arg.annotation:
                type_annotation = astor.to_source(arg.annotation).strip()
                args_with_types.append(f"{arg.arg}: {type_annotation}")
            else:
                args_with_types.append(arg.arg)
        
        # Get the function body
        body_lines = []
        for stmt in func_def.body:
            if isinstance(stmt, ast.Return):
                # Convert return to yield
                return_code = astor.to_source(stmt).strip()
                if return_code == 'return' or return_code == 'return None':
                    body_lines.append('        yield None')
                else:
                    # Replace 'return' with 'yield'
                    yield_code = return_code.replace('return', 'yield', 1)
                    body_lines.append(f'        {yield_code}')
            else:
                stmt_code = astor.to_source(stmt)
                # Add proper indentation for the method body
                stmt_lines = stmt_code.split('\n')
                for stmt_line in stmt_lines:
                    if stmt_line.strip():  # Skip empty lines
                        body_lines.append(f'        {stmt_line.strip()}')
        
        # If no return statement found, add yield None
        if not any('yield' in line for line in body_lines):
            body_lines.append('        yield None')
        
        # Create the process_tables method
        args_str = ', '.join(['self'] + args_with_types)
        method_body = '\n'.join(body_lines)
        
        # Determine return type annotation
        return_type = ""
        if func_def.returns:
            return_type = f" -> {astor.to_source(func_def.returns).strip()}"
        
        process_tables_method = f"    def process_tables({args_str}){return_type}:\n{method_body}"
        
        # Create the operator class
        operator_class = "from pytexera import *\n"
        
        # Add original import statements (clean them of leading whitespace)
        if import_statements:
            clean_imports = []
            for imp in import_statements:
                clean_imports.append(imp.lstrip())
            operator_class += '\n'.join(clean_imports) + '\n'
        
        operator_class += "\nclass Operator(UDFGeneralOperator):\n"
        operator_class += process_tables_method + "\n"
        
        # Return baseline compilation result
        return {
            'ranked_cuts': [],
            'ssa_code': function_code_str,  # Original function code
            'converted_code': function_code_str,  # No conversion needed for baseline
            'process_tables': {'process_tables': process_tables_method},
            'operator_class': operator_class,
            'num_args': len(func_def.args.args),
            'cuts_used': [],
            'filtered_cuts': [],
            'import_statements': import_statements,
            'cleaned_code': function_code_str,
            'baseline_mode': True
        }
        
    except Exception as e:
        raise ValueError(f"Failed to compile in baseline mode: {e}")


def compile(code: str, line_number=None):
    """
    Apply SSA transformation, build dependency graph, and find valid cut points.
    Rank cuts by variable size and convert SSA variables to self.<var> format.
    Generate process tables for each argument and split code into sub-functions.
    
    Args:
        code (str): A string containing a function definition (may include import statements)
        line_number (int, optional): Specific line number to split at. If None, uses best cut.
        
    Returns:
        dict: Contains ranked cuts, SSA code, converted code with self.<var>, 
              process tables, and sub-functions
    """
    # Check if the first line is "#baseline" for baseline compilation
    lines = code.split('\n')
    first_line = lines[0].strip() if lines else ""
    
    if first_line == "#baseline":
        # Handle baseline compilation
        return compile_baseline(code)
    
    # Step 0: Separate import statements from function definition
    import_statements = []
    function_code = []
    
    for line in lines:
        stripped_line = line.strip()
        if stripped_line.startswith('import ') or stripped_line.startswith('from '):
            import_statements.append(line)
        else:
            function_code.append(line)
    
    # Reconstruct the function code without imports
    function_code_str = '\n'.join(function_code)
    
    # Step 0.5: Clean the function code by removing empty lines and comments
    cleaned_function_code = preprocess_code(function_code_str)
    print(f"DEBUG: Original function code length: {len(function_code_str)}")
    print(f"DEBUG: Cleaned function code length: {len(cleaned_function_code)}")
    
    # Step 1: Infer types from the cleaned code
    type_info = infer_types_from_code(cleaned_function_code)
    print(f"DEBUG: Inferred types: {type_info}")
    
    # Step 2: Apply SSA transformation to the cleaned code
    ssa_code = SSA(cleaned_function_code)
    
    # Step 3: Convert SSA variables to self.<var> format
    def convert_ssa_to_self(ssa_code, type_info=None):
        # Parse the function to get input arguments (use cleaned code)
        tree = ast.parse(cleaned_function_code)
        func_def = tree.body[0]
        if isinstance(func_def, ast.FunctionDef):
            input_args = [arg.arg for arg in func_def.args.args]
        else:
            input_args = []
        
        # Parse the SSA code to extract local variables from it
        ssa_tree = ast.parse(ssa_code)
        ssa_func_def = ssa_tree.body[0]
        
        # Extract local variables from the SSA function body
        local_vars = set()
        for stmt in ssa_func_def.body:
            if isinstance(stmt, ast.Assign):
                for target in stmt.targets:
                    if isinstance(target, ast.Name):
                        local_vars.add(target.id)
                    elif isinstance(target, ast.Tuple):
                        for elt in target.elts:
                            if isinstance(elt, ast.Name):
                                local_vars.add(elt.id)
            elif isinstance(stmt, ast.AugAssign):
                if isinstance(stmt.target, ast.Name):
                    local_vars.add(stmt.target.id)
        
        # Debug print: Show all local variables found
        print(f"DEBUG: Local variables extracted from SSA code: {sorted(local_vars)}")
        print(f"DEBUG: Input arguments: {input_args}")
        if type_info:
            print(f"DEBUG: Type information: {type_info}")
        
        # Create AST transformer to add self. prefix to local variables
        class SelfPrefixTransformer(ast.NodeTransformer):
            def __init__(self, local_vars, input_args, type_info=None):
                self.local_vars = local_vars
                self.input_args = input_args
                self.type_info = type_info or {}
                self.transformed_vars = set()  # Track which variables were transformed
            
            def visit_Name(self, node):
                var_name = node.id
                
                # Skip if it's already prefixed with self.
                if var_name.startswith('self.'):
                    return node
                
                # Skip if it's a function argument
                if var_name in self.input_args:
                    return node
                
                # Skip if it's a built-in name
                if var_name in __builtins__:
                    return node
                
                # Skip if it's a keyword
                keywords = {'True', 'False', 'None', 'self', 'yield', 'return', 'import', 'from', 'as'}
                if var_name in keywords:
                    return node
                
                # Add self. prefix to local variables in both Load and Store contexts
                if var_name in self.local_vars:
                    self.transformed_vars.add(var_name)
                    var_type = self.type_info.get(var_name, 'unknown')
                    print(f"DEBUG: Transforming variable '{var_name}' (type: {var_type}) to 'self.{var_name}' ({type(node.ctx).__name__} context)")
                    return ast.Attribute(
                        value=ast.Name(id='self', ctx=ast.Load()),
                        attr=var_name,
                        ctx=node.ctx
                    )
                else:
                    print(f"DEBUG: Variable '{var_name}' not in local_vars, context: {type(node.ctx).__name__}")
                
                return node
        
        # Apply the transformer to the SSA code
        try:
            transformer = SelfPrefixTransformer(local_vars, input_args, type_info)
            modified_tree = transformer.visit(ssa_tree)
            
            # Debug print: Show which variables were actually transformed
            print(f"DEBUG: Variables actually transformed: {sorted(transformer.transformed_vars)}")
            
            # Convert back to source code
            converted_code = astor.to_source(modified_tree)
            return converted_code
            
        except Exception as e:
            # If AST transformation fails, fall back to the original SSA code
            print(f"Warning: AST transformation failed: {e}. Using original SSA code.")
            return ssa_code
    
    converted_code = convert_ssa_to_self(ssa_code, type_info)
    
    # Step 4: Build variable dependency graph with type information (using cleaned code)
    graph = VariableDependencyGraph(ssa_code)
    
    # Update the graph's variable types with our inferred types
    if type_info:
        for var_name, var_type in type_info.items():
            # Find all versions of this variable in the graph
            for vertex in graph.vertices:
                vertex_var, vertex_line = vertex
                if graph._get_base_variable_name(vertex_var) == var_name:
                    graph.variable_types[vertex_var] = var_type
    
    # Step 5: Find valid cut points
    valid_cuts = graph.find_valid_cuts()
    
    # Step 6: Rank cuts by variable size
    ranked_cuts = graph.rank_cuts_by_variable_size(valid_cuts)
    
    # Step 7: If specific line number is provided, filter cuts to use that line
    if line_number is not None:
        # Find the cut at the specified line number
        specified_cut = None
        for cut in ranked_cuts:
            if cut['line_number'] == line_number:
                specified_cut = cut
                break
        
        if specified_cut:
            # Use only the specified cut
            ranked_cuts = [specified_cut]
        else:
            # If the specified line is not a valid cut, use the best available cut
            print(f"Warning: Line {line_number} is not a valid cut point. Using best available cut.")
    
    # Step 8: Generate process tables and split code (using cleaned code)
    split_result = generate_process_tables_and_split(converted_code, ranked_cuts, cleaned_function_code, type_info)
    
    # Step 9: Add import statements to the operator class
    operator_class_with_imports = ""
    if import_statements:
        # Ensure import statements are not indented
        clean_imports = []
        for imp in import_statements:
            # Remove any leading whitespace from import statements
            clean_imports.append(imp.lstrip())
        operator_class_with_imports = '\n'.join(clean_imports) + '\n\n'
    operator_class_with_imports += split_result['operator_class']
    
    return {
        'ranked_cuts': ranked_cuts,
        'ssa_code': ssa_code,
        'converted_code': converted_code,
        'process_tables': split_result['process_tables'],
        'operator_class': operator_class_with_imports,
        'num_args': split_result['num_args'],
        'cuts_used': split_result['cuts_used'],
        'filtered_cuts': split_result['filtered_cuts'],
        'import_statements': import_statements,
        'cleaned_code': cleaned_function_code
    }


def apply_loop_transformation_to_process_table(process_table_code: str, table_name: str) -> str:
    """
    Apply loop transformation to a process table method if it contains loop patterns.
    
    Args:
        process_table_code (str): The process table method code
        table_name (str): Name of the process table (e.g., 'process_table_0')
        
    Returns:
        str: Transformed process table code or original if no transformation needed
    """
    try:
        # Extract the function body to check for loop patterns
        # Find the method body by locating the first ':' and extracting everything after it
        colon_pos = process_table_code.find(':')
        if colon_pos == -1:
            return process_table_code  # No method signature found
        
        method_body = process_table_code[colon_pos + 1:]
        lines = method_body.split('\n')
        
        # Filter out empty lines and lines that are clearly part of method signature
        body_lines = []
        for line in lines:
            stripped = line.strip()
            if stripped and not stripped.endswith('):') and not stripped.startswith('def '):
                body_lines.append(line)
        
        if not body_lines:
            return process_table_code  # No meaningful body found
        
        # Fix indentation for the temporary function
        fixed_lines = []
        
        # Find the base indentation (minimum non-zero indentation)
        base_indent = None
        for line in body_lines:
            if line.strip():
                current_indent = len(line) - len(line.lstrip())
                if current_indent > 0 and (base_indent is None or current_indent < base_indent):
                    base_indent = current_indent
        
        if base_indent is None:
            base_indent = 8  # Default if no indentation found
        
        # Normalize indentation for function body
        for line in body_lines:
            if line.strip():
                current_indent = len(line) - len(line.lstrip())
                relative_indent = max(0, current_indent - base_indent)
                fixed_lines.append('    ' + ' ' * relative_indent + line.lstrip())
            else:
                fixed_lines.append('')
        
        temp_func_code = f"def temp_func():\n" + '\n'.join(fixed_lines)
        
        # Debug output (can be removed for production)
        # print(f"  Debug: Temp function code for {table_name}:")
        # print(f"  {temp_func_code}")
        
        # Apply loop transformation to the temporary function
        try:
            # Try different import paths since this might be called from different contexts
            try:
                from loop_transformer import LoopTransformer
            except ImportError:
                from src.loop_transformer import LoopTransformer
            
            transformer = LoopTransformer()
            transformed_temp = transformer.transform_function(temp_func_code)
            
            # Check if transformation actually occurred (if code changed)
            if transformed_temp.strip() == temp_func_code.strip():
                # No transformation needed, return original
                return process_table_code
            
            print(f"  Applying loop transformation to {table_name}...")
            
        except ImportError as e:
            # Fallback if loop_transformer is not available
            print(f"  Warning: LoopTransformer not available ({e}), using original code")
            return process_table_code
        
        # Extract the transformed body and reformat back to process table method
        transformed_tree = ast.parse(transformed_temp)
        if transformed_tree.body and isinstance(transformed_tree.body[0], ast.FunctionDef):
            transformed_func = transformed_tree.body[0]
            
            # Get the original method signature
            method_signature = process_table_code[:process_table_code.find(':') + 1]
            
            # Reconstruct the transformed method body with proper indentation
            transformed_body_lines = []
            for stmt in transformed_func.body:
                stmt_code = astor.to_source(stmt)
                # Handle multi-line statements properly while preserving relative indentation
                lines = stmt_code.split('\n')
                
                # Find the base indentation from the first non-empty line
                base_indent = 0
                for line in lines:
                    if line.strip():
                        base_indent = len(line) - len(line.lstrip())
                        break
                
                # Add each line with proper method body indentation (8 spaces) + relative indent
                for line in lines:
                    if line.strip():  # Skip empty lines
                        current_indent = len(line) - len(line.lstrip())
                        relative_indent = max(0, current_indent - base_indent)
                        transformed_body_lines.append(f"        {' ' * relative_indent}{line.strip()}")
            
            transformed_process_table = method_signature + "\n" + "\n".join(transformed_body_lines)
            
            print(f"  Loop transformation applied successfully to {table_name}")
            return transformed_process_table
        
    except Exception as e:
        print(f"  Warning: Loop transformation failed for {table_name}: {e}")
        print(f"  Using original code.")
    
    return process_table_code


def generate_process_tables_and_split(converted_code: str, ranked_cuts: list, original_code: str, type_info: dict = None) -> dict:
    """
    Generate process tables for each argument, where each process table contains
    a part of the converted code split by N-1 cuts into N parts.
    Wraps all process tables in an Operator class.
    
    Args:
        converted_code (str): The converted code with self.<var> format
        ranked_cuts (list): List of ranked cut points
        original_code (str): The original code
        type_info (dict): Dictionary mapping variable names to their types
        
    Returns:
        dict: Contains process tables with code parts wrapped in Operator class
    """
    def analyze_used_arguments(code_lines, all_args):
        """Analyze which function arguments are actually used in the given code lines."""
        used_args = set()
        
        # Join all lines and look for argument usage
        full_code = ' '.join(code_lines)
        
        for arg in all_args:
            # Look for the argument name in the code (with word boundaries to avoid partial matches)
            import re
            # Use word boundaries to ensure we match the exact argument name
            # and not parts of other variable names
            pattern = r'\b' + re.escape(arg) + r'\b'
            if re.search(pattern, full_code):
                used_args.add(arg)
        
        return list(used_args)
    
    def get_type_annotation(var_name: str) -> str:
        """Get the type annotation for a variable."""
        if not type_info:
            return ""
        
        var_type = type_info.get(var_name, 'unknown')
        if var_type == 'DataFrame':
            return ': pd.DataFrame'
        elif var_type == 'Series':
            return ': pd.Series'
        elif var_type == 'int':
            return ': int'
        elif var_type == 'float':
            return ': float'
        elif var_type == 'str':
            return ': str'
        elif var_type == 'bool':
            return ': bool'
        elif var_type == 'list':
            return ': list'
        elif var_type == 'dict':
            return ': dict'
        elif var_type == 'tuple':
            return ': tuple'
        elif var_type == 'set':
            return ': set'
        else:
            return ""
    
    def convert_return_to_yield(code_lines):
        """Convert return statements to yield statements and add yield None if no return."""
        processed_lines = []
        has_return = False
        
        for line in code_lines:
            stripped_line = line.strip()
            if stripped_line.startswith('return'):
                # Convert return to yield
                if stripped_line == 'return' or stripped_line == 'return None':
                    processed_lines.append(line.replace('return', 'yield None'))
                else:
                    # Extract the return value and convert to yield
                    return_value = line.replace('return', '').strip()
                    processed_lines.append(line.replace('return', 'yield'))
                has_return = True
            else:
                processed_lines.append(line)
        
        # If no return statement found, add yield None at the end
        if not has_return:
            # Preserve the indentation of the last line or use default indentation
            if processed_lines:
                last_line = processed_lines[-1]
                # Find the indentation of the last line
                indentation = len(last_line) - len(last_line.lstrip())
                processed_lines.append(' ' * indentation + 'yield None')
            else:
                processed_lines.append('        yield None')
        
        return processed_lines
    
    def add_type_annotations_to_lines(code_lines):
        """Add type annotations to local variable assignments."""
        if not type_info:
            return code_lines
        
        processed_lines = []
        for line in code_lines:
            # Look for assignment patterns like "self.var = ..."
            import re
            assignment_pattern = r'^(\s*)(self\.[a-zA-Z_][a-zA-Z0-9_]*)\s*='
            match = re.match(assignment_pattern, line)
            
            if match:
                indent, var_name = match.groups()
                # Extract the base variable name (remove 'self.')
                base_var_name = var_name.replace('self.', '')
                var_type = type_info.get(base_var_name, 'unknown')
                
                if var_type != 'unknown':
                    type_annotation = get_type_annotation(base_var_name)
                    # Add type annotation to the variable
                    annotated_line = f"{indent}{var_name}{type_annotation} ="
                    # Add the rest of the line after the assignment
                    rest_of_line = line[match.end():]
                    annotated_line += rest_of_line
                    processed_lines.append(annotated_line)
                else:
                    processed_lines.append(line)
            else:
                processed_lines.append(line)
        
        return processed_lines
    
    # Parse the function to get input arguments
    tree = ast.parse(converted_code)
    func_def = tree.body[0]
    if not isinstance(func_def, ast.FunctionDef):
        raise ValueError("Input must be a function definition")
    
    input_args = [arg.arg for arg in func_def.args.args]
    num_args = len(input_args)
    
    # Extract function body lines from converted code (which already has self. prefixes)
    converted_body_lines = []
    
    for stmt in func_def.body:
        if isinstance(stmt, ast.Return):
            return_line = astor.to_source(stmt).strip()
            # Include return statement in body lines
            converted_body_lines.append(return_line)
        else:
            # Preserve indentation for multi-line statements
            stmt_code = astor.to_source(stmt)
            # Split into lines and add each line separately to preserve indentation
            lines = stmt_code.split('\n')
            for line in lines:
                if line.strip():  # Skip empty lines
                    converted_body_lines.append(line.rstrip())  # Remove trailing whitespace but keep leading
    
    # Filter out invalid cuts: only allow cuts where 2 < line_number < len(body_lines) + 1
    valid_cut_min = 3
    valid_cut_max = len(converted_body_lines)
    filtered_cuts = [cut for cut in ranked_cuts if valid_cut_min <= cut['line_number'] <= valid_cut_max]
    ranked_cuts = filtered_cuts

    # Generate process tables with code parts
    process_tables = {}
    
    if num_args <= 1:
        # For 0 or 1 arguments, create process tables with the full code
        for i, arg in enumerate(input_args, 0):
            arg_type = get_type_annotation(arg)
            
            if converted_body_lines:
                # Create temporary process table for loop transformation
                temp_lines = add_type_annotations_to_lines(converted_body_lines)
                temp_process_table_body = '\n'.join(f'        {line}' for line in temp_lines)
                
                # Analyze which arguments are actually used
                used_args = analyze_used_arguments(temp_lines, input_args)
                
                # Create method signature with type annotations
                if used_args:
                    args_with_types = []
                    for used_arg in used_args:
                        used_arg_type = get_type_annotation(used_arg)
                        args_with_types.append(f'{used_arg}{used_arg_type}')
                    args_str = ', '.join(['self'] + args_with_types)
                else:
                    args_str = 'self'
                
                temp_process_table_code = f'    def process_table_{i}({args_str}):\n{temp_process_table_body}'
                
                # Apply loop transformation FIRST (before converting returns to yields)
                table_name = f'process_table_{i}'
                transformed_process_table_code = apply_loop_transformation_to_process_table(temp_process_table_code, table_name)
                
                # Then convert any remaining return statements to yield statements
                # Extract body lines from the transformed code and apply return-to-yield conversion
                transformed_lines = transformed_process_table_code.split('\n')
                body_start = next(i for i, line in enumerate(transformed_lines) if ':' in line) + 1
                body_lines = [line[8:] if line.startswith('        ') else line.strip() for line in transformed_lines[body_start:] if line.strip()]
                
                if body_lines:
                    processed_lines = convert_return_to_yield(body_lines)
                    process_table_body = '\n'.join(f'        {line}' for line in processed_lines)
                    transformed_process_table_code = f'    def process_table_{i}({args_str}):\n{process_table_body}'
            else:
                process_table_code = f'    def process_table_{i}(self, {arg}{arg_type}):\n        yield None'
                transformed_process_table_code = process_table_code
            
            process_tables[f'process_table_{i}'] = transformed_process_table_code
    else:
        # Use the best N-1 valid cuts to split the code into N parts
        best_cuts = filtered_cuts[:num_args - 1]
        sorted_cuts = sorted(best_cuts, key=lambda x: x['line_number'])
        
        # Split the code into parts
        start_line = 0
        for i in range(num_args):
            if i < len(sorted_cuts):
                # Use the cut point to determine the end of this part
                # To cut before line N, use converted_body_lines[0:N-2] for the first part
                cut_line = sorted_cuts[i]['line_number'] - 2
                part_lines = converted_body_lines[start_line:cut_line]
                start_line = cut_line
            else:
                # This is the last part (from last cut to end)
                part_lines = converted_body_lines[start_line:]
            
            # Create process table with this part of the code
            arg_name = input_args[i]
            
            if part_lines:
                # Add type annotations to local variables first
                temp_lines = add_type_annotations_to_lines(part_lines)
                temp_process_table_body = '\n'.join(f'        {line}' for line in temp_lines)
                
                # Analyze which arguments are actually used in this process table's body
                used_args = analyze_used_arguments(temp_lines, input_args)
                
                # Create method signature with type annotations
                if used_args:
                    args_with_types = []
                    for arg in used_args:
                        arg_type = get_type_annotation(arg)
                        args_with_types.append(f'{arg}{arg_type}')
                    args_str = ', '.join(['self'] + args_with_types)
                else:
                    args_str = 'self'
                
                temp_process_table_code = f'    def process_table_{i}({args_str}):\n{temp_process_table_body}'
                
                # Apply loop transformation FIRST (before converting returns to yields)
                table_name = f'process_table_{i}'
                transformed_process_table_code = apply_loop_transformation_to_process_table(temp_process_table_code, table_name)
                
                # Then convert any remaining return statements to yield statements
                # Extract body lines from the transformed code and apply return-to-yield conversion
                transformed_lines = transformed_process_table_code.split('\n')
                body_start = next(i for i, line in enumerate(transformed_lines) if ':' in line) + 1
                body_lines = [line[8:] if line.startswith('        ') else line.strip() for line in transformed_lines[body_start:] if line.strip()]
                
                if body_lines:
                    processed_lines = convert_return_to_yield(body_lines)
                    process_table_body = '\n'.join(f'        {line}' for line in processed_lines)
                    transformed_process_table_code = f'    def process_table_{i}({args_str}):\n{process_table_body}'
            else:
                arg_type = get_type_annotation(arg_name)
                temp_process_table_code = f'    def process_table_{i}(self, {arg_name}{arg_type}):\n        yield None'
                
                # Apply loop transformation even for empty process tables
                table_name = f'process_table_{i}'
                transformed_process_table_code = apply_loop_transformation_to_process_table(temp_process_table_code, table_name)
            
            process_tables[f'process_table_{i}'] = transformed_process_table_code
    
    # Wrap all process tables in an Operator class
    operator_class_code = "from pytexera import *\nclass Operator(UDFGeneralOperator):\n"
    for table_name, table_code in process_tables.items():
        # The process table code already has proper indentation, just add it as is
        operator_class_code += table_code + "\n"
    
    return {
        'process_tables': process_tables,
        'operator_class': operator_class_code,
        'num_args': num_args,
        'cuts_used': best_cuts if num_args > 1 else [],
        'filtered_cuts': filtered_cuts
    }


# Example usage and testing
if __name__ == "__main__":
    # Test the SSA function
#     test_code = """
# def foo(X, Y, Z):
#     X = X  + 1
#     X = X * 2
#     X = X + Y
#     Y = X - 1
#     Y = X - Y
#     k = X - Y
#     k += 1
#     k &= (k + Z)
#     Z += 1
#     return k
#     """
#     test_code = """
# def merge_and_aggregate(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
#     # Step 1: Filter df1: Keep only rows where activity is meaningful
#     filtered = df1[df1['activity'] != 'idle']

#     # Step 2 (later): Use df2 to keep only users who are 'active' in user info
#     active_users = df2[df2['status'] == 'active']['user_id']
#     result = filtered[filtered['user_id'].isin(active_users)]

#     return result
#     """

#     test_code = """
# from pytexera import *
# import pandas as pd
# import numpy as np

# def compare_texts_vectorized(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
#     s1_full = df1['text'].fillna("").str.strip().str.lower()
#     n = min(len(s1_full), len(df2))
#     s1 = s1_full.head(n)
#     s2 = df2['text'].head(n).fillna("").str.strip().str.lower()
#     exact_mask = s1.eq(s2)
#     partial_mask = s1.str.contains(s2, regex=False) | s2.str.contains(s1, regex=False)
#     match_type = np.select([exact_mask, partial_mask], ['exact', 'partial'], default='none')
#     return pd.DataFrame({'df1_text': s1, 'df2_text': s2, 'match_type': match_type})
#     """
    test_code = """
from pytexera import *
import pandas as pd
import numpy as np

def compare_texts_vectorized(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    s1_full = df1['text'].fillna("").str.strip().str.lower()
    n = min(len(s1_full), len(df2))
    s1 = s1_full.head(n)
    s2 = df2['text'].head(n).fillna("").str.strip().str.lower()
    exact_mask = s1.eq(s2)
    partial_mask = s1.str.contains(s2, regex=False) | s2.str.contains(s1, regex=False)
    match_type = np.select([exact_mask, partial_mask], ['exact', 'partial'], default='none')
    result = []
    for t in df2:
        result.append({'df1_text': t['text'], 'df2_text': t['text'], 'match_type': match_type[i]})
    return result
    """
    print("Original code:")
    print(test_code)
    print("\nSSA format:")
    ssa_code = SSA(test_code)
    print(ssa_code)
    
    print("\n" + "="*50)
    print("Variable Dependency Graph Analysis:")
    print("="*50)
    
    # Create dependency graph
    graph = VariableDependencyGraph(ssa_code)
    print(graph.visualize())

    
    print("\n" + "="*50)
    print("Drawing Visual Graph:")
    print("="*50)
    
    # Draw the graph and generate PNG
    try:
        graph_file = graph.draw_graph(output_format="png", filename="dependency_graph")
        print(f"Graph visualization saved as: {graph_file}")
        
        # Also generate DOT file for reference
        dot_file = graph.generate_dot("dependency_graph.dot")
        print(f"DOT file also generated: {dot_file}")
        
    except Exception as e:
        print(f"Could not draw graph: {e}")
        print("Generating DOT file instead...")
        dot_file = graph.generate_dot()
        print(f"DOT file generated: {dot_file}")
        print("You can visualize it using Graphviz: dot -Tpng dependency_graph.dot -o graph.png")
    
    print("\n" + "="*50)
    print("Testing Compile Function:")
    print("="*50)
    
    # Test the compile function
    try:
        result = compile(test_code)
        ranked_cuts = result['ranked_cuts']
        ssa_code = result['ssa_code']
        converted_code = result['converted_code']
        process_tables = result['process_tables']
        operator_class = result['operator_class']
        num_args = result['num_args']
        cuts_used = result['cuts_used']
        filtered_cuts = result['filtered_cuts']
        
        print(f"Found {len(ranked_cuts)} valid cut point(s), ranked by variable size:")
        for i, cut in enumerate(ranked_cuts, 1):
            print(f"  {i}. {cut['description']}")
            print(f"     Line number: {cut['line_number']}")
            print(f"     Cut variables: {cut['cut_variables']}")
            print(f"     Total variable size: {cut['total_variable_size']} bytes")
            print(f"     Average variable size: {cut['average_variable_size']:.1f} bytes")
            print(f"     Rank score: {cut['rank_score']} (lower is better)")
            print(f"     Crossing edges: {cut['crossing_edges']}")
            print()
        
        print("\n" + "="*50)
        print("Generated SSA Code:")
        print("="*50)
        
        print(ssa_code)
        
        print("\n" + "="*50)
        print("Converted Code:")
        print("="*50)
        
        print(converted_code)
        
        print("\n" + "="*50)
        print("Process Tables:")
        print("="*50)
        print(f"Number of arguments: {num_args}")
        print("\nOperator Class:")
        print(operator_class)
        if cuts_used:
            print(f"Using {len(cuts_used)} cuts for splitting:")
            for i, cut in enumerate(cuts_used):
                print(f"  Cut {i+1}: Line {cut['line_number']} - {cut['description']}")
            print()
        print("\n" + "="*50)
        print("Testing Different Cut Line Numbers:")
        print("="*50)
        # Test different cut line numbers
        test_cut_lines = [2, 3, 4, 5]
        # Use filtered_cuts for valid cut lines
        valid_cut_lines = set(cut['line_number'] for cut in filtered_cuts)
        for cut_line in test_cut_lines:
            if cut_line not in valid_cut_lines:
                print(f"\n--- Testing cut at line {cut_line} ---")
                print(f"Cut at line {cut_line} is invalid and skipped.")
                continue
            print(f"\n--- Testing cut at line {cut_line} ---")
            try:
                result_with_cut = compile(test_code, line_number=cut_line)
                ranked_cuts = result_with_cut['ranked_cuts']
                process_tables = result_with_cut['process_tables']
                operator_class = result_with_cut['operator_class']
                print(f"Cut at line {cut_line} results:")
                if ranked_cuts:
                    best_cut = ranked_cuts[0]
                    print(f"  Best cut: Line {best_cut['line_number']}")
                    print(f"  Cut variables: {best_cut['cut_variables']}")
                    print(f"  Rank score: {best_cut['rank_score']}")
                    if 'heuristic_bonus' in best_cut:
                        print(f"  Heuristic bonus: {best_cut['heuristic_bonus']}")
                print(f"  Process tables generated: {len(process_tables)}")
                print("  Generated code:\n")
                print(f"{operator_class}")
            except Exception as e:
                print(f"  Error with cut at line {cut_line}: {e}")
    except Exception as e:
        print(f"Error in compile function: {e}")
    
    print("\n" + "="*50)
    print("Testing Baseline Compilation:")
    print("="*50)
    
    # Test baseline compilation
    baseline_test_code = '''#baseline
import pandas as pd
import numpy as np

def compare_texts_vectorized(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    s1_full = df1['text'].fillna("").str.strip().str.lower()
    n = min(len(s1_full), len(df2))
    s1 = s1_full.head(n)
    s2 = df2['text'].head(n).fillna("").str.strip().str.lower()
    exact_mask = s1.eq(s2)
    partial_mask = s1.str.contains(s2, regex=False) | s2.str.contains(s1, regex=False)
    match_type = np.select([exact_mask, partial_mask], ['exact', 'partial'], default='none')
    return pd.DataFrame({'df1_text': s1, 'df2_text': s2, 'match_type': match_type})
'''
    
    try:
        print("Original baseline code:")
        print(baseline_test_code)
        
        print("\nCompiling baseline code...")
        baseline_result = compile(baseline_test_code)
        
        print("\nBaseline compilation result:")
        print(f"Baseline mode: {baseline_result.get('baseline_mode', False)}")
        print(f"Number of arguments: {baseline_result['num_args']}")
        print(f"Operator class:")
        print(baseline_result['operator_class'])
        
    except Exception as e:
        print(f"Error in baseline compilation: {e}")
        import traceback
        traceback.print_exc()
 