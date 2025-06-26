import ast
import astor
from collections import defaultdict, deque
import re


def preprocess_code(code_snippet: str) -> str:
    """
    Preprocess code by removing empty lines and comments.
    
    Args:
        code_snippet (str): A string containing source code
        
    Returns:
        str: Cleaned code without empty lines and comments
    """
    lines = code_snippet.split('\n')
    cleaned_lines = []
    
    for line in lines:
        # Remove leading/trailing whitespace
        stripped_line = line.strip()
        
        # Skip empty lines
        if not stripped_line:
            continue
            
        # Skip comment-only lines (lines that start with #)
        if stripped_line.startswith('#'):
            continue
            
        # Remove inline comments (everything after #)
        if '#' in stripped_line:
            # Find the position of the first #
            comment_pos = stripped_line.find('#')
            # Keep only the part before the comment
            stripped_line = stripped_line[:comment_pos].rstrip()
            # Skip if the line becomes empty after removing comment
            if not stripped_line:
                continue
        
        # Add the cleaned line
        cleaned_lines.append(line)
    
    return '\n'.join(cleaned_lines)


def SSA(code_snippet: str) -> str:
    """
    Convert a function definition to Static Single Assignment (SSA) format.
    
    Args:
        code_snippet (str): A string containing a function definition
        
    Returns:
        str: The function converted to SSA format
    """
    try:
        # Preprocess the code to remove empty lines and comments
        cleaned_code = preprocess_code(code_snippet)
        
        # Parse the code into an AST
        tree = ast.parse(cleaned_code)
        
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


def Split(code: str, line_number=None):
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
    # Step 0: Separate import statements from function definition
    import_statements = []
    function_code = []
    
    lines = code.split('\n')
    for line in lines:
        stripped_line = line.strip()
        if stripped_line.startswith('import ') or stripped_line.startswith('from '):
            import_statements.append(line)
        else:
            function_code.append(line)
    
    # Reconstruct the function code without imports
    function_code_str = '\n'.join(function_code)
    
    # Step 1: Apply SSA transformation
    ssa_code = SSA(function_code_str)
    
    # Step 2: Convert SSA variables to self.<var> format
    def convert_ssa_to_self(ssa_code):
        # Parse the function to get input arguments (use cleaned code)
        cleaned_code = preprocess_code(function_code_str)
        tree = ast.parse(cleaned_code)
        func_def = tree.body[0]
        if isinstance(func_def, ast.FunctionDef):
            input_args = [arg.arg for arg in func_def.args.args]
        else:
            input_args = []  # No default fallback needed
        
        def replace_ssa_vars(line):
            def repl(match):
                var = match.group(0)
                for arg in input_args:
                    if var.startswith(arg) and var != arg:
                        return f"self.{arg}"
                return var
            # Replace X1, X2, ..., Y1, Y2, Z1, Z2, ... but not X, Y, Z themselves
            if input_args:
                pattern = r'\b(' + '|'.join([f'{arg}\\d+' for arg in input_args]) + r')\b'
                return re.sub(pattern, repl, line)
            else:
                return line
        
        # Convert each line
        lines = ssa_code.split('\n')
        converted_lines = []
        for line in lines:
            if line.strip() and not line.strip().startswith('def'):
                converted_line = replace_ssa_vars(line)
                
                # Post-process to add parentheses where needed for precedence
                # Fix cases like 'k1 & k1 + Z' to 'k1 & (k1 + Z)'
                if '&' in converted_line and '+' in converted_line:
                    # Look for patterns like 'var & var + var'
                    pattern = r'(\w+)\s*&\s*(\w+)\s*\+\s*(\w+)'
                    match = re.search(pattern, converted_line)
                    if match:
                        var1, var2, var3 = match.groups()
                        if var1 == var2:  # Same variable on both sides of &
                            # Replace with parentheses around the addition
                            replacement = f"{var1} & ({var2} + {var3})"
                            converted_line = re.sub(pattern, replacement, converted_line)
                
                converted_lines.append(converted_line)
            else:
                converted_lines.append(line)
        
        return '\n'.join(converted_lines)
    
    converted_code = convert_ssa_to_self(ssa_code)
    
    # Step 3: Build variable dependency graph
    graph = VariableDependencyGraph(ssa_code)
    
    # Step 4: Find valid cut points
    valid_cuts = graph.find_valid_cuts()
    
    # Step 5: Rank cuts by variable size
    ranked_cuts = graph.rank_cuts_by_variable_size(valid_cuts)
    
    # Step 6: If specific line number is provided, filter cuts to use that line
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
    
    # Step 7: Generate process tables and split code
    split_result = generate_process_tables_and_split(converted_code, ranked_cuts, function_code_str)
    
    # Step 8: Add import statements to the operator class
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
        'import_statements': import_statements
    }


def generate_process_tables_and_split(converted_code: str, ranked_cuts: list, original_code: str) -> dict:
    """
    Generate process tables for each argument, where each process table contains
    a part of the converted code split by N-1 cuts into N parts.
    Wraps all process tables in an Operator class.
    
    Args:
        converted_code (str): The converted code with self.<var> format
        ranked_cuts (list): List of ranked cut points
        original_code (str): The original code
        
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
    
    def extract_local_variables(body_lines):
        """Extract all local variables defined in the function body."""
        local_vars = set()
        
        for line in body_lines:
            # Look for assignment patterns like "var = ..."
            import re
            # Pattern to match variable assignments
            assignment_pattern = r'^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*='
            match = re.match(assignment_pattern, line)
            if match:
                var_name = match.group(1)
                local_vars.add(var_name)
        
        return local_vars
    
    def add_self_to_local_vars(code_lines, arg_name, local_vars, input_args):
        """Add self. prefix to local variables that are not the current argument."""
        processed_lines = []
        
        # Keywords and operators that should not be prefixed
        keywords = {'def', 'self', 'if', 'else', 'for', 'while', 'return', 'pass', 'True', 'False', 'None', 'in', 'is', 'not', 'and', 'or', 'yield', 'import', 'from', 'as'}
        operators = {'!=', '==', '>=', '<=', '>', '<', '=', '+', '-', '*', '/', '&', '|', '^', '(', ')', '[', ']', '{', '}', ',', '.', ':', ';'}
        
        for line in code_lines:
            # Skip import statements entirely
            stripped_line = line.strip()
            if stripped_line.startswith('import ') or stripped_line.startswith('from '):
                processed_lines.append(line)
                continue
            
            # Use regex to find variable names and replace them with self. prefix
            # But exclude keywords, operators, string literals, and method calls
            import re
            
            def replace_var(match):
                var_name = match.group(0)
                
                # Skip if it's already prefixed with self.
                if var_name.startswith('self.'):
                    return var_name
                
                # Skip if it's the current argument
                if var_name == arg_name:
                    return var_name
                
                # Skip if it's a function argument
                if var_name in input_args:
                    return var_name
                
                # Skip if it's a keyword or operator
                if var_name in keywords or var_name in operators:
                    return var_name
                
                # Skip if it's a number
                if var_name.isdigit() or var_name.replace('.', '').isdigit():
                    return var_name
                
                # Get context around the match
                line_before = line[:match.start()]
                line_after = line[match.end():]
                
                # Skip if it's a method call (preceded by .)
                if line_before.endswith('.'):
                    return var_name
                
                # Skip if it's a string literal (preceded by ' or ")
                if line_before.endswith("'") or line_before.endswith('"'):
                    return var_name
                
                # Skip if it's a method name (followed by .)
                if line_after.startswith('.'):
                    return var_name
                
                # Skip if it's a string literal (followed by ' or ")
                if line_after.startswith("'") or line_after.startswith('"'):
                    return var_name
                
                # Special case: if it's followed by [ and it's a local variable, it's likely a DataFrame/array access
                # like filtered['user_id'] - this should get self. prefix
                if line_after.startswith('[') and var_name in local_vars:
                    return f'self.{var_name}'
                
                # Add self. prefix to local variables
                if var_name in local_vars:
                    return f'self.{var_name}'
                
                return var_name
            
            # Pattern to match variable names in complex expressions
            # This handles cases like df1['activity'], filtered['user_id'], etc.
            pattern = r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b'
            processed_line = re.sub(pattern, replace_var, line)
            processed_lines.append(processed_line)
        
        return processed_lines
    
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
    
    # Parse the function to get input arguments
    tree = ast.parse(converted_code)
    func_def = tree.body[0]
    if not isinstance(func_def, ast.FunctionDef):
        raise ValueError("Input must be a function definition")
    
    input_args = [arg.arg for arg in func_def.args.args]
    num_args = len(input_args)
    
    # Use the cleaned code (same as SSA format) for splitting
    # Parse the cleaned code to get body lines
    cleaned_code = preprocess_code(original_code)  # Use the original code, not converted_code
    cleaned_tree = ast.parse(cleaned_code)
    cleaned_func_def = cleaned_tree.body[0]
    
    # Extract function body lines from cleaned code (excluding function definition and return)
    body_lines = []
    return_line = None
    
    for stmt in cleaned_func_def.body:
        if isinstance(stmt, ast.Return):
            return_line = astor.to_source(stmt).strip()
            # Include return statement in body lines
            body_lines.append(return_line)
        else:
            body_lines.append(astor.to_source(stmt).strip())
    
    # Extract all local variables from the entire function
    local_vars = extract_local_variables(body_lines)
    
    # Filter out invalid cuts: only allow cuts where 2 < line_number < len(body_lines) + 1
    valid_cut_min = 3
    valid_cut_max = len(body_lines)
    filtered_cuts = [cut for cut in ranked_cuts if valid_cut_min <= cut['line_number'] <= valid_cut_max]
    ranked_cuts = filtered_cuts

    # Generate process tables with code parts
    process_tables = {}
    
    if num_args <= 1:
        # For 0 or 1 arguments, just create empty process tables
        for i, arg in enumerate(input_args, 0):
            process_tables[f'process_table_{i}'] = f'    def process_table_{i}(self, {arg}):\n        yield None'
    else:
        # Use the best N-1 valid cuts to split the code into N parts
        best_cuts = filtered_cuts[:num_args - 1]
        sorted_cuts = sorted(best_cuts, key=lambda x: x['line_number'])
        
        # Split the code into parts
        start_line = 0
        for i in range(num_args):
            if i < len(sorted_cuts):
                # Use the cut point to determine the end of this part
                # To cut before line N, use body_lines[0:N-2] for the first part
                cut_line = sorted_cuts[i]['line_number'] - 2
                part_lines = body_lines[start_line:cut_line]
                start_line = cut_line
            else:
                # This is the last part (from last cut to end)
                part_lines = body_lines[start_line:]
            
            # Create process table with this part of the code
            arg_name = input_args[i]
            
            if part_lines:
                # Add self. prefix to local variables
                processed_lines = add_self_to_local_vars(part_lines, arg_name, local_vars, input_args)
                # Convert return statements to yield statements
                processed_lines = convert_return_to_yield(processed_lines)
                process_table_body = '\n'.join(f'        {line}' for line in processed_lines)
                
                # Analyze which arguments are actually used in this process table's body
                used_args = analyze_used_arguments(processed_lines, input_args)
                
                # Create method signature with only the arguments that are actually used
                if used_args:
                    args_str = ', '.join(['self'] + used_args)
                else:
                    args_str = 'self'
                
                process_table_code = f'    def process_table_{i}({args_str}):\n{process_table_body}'
            else:
                process_table_code = f'    def process_table_{i}(self):\n        yield None'
            
            process_tables[f'process_table_{i}'] = process_table_code
    
    # Wrap all process tables in an Operator class
    operator_class_code = "class Operator:\n"
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

    test_code = """
import pandas as pd
def enrich_and_score(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    # Step 1: Filter df1
    df1_filtered = df1[df1['activity'] != 'idle']

    # Step 2: Merge df1 with df2 on user_id
    merged = pd.merge(df1_filtered, df2, on='user_id', how='inner')

    # Step 3: Define a simple activity -> value mapping
    activity_points = {
        'login': 1,
        'logout': 0.5,
        'purchase': 5,
        'comment': 2
    }

    # Step 4: Compute score = activity_value * group weight
    merged['activity_value'] = merged['activity'].map(activity_points).fillna(0)
    merged['score'] = merged['activity_value'] * merged['weight']

    return merged[['user_id', 'activity', 'group', 'score']]
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
    print("Testing Split Function:")
    print("="*50)
    
    # Test the Split function
    try:
        result = Split(test_code)
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
                result_with_cut = Split(test_code, line_number=cut_line)
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
        print(f"Error in Split function: {e}")
 