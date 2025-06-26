import ast
# import astviz  # Only import in __main__

def detect_ports_and_classify_statements(source_code: str):
    tree = ast.parse(source_code)
    # astviz.view(tree)  # Remove from here
    fn_def = tree.body[0]
    
    # Step 1: Identify input arguments -> ports
    arg_ports = [arg.arg for arg in fn_def.args.args]
    port_origin = {arg: f"{arg}_port" for arg in arg_ports}
    var_to_port = {}

    # Step 2: Direct unpacking from input (e.g., X_train, y_train = train_set)
    for stmt in fn_def.body:
        if isinstance(stmt, ast.Assign) and isinstance(stmt.value, ast.Name):
            source = stmt.value.id
            if source in port_origin:
                targets = stmt.targets[0]
                if isinstance(targets, ast.Tuple):
                    for elt in targets.elts:
                        if isinstance(elt, ast.Name):
                            var_to_port[elt.id] = port_origin[source]

    # Step 3: Propagate port origin to derived variables
    def propagate_variable_origins(fn_body):
        for stmt in fn_body:
            lhs_vars = set()
            rhs_ports = set()
            for node in ast.walk(stmt):
                if isinstance(node, ast.Assign):
                    for target in node.targets:
                        if isinstance(target, ast.Name):
                            lhs_vars.add(target.id)
                elif isinstance(node, ast.Name):
                    if node.id in var_to_port:
                        rhs_ports.add(var_to_port[node.id])
            for var in lhs_vars:
                if len(rhs_ports) == 1:
                    var_to_port[var] = list(rhs_ports)[0]

    propagate_variable_origins(fn_def.body)

    # Step 4: Classify each statement
    stmt_to_port = []
    for stmt in fn_def.body:
        stmt_vars = {node.id for node in ast.walk(stmt) if isinstance(node, ast.Name)}
        used_ports = {var_to_port.get(var, None) for var in stmt_vars}
        used_ports.discard(None)

        if len(used_ports) == 1:
            port = list(used_ports)[0]
        elif len(used_ports) > 1:
            port = "shared"
        else:
            port = "global"

        stmt_to_port.append((ast.unparse(stmt).strip(), port))

    return port_origin, stmt_to_port

import pandas as pd
import astroid

from collections import defaultdict

def build_dependency_graph(function_node):
    dependency_graph = defaultdict(set)
    for stmt in function_node.body:
        if isinstance(stmt, astroid.Assign):
            rhs_vars = {v.name for v in stmt.value.nodes_of_class(astroid.Name)}
            for target in stmt.targets:
                if isinstance(target, astroid.AssignName):
                    dependency_graph[target.name].update(rhs_vars)
        elif isinstance(stmt, (astroid.For, astroid.Expr, astroid.Return)):
            for node in stmt.nodes_of_class(astroid.AssignName):
                rhs_vars = {v.name for v in stmt.nodes_of_class(astroid.Name)}
                dependency_graph[node.name].update(rhs_vars)
    return dependency_graph

def backward_propagate_ports(var_port_map, dependency_graph):
    updated = True
    while updated:
        updated = False
        for var, deps in dependency_graph.items():
            dep_ports = {var_port_map.get(dep) for dep in deps if dep in var_port_map}
            dep_ports.discard(None)
            if len(dep_ports) == 1 and (var not in var_port_map or var_port_map[var] != next(iter(dep_ports))):
                var_port_map[var] = next(iter(dep_ports))
                updated = True

def label_statements_by_port(code: str):
    import astroid
    import pandas as pd
    module = astroid.parse(code)
    function_node = next((n for n in module.body if isinstance(n, astroid.FunctionDef)), None)
    if function_node is None:
        raise ValueError("No function definition found.")

    # Step 1: Arguments as ports
    arg_names = [arg.name for arg in function_node.args.args]
    arg_port_map = {arg: f"{arg}_port" for arg in arg_names}
    var_port_map = dict(arg_port_map)  # Dye map: variable -> port or 'shared'

    statement_ports = []
    for stmt in function_node.body:
        # Find variables being assigned (Store)
        assigned_vars = {node.name for node in stmt.nodes_of_class(astroid.AssignName)}
        # Find variables being loaded (used)
        loaded_vars = {node.name for node in stmt.nodes_of_class(astroid.Name)}
        loaded_vars -= assigned_vars
        # Determine the port(s) of the loaded variables
        loaded_arg_ports = {arg_port_map.get(var) for var in loaded_vars if var in arg_port_map}
        loaded_arg_ports.discard(None)
        loaded_local_ports = {var_port_map.get(var) for var in loaded_vars if var in var_port_map and var not in arg_port_map}
        loaded_local_ports.discard(None)
        # 染色逻辑：优先 argument 的 port
        for var in assigned_vars:
            if loaded_arg_ports:
                if len(loaded_arg_ports) == 1:
                    var_port_map[var] = next(iter(loaded_arg_ports))
                else:
                    var_port_map[var] = "shared"
            elif loaded_local_ports:
                if len(loaded_local_ports) == 1:
                    var_port_map[var] = next(iter(loaded_local_ports))
                else:
                    var_port_map[var] = "shared"
            # else: do not assign if no clear port
        # Assign statement to port
        if assigned_vars:
            stmt_ports = {var_port_map.get(var) for var in assigned_vars if var in var_port_map}
            stmt_ports.discard(None)
            if len(stmt_ports) == 1:
                label = next(iter(stmt_ports))
            elif len(stmt_ports) > 1:
                label = "shared"
            else:
                label = "global"
        else:
            # If only loading, assign to the port(s) of the loaded variables
            all_ports = loaded_arg_ports | loaded_local_ports
            if len(all_ports) == 1:
                label = next(iter(all_ports))
            elif len(all_ports) > 1:
                label = "shared"
            else:
                label = "global"
        statement_ports.append((stmt.lineno, stmt.as_string(), label))

    df = pd.DataFrame(statement_ports, columns=["Line", "Statement", "Port Assignment"])

    # --- Backward propagation for global assignments ---
    # 1. Find all variables assigned as global
    global_vars = set()
    stmt_lineno_to_var = {}
    for i, (lineno, stmt_str, label) in enumerate(statement_ports):
        if label == "global":
            # Try to extract assigned variable(s) from the statement string
            # (astroid already gives us assigned_vars per statement)
            stmt = function_node.body[i]
            assigned_vars = {node.name for node in stmt.nodes_of_class(astroid.AssignName)}
            for var in assigned_vars:
                global_vars.add(var)
                stmt_lineno_to_var[lineno] = var

    # 2. For each global var, find which port's statements use it
    var_used_by_ports = {var: set() for var in global_vars}
    for i, stmt in enumerate(function_node.body):
        loaded_vars = {node.name for node in stmt.nodes_of_class(astroid.Name)}
        for var in global_vars:
            if var in loaded_vars:
                port_label = statement_ports[i][2]
                if port_label not in ("global", "shared"):
                    var_used_by_ports[var].add(port_label)

    # 3. Update variable and assignment statement port if only used by one port
    for var, ports in var_used_by_ports.items():
        if len(ports) == 1:
            port = next(iter(ports))
            # Update all assignment statements for this var
            for i, (lineno, stmt_str, label) in enumerate(statement_ports):
                if stmt_lineno_to_var.get(lineno) == var and label == "global":
                    statement_ports[i] = (lineno, stmt_str, port)
            # Update var_port_map for this var
            var_port_map[var] = port
        elif len(ports) > 1:
            # If used by multiple ports, mark as shared
            for i, (lineno, stmt_str, label) in enumerate(statement_ports):
                if stmt_lineno_to_var.get(lineno) == var and label == "global":
                    statement_ports[i] = (lineno, stmt_str, "shared")
            var_port_map[var] = "shared"
        # else: if not used, keep as global

    df = pd.DataFrame(statement_ports, columns=["Line", "Statement", "Port Assignment"])
    return df

if __name__ == "__main__":
#     source_code = """
# def train_and_evaluate_model(train_set, test_set):
#     X_train, y_train = train_set
#     X_test, y_test = test_set

#     model = LogisticRegression()
#     model.fit(X_train, y_train)

#     predictions = []
#     for x in X_test:
#         y_pred = model.predict(x)[0]
#         predictions.append(y_pred)

#     correct = 0
#     total = len(y_test)
#     for i, y_true in enumerate(y_test):
#         if predictions[i] == y_true:
#             correct += 1

#     accuracy = correct / total
#     return model, predictions, accuracy
#     """

    source_code = """
def f(x, y):
    model = train(x)
    yield (model, "model")

    for t in y:
        z = model.predict(t)
        yield z, "test"
"""
    # Print the AST of the source code
    import ast
    print(ast.dump(ast.parse(source_code), indent=4))

    # Visualize the AST using graphviz
    from graphviz import Digraph
    def ast_to_graphviz(node, graph=None, parent=None):
        import ast
        import hashlib
        # Color palette for variable nodes
        color_palette = [
            'lightblue', 'lightgreen', 'yellow', 'orange', 'pink', 'violet', 'gold', 'cyan', 'magenta', 'salmon', 'khaki', 'plum', 'wheat', 'tan', 'thistle', 'azure', 'beige', 'coral', 'ivory', 'lavender'
        ]

        if graph is None:
            graph = Digraph()
        node_id = str(id(node))
        # Show variable names for ast.Name nodes, with color
        if isinstance(node, ast.Name):
            label = f"Name: {node.id}"
            # Assign a color based on variable name
            color_idx = int(hashlib.md5(node.id.encode()).hexdigest(), 16) % len(color_palette)
            color = color_palette[color_idx]
            graph.node(node_id, label, style='filled', fillcolor=color)
        else:
            label = type(node).__name__
            graph.node(node_id, label)
        if parent:
            graph.edge(str(id(parent)), node_id)
        for child in ast.iter_child_nodes(node):
            ast_to_graphviz(child, graph, node)
        return graph

    tree = ast.parse(source_code)
    # graph = ast_to_graphviz(tree)
    # graph.render('ast_output', view=True)  # This will create ast_output.pdf and open it

    port_origin, stmt_to_port = detect_ports_and_classify_statements(source_code)
    # print(port_origin)
    # print(stmt_to_port)

    df = label_statements_by_port(source_code)
    print(df)

    # --- Variable Dependency Graph ---
    def build_variable_dependency_graph(source_code):
        import ast
        from graphviz import Digraph
        tree = ast.parse(source_code)
        fn_def = next((n for n in tree.body if isinstance(n, ast.FunctionDef)), None)
        if fn_def is None:
            raise ValueError("No function definition found.")
        dep_graph = {}  # var: set of vars it depends on
        # Add arguments as nodes
        for arg in fn_def.args.args:
            dep_graph[arg.arg] = set()
        # Helper to process a list of statements recursively
        def process_body(body):
            for stmt in body:
                # Assignments
                if isinstance(stmt, ast.Assign):
                    lhs_vars = set()
                    for target in stmt.targets:
                        if isinstance(target, ast.Name):
                            lhs_vars.add(target.id)
                        elif isinstance(target, ast.Tuple):
                            for elt in target.elts:
                                if isinstance(elt, ast.Name):
                                    lhs_vars.add(elt.id)
                    rhs_vars = {n.id for n in ast.walk(stmt.value) if isinstance(n, ast.Name)}
                    for lhs in lhs_vars:
                        if lhs not in dep_graph:
                            dep_graph[lhs] = set()
                        dep_graph[lhs].update(rhs_vars)
                # For loops (target depends on iter), and process body recursively
                elif isinstance(stmt, ast.For):
                    if isinstance(stmt.target, ast.Name):
                        iter_vars = {n.id for n in ast.walk(stmt.iter) if isinstance(n, ast.Name)}
                        dep_graph.setdefault(stmt.target.id, set()).update(iter_vars)
                    process_body(stmt.body)
                # If, While, With, etc. (process their bodies recursively)
                elif hasattr(stmt, 'body') and isinstance(stmt.body, list):
                    process_body(stmt.body)
                # Also process orelse blocks if present
                if hasattr(stmt, 'orelse') and isinstance(stmt.orelse, list):
                    process_body(stmt.orelse)
        process_body(fn_def.body)
        # Visualize
        g = Digraph(comment="Variable Dependency Graph")
        for var in dep_graph:
            g.node(var)
        for var, deps in dep_graph.items():
            for dep in deps:
                g.edge(dep, var)
        return g

    # Build and visualize the variable dependency graph
    # var_graph = build_variable_dependency_graph(source_code)
    # var_graph.render('variable_dependency_graph', view=True)

    import ast

class SSATransformer(ast.NodeTransformer):
    def __init__(self):
        self.version = {}
    
    def fresh(self, var):
        self.version[var] = self.version.get(var, 0) + 1
        return f"{var}_{self.version[var]}"
    
    def visit_Assign(self, node):
        self.generic_visit(node)
        if isinstance(node.targets[0], ast.Name):
            var = node.targets[0].id
            node.targets[0].id = self.fresh(var)
        return node
    
    def visit_Name(self, node):
        if isinstance(node.ctx, ast.Load) and node.id in self.version:
            node.id = f"{node.id}_{self.version[node.id]}"
        return node


tree = ast.parse(source_code)
ssa_tree = SSATransformer().visit(tree)
import astor
print(astor.to_source(ssa_tree))

def build_ssa_dependency_graph_with_lineno(tree):
    dep_graph = {}
    node_labels = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name):
            lhs = node.targets[0].id
            lineno = getattr(node, 'lineno', None)
            label = f"{lhs} (line {lineno})" if lineno else lhs
            node_labels[lhs] = label
            rhs_vars = {n.id for n in ast.walk(node.value) if isinstance(n, ast.Name)}
            dep_graph[lhs] = (rhs_vars, lineno)
    return dep_graph, node_labels

def visualize_dep_graph_with_lineno(dep_graph, node_labels):
    from graphviz import Digraph
    g = Digraph(comment="SSA Variable Dependency Graph with Line Numbers")
    for var, label in node_labels.items():
        g.node(var, label)
    for var, (deps, _) in dep_graph.items():
        for dep in deps:
            if dep in node_labels:
                g.edge(dep, var)
    g.render('ssa_variable_dependency_graph', view=True)

# Example usage:
tree = ast.parse(source_code)
ssa_tree = SSATransformer().visit(tree)
dep_graph, node_labels = build_ssa_dependency_graph_with_lineno(ssa_tree)
visualize_dep_graph_with_lineno(dep_graph, node_labels)