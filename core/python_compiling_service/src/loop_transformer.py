import ast
import astor

class LoopToYieldTransformer(ast.NodeTransformer):
    """
    Transforms functions that collect results in loops into streaming/yielding functions.
    
    Detects patterns:
    1. Explicit loop pattern:
       - results = []
       - for item in data:
       -     result = process(item)
       -     results.append(result)
       - return results, other_stuff
    
    2. List comprehension pattern:
       - results = [process(item) for item in data]
       - return results, other_stuff
    
    Transforms to:
       - for item in data:
       -     result = process(item) 
       -     yield {"result": result}
       - yield {"other_stuff": other_stuff}
    """
    
    def __init__(self):
        super().__init__()
        self.collection_vars = set()  # Variables used to collect results (like 'preds')
        self.return_collections = set()  # Collections that are returned
        self.comprehension_vars = {}  # Map from variable to its comprehension info
        
    def visit_FunctionDef(self, node):
        """Transform function by detecting collection patterns."""
        # Reset for each function
        self.collection_vars = set()
        self.return_collections = set()
        self.comprehension_vars = {}
        
        # First pass: identify collection variables and return patterns
        self._analyze_function(node)
        
        # Second pass: transform the function body
        new_body = []
        for stmt in node.body:
            transformed_stmt = self.visit(stmt)
            if transformed_stmt is not None:
                if isinstance(transformed_stmt, list):
                    new_body.extend(transformed_stmt)
                else:
                    new_body.append(transformed_stmt)
        
        # Create new function with transformed body
        return ast.FunctionDef(
            name=node.name,
            args=node.args,
            body=new_body,
            decorator_list=node.decorator_list,
            returns=node.returns
        )
    
    def _analyze_function(self, node):
        """Analyze function to identify collection patterns."""
        # Find collection initializations (e.g., preds = [] or self.preds = [])
        for stmt in node.body:
            if isinstance(stmt, ast.Assign):
                if len(stmt.targets) == 1:
                    target = stmt.targets[0]
                    var_name = None
                    
                    # Handle both regular variables and self.variable assignments
                    if isinstance(target, ast.Name):
                        var_name = target.id
                    elif (isinstance(target, ast.Attribute) and 
                          isinstance(target.value, ast.Name) and 
                          target.value.id == 'self'):
                        var_name = target.attr
                    
                    if var_name:
                        # Pattern 1: Empty list initialization (preds = [] or self.preds = [])
                        if (isinstance(stmt.value, ast.List) and 
                            len(stmt.value.elts) == 0):
                            self.collection_vars.add(var_name)
                        
                        # Pattern 2: List comprehension (preds = [expr for item in data])
                        elif isinstance(stmt.value, ast.ListComp):
                            self.collection_vars.add(var_name)
                            self.comprehension_vars[var_name] = {
                                'comprehension': stmt.value,
                                'target': stmt.value.generators[0].target,
                                'iter': stmt.value.generators[0].iter,
                                'elt': stmt.value.elt
                            }
        
        # Find what's returned to identify which collections are returned
        for stmt in node.body:
            if isinstance(stmt, ast.Return) and stmt.value:
                self._analyze_return_value(stmt.value)
    
    def _analyze_return_value(self, value):
        """Analyze return value to find returned collections."""
        if isinstance(value, ast.Name):
            if value.id in self.collection_vars:
                self.return_collections.add(value.id)
        elif (isinstance(value, ast.Attribute) and
              isinstance(value.value, ast.Name) and
              value.value.id == 'self'):
            if value.attr in self.collection_vars:
                self.return_collections.add(value.attr)
        elif isinstance(value, ast.Tuple):
            for elt in value.elts:
                if isinstance(elt, ast.Name) and elt.id in self.collection_vars:
                    self.return_collections.add(elt.id)
                elif (isinstance(elt, ast.Attribute) and
                      isinstance(elt.value, ast.Name) and
                      elt.value.id == 'self' and
                      elt.attr in self.collection_vars):
                    self.return_collections.add(elt.attr)
    
    def visit_Assign(self, node):
        """Handle assignment statements."""
        if len(node.targets) == 1:
            target = node.targets[0]
            var_name = None
            
            # Handle both regular variables and self.variable assignments
            if isinstance(target, ast.Name):
                var_name = target.id
            elif (isinstance(target, ast.Attribute) and 
                  isinstance(target.value, ast.Name) and 
                  target.value.id == 'self'):
                var_name = target.attr
            
            if var_name:
                # Remove empty list initializations (e.g., preds = [] or self.preds = [])
                if (var_name in self.collection_vars and
                    isinstance(node.value, ast.List) and 
                    len(node.value.elts) == 0):
                    return None  # Remove this assignment
                
                # Transform list comprehensions to for loops with yields
                elif (var_name in self.collection_vars and
                      isinstance(node.value, ast.ListComp)):
                    return self._transform_list_comprehension(var_name, node.value)
        
        return node
    
    def _transform_list_comprehension(self, var_name, list_comp):
        """Transform list comprehension to for loop with yield statements."""
        if not list_comp.generators:
            return None
        
        generator = list_comp.generators[0]  # Take the first generator
        
        # Determine the key name for yielded values
        if var_name == 'preds':
            key_name = 'pred'
        elif var_name == 'results':
            key_name = 'result'
        elif var_name == 'result':
            key_name = 'result'
        elif var_name == 'outputs':
            key_name = 'output'  
        elif var_name == 'items':
            key_name = 'item'
        else:
            # Use singular form or just the variable name
            key_name = var_name.rstrip('s') if var_name.endswith('s') else var_name
        
        # Create yield statement
        yield_stmt = ast.Expr(value=ast.Yield(value=ast.Dict(
            keys=[ast.Constant(value=key_name)],
            values=[list_comp.elt]
        )))
        
        # Create for loop with yield
        for_loop = ast.For(
            target=generator.target,
            iter=generator.iter,
            body=[yield_stmt],
            orelse=[]
        )
        
        return for_loop
    
    def visit_For(self, node):
        """Transform for loops that contain collection appends."""
        new_body = []
        
        for stmt in node.body:
            if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Call):
                call = stmt.value
                # Detect append calls on collection variables (both var.append() and self.var.append())
                if (isinstance(call.func, ast.Attribute) and
                    call.func.attr == 'append'):
                    
                    collection_name = None
                    
                    # Handle both var.append() and self.var.append()
                    if isinstance(call.func.value, ast.Name):
                        if call.func.value.id in self.collection_vars:
                            collection_name = call.func.value.id
                    elif (isinstance(call.func.value, ast.Attribute) and
                          isinstance(call.func.value.value, ast.Name) and
                          call.func.value.value.id == 'self'):
                        if call.func.value.attr in self.collection_vars:
                            collection_name = call.func.value.attr
                    
                    if collection_name:
                        # Transform append to yield
                        if len(call.args) == 1:
                            value = call.args[0]
                            # Determine the key name for the yielded value
                            if collection_name == 'preds':
                                key_name = 'pred'
                            elif collection_name == 'results':
                                key_name = 'result'
                            elif collection_name == 'result':
                                key_name = 'result'
                            elif collection_name == 'outputs':
                                key_name = 'output'  
                            elif collection_name == 'items':
                                key_name = 'item'
                            else:
                                # Use singular form or just the variable name
                                key_name = collection_name.rstrip('s') if collection_name.endswith('s') else collection_name
                            
                            yield_stmt = ast.Expr(value=ast.Yield(value=ast.Dict(
                                keys=[ast.Constant(value=key_name)],
                                values=[value]
                            )))
                            new_body.append(yield_stmt)
                        continue
            
            # Keep other statements as-is (but visit them for nested transformations)
            transformed_stmt = self.visit(stmt)
            if transformed_stmt is not None:
                new_body.append(transformed_stmt)
        
        # Return the transformed for loop
        return ast.For(
            target=node.target,
            iter=node.iter,
            body=new_body,
            orelse=node.orelse
        )
    
    def visit_Return(self, node):
        """Transform return statements."""
        if not node.value:
            return node
        
        # Handle different return patterns
        if isinstance(node.value, ast.Tuple):
            # Return tuple like (preds, accuracy) or (self.preds, self.accuracy)
            non_collection_values = []
            
            for elt in node.value.elts:
                is_collection = False
                if isinstance(elt, ast.Name) and elt.id in self.return_collections:
                    is_collection = True
                elif (isinstance(elt, ast.Attribute) and
                      isinstance(elt.value, ast.Name) and
                      elt.value.id == 'self' and
                      elt.attr in self.return_collections):
                    is_collection = True
                
                if is_collection:
                    # Skip collection variables as they're now yielded individually
                    continue
                else:
                    non_collection_values.append(elt)
            
            # If there are non-collection values, yield them
            if non_collection_values:
                yield_stmts = []
                for i, value in enumerate(non_collection_values):
                    # Try to infer a meaningful key name
                    if isinstance(value, ast.Name):
                        key_name = value.id
                    elif (isinstance(value, ast.Call) and 
                          isinstance(value.func, ast.Name)):
                        # If it's a function call, use function name
                        key_name = value.func.id
                    else:
                        key_name = f"result_{i}"
                    
                    yield_stmt = ast.Expr(value=ast.Yield(value=ast.Dict(
                        keys=[ast.Constant(value=key_name)],
                        values=[value]
                    )))
                    yield_stmts.append(yield_stmt)
                
                return yield_stmts
            else:
                return None  # Remove return if only collections were returned
                
        elif isinstance(node.value, ast.Name):
            # Single return value
            if node.value.id in self.return_collections:
                return None  # Remove return of collection
            else:
                # Yield non-collection value
                key_name = node.value.id
                return ast.Expr(value=ast.Yield(value=ast.Dict(
                    keys=[ast.Constant(value=key_name)],
                    values=[node.value]
                )))
        elif (isinstance(node.value, ast.Attribute) and
              isinstance(node.value.value, ast.Name) and
              node.value.value.id == 'self'):
            # Single return value like self.variable
            if node.value.attr in self.return_collections:
                return None  # Remove return of collection
            else:
                # Yield non-collection value
                key_name = node.value.attr
                return ast.Expr(value=ast.Yield(value=ast.Dict(
                    keys=[ast.Constant(value=key_name)],
                    values=[node.value]
                )))
        
        # For other return patterns, keep as-is but transform the value
        return ast.Return(value=self.visit(node.value))


def transform_code(code):
    """Transform code to use yielding instead of collecting results."""
    tree = ast.parse(code)
    transformer = LoopToYieldTransformer()
    transformed = transformer.visit(tree)
    ast.fix_missing_locations(transformed)
    return astor.to_source(transformed)


class LoopTransformer:
    """Wrapper class for loop transformation to match expected interface."""
    
    def transform_function(self, code):
        """Transform function code using the loop transformer."""
        return transform_code(code)


if __name__ == "__main__":
    print("Testing with origin.py:")
    print("="*50)
    with open("tests/origin.py", "r") as f:
        origin_code = f.read()
    
    print("Original code:")
    print(origin_code)
    print("\nTransformed code:")
    transformed_code = transform_code(origin_code)
    print(transformed_code)
    
    with open("tests/target_generated.py", "w") as f:
        f.write(transformed_code)
    
    print("\n" + "="*50)
    print("Testing with origin2.py:")
    print("="*50)
    with open("tests/origin2.py", "r") as f:
        origin2_code = f.read()
    
    print("Original code:")
    print(origin2_code)
    print("\nTransformed code:")
    transformed_code2 = transform_code(origin2_code)
    print(transformed_code2)
    
    with open("tests/target_generated2.py", "w") as f:
        f.write(transformed_code2)