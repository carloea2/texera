#!/usr/bin/env python3
"""
HTTP Service for UDF Compiling API

This service provides an API endpoint that receives Python code snippets
and returns the compiled operator class using the compile() function.
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import json
import traceback
import sys
import os

# Add the src directory to the path so we can import our modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from UDF_splitter import compile

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({
        'status': 'healthy',
        'service': 'udf-compiling-api',
        'version': '1.0.0'
    })

@app.route('/compile', methods=['POST'])
def compile_code():
    """
    API endpoint to compile Python code into an operator class.
    
    Expected JSON payload:
    {
        "code": "#3\nimport pandas as pd\ndef function(X, Y):\n    X = X + 1\n    return X"
    }
    
    If the first line of code is a comment with a number (e.g., "#3"), that number will be used as the line number to split.
    If the first line is "#baseline", baseline compilation mode will be used.
    
    Returns:
    {
        "success": true,
        "result": {
            "ranked_cuts": [...],
            "operator_class": "import pandas as pd\n\nclass Operator:\n    def process_table_0(...):\n        ...",
            "ssa_code": "...",
            "converted_code": "...",
            "num_args": 2,
            "cuts_used": [...],
            "import_statements": ["import pandas as pd"]
        }
    }
    """
    try:
        # Get JSON data from request
        data = request.get_json()
        
        if not data:
            return jsonify({
                'success': False,
                'error': 'No JSON data provided'
            }), 400
        
        # Extract code from request
        code = data.get('code')
        if not code:
            return jsonify({
                'success': False,
                'error': 'No code provided in request'
            }), 400
        
        # Validate that code is a string
        if not isinstance(code, str):
            return jsonify({
                'success': False,
                'error': 'Code must be a string'
            }), 400
        
        # Parse line number from first line if it's a comment with a number
        line_number = None
        lines = code.split('\n')
        if lines:
            first_line = lines[0].strip()
            if first_line.startswith('#') and len(first_line) > 1:
                # Check if the part after # is a number
                comment_content = first_line[1:].strip()
                if comment_content.isdigit():
                    line_number = int(comment_content)
                    print(f"Extracted line number {line_number} from first line: {first_line}")
        
        # Call the compile function
        result = compile(code, line_number)
        
        # Convert the result to JSON-serializable format
        serializable_result = {
            'ranked_cuts': result['ranked_cuts'],
            'operator_class': result['operator_class'],
            'ssa_code': result['ssa_code'],
            'converted_code': result['converted_code'],
            'num_args': result['num_args'],
            'cuts_used': result['cuts_used'],
            'import_statements': result['import_statements']
        }
        print(result['operator_class'])
        return result['operator_class']
        
    except Exception as e:
        # Log the error for debugging
        print(f"Error in compile_code endpoint: {str(e)}")
        print(traceback.format_exc())
        
        return jsonify({
            'success': False,
            'error': f'Internal server error: {str(e)}'
        }), 500

@app.route('/compile', methods=['GET'])
def compile_code_get():
    """
    GET endpoint for testing - accepts code as query parameter.
    If the first line of code is a comment with a number (e.g., "#3"), that number will be used as the line number to split.
    """
    code = request.args.get('code')
    
    if not code:
        return jsonify({
            'success': False,
            'error': 'No code provided in query parameter'
        }), 400
    
    # Parse line number from first line if it's a comment with a number
    line_number = None
    lines = code.split('\n')
    if lines:
        first_line = lines[0].strip()
        if first_line.startswith('#') and len(first_line) > 1:
            # Check if the part after # is a number
            comment_content = first_line[1:].strip()
            if comment_content.isdigit():
                line_number = int(comment_content)
                print(f"Extracted line number {line_number} from first line: {first_line}")
    
    # Use the same logic as POST endpoint
    try:
        result = compile(code, line_number)
        
        serializable_result = {
            'ranked_cuts': result['ranked_cuts'],
            'operator_class': result['operator_class'],
            'ssa_code': result['ssa_code'],
            'converted_code': result['converted_code'],
            'num_args': result['num_args'],
            'cuts_used': result['cuts_used'],
            'import_statements': result['import_statements']
        }
        
        return result['operator_class']
        
    except Exception as e:
        print(f"Error in compile_code_get endpoint: {str(e)}")
        print(traceback.format_exc())
        
        return jsonify({
            'success': False,
            'error': f'Internal server error: {str(e)}'
        }), 500

@app.route('/example', methods=['GET'])
def get_example():
    """Get an example of the expected request format."""
    
    # Example 1: Normal compilation with specific line cut
    example_code_with_cut = '''#5
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
    
    return merged[['user_id', 'activity', 'group', 'score']]'''
    
    # Example 2: Baseline compilation
    example_code_baseline = '''#baseline
import pandas as pd

def simple_function(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    merged = pd.merge(df1, df2, on='user_id', how='inner')
    return merged[['user_id', 'activity', 'score']]'''
    
    # Example 3: Auto compilation (no line number)
    example_code_auto = '''import pandas as pd

def auto_compile(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    df1_filtered = df1[df1['activity'] != 'idle']
    merged = pd.merge(df1_filtered, df2, on='user_id', how='inner')
    return merged[['user_id', 'activity', 'score']]'''
    
    examples = {
        'example_with_line_cut': {
            'code': example_code_with_cut,
            'description': 'Compilation with specific line cut. The first line "#5" specifies to cut at line 5.'
        },
        'example_baseline': {
            'code': example_code_baseline,
            'description': 'Baseline compilation. The first line "#baseline" creates a single process_tables method.'
        },
        'example_auto': {
            'code': example_code_auto,
            'description': 'Auto compilation. No line number specified, will use optimal cuts based on dependency analysis.'
        }
    }
    
    return jsonify({
        'examples': examples,
        'instructions': {
            'line_cut': 'Start your code with "#<number>" to specify a line to cut at',
            'baseline': 'Start your code with "#baseline" for baseline compilation (single method)',
            'auto': 'No special first line for automatic optimal cutting'
        }
    })

if __name__ == '__main__':
    print("Starting UDF Compiling Service...")
    print("Available endpoints:")
    print("  GET  /health - Health check")
    print("  POST /compile - Compile Python code to operator class")
    print("  GET  /compile - Test endpoint (use query parameters)")
    print("  GET  /example - Get example request formats")
    print("\nCode compilation modes:")
    print("  #<number>  - Cut at specific line number (e.g., '#5')")
    print("  #baseline  - Baseline compilation (single process_tables method)")
    print("  (no prefix) - Auto compilation with optimal cuts")
    print("\nService will be available at http://localhost:9999")
    
    app.run(host='0.0.0.0', port=9999, debug=True) 