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
        "code": "import pandas as pd\ndef function(X, Y):\n    X = X + 1\n    return X",
        "line_number": 3  // optional
    }
    
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
        
        # Extract optional line number
        line_number = data.get('line_number')
        
        # Validate that code is a string
        if not isinstance(code, str):
            return jsonify({
                'success': False,
                'error': 'Code must be a string'
            }), 400
        
        # Validate line_number if provided
        if line_number is not None:
            if not isinstance(line_number, int) or line_number < 1:
                return jsonify({
                    'success': False,
                    'error': 'line_number must be a positive integer'
                }), 400
        
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
    """
    code = request.args.get('code')
    line_number = request.args.get('line_number')
    
    if not code:
        return jsonify({
            'success': False,
            'error': 'No code provided in query parameter'
        }), 400
    
    # Convert line_number to int if provided
    if line_number:
        try:
            line_number = int(line_number)
        except ValueError:
            return jsonify({
                'success': False,
                'error': 'line_number must be a valid integer'
            }), 400
    
    # Create a mock POST request
    mock_data = {'code': code}
    if line_number:
        mock_data['line_number'] = line_number
    
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
    example_code = '''import pandas as pd

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
    
    example_request = {
        'code': example_code,
        'line_number': 5  # Optional: specify a specific line to cut at
    }
    
    return jsonify({
        'example_request': example_request,
        'description': 'This is an example of the expected request format for the compile endpoint.'
    })

if __name__ == '__main__':
    print("Starting UDF Compiling Service...")
    print("Available endpoints:")
    print("  GET  /health - Health check")
    print("  POST /compile - Compile Python code to operator class")
    print("  GET  /compile - Test endpoint (use query parameters)")
    print("  GET  /example - Get example request format")
    print("\nService will be available at http://localhost:9999")
    
    app.run(host='0.0.0.0', port=9999, debug=True) 