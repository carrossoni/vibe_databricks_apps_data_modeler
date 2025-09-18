import json
import logging
import os
import uuid
from datetime import datetime

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

from flask import Flask, jsonify, request
from flask_cors import CORS
from pydantic import ValidationError
from data_modeling_routes import data_modeling_bp

# Debug
# os.environ["DATABRICKS_HOST"] = "https://e2-dogfood.staging.cloud.databricks.com/"
# Configure Databricks credentials via environment variables or CLI


# Create webserver (API only - no static file serving)
app = Flask(__name__)
CORS(app, origins=['http://localhost:3000', 'http://localhost:5173'])  # Allow frontend origins
logger = logging.getLogger(__name__)

# Register blueprints
app.register_blueprint(data_modeling_bp)

# Note: Legacy saved_canvases directory removed - projects are now saved in saved_projects/

@app.route('/')
def home():
    """API root endpoint"""
    return jsonify({
        'message': 'Databricks Data Modeler API',
        'version': '1.0',
        'endpoints': {
            'data_modeling': '/api/data_modeling/',
            'health': '/api/health'
        }
    })

@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'service': 'databricks-data-modeler-api'})


@app.route('/api/simplemessage', methods=['GET'])
def get_simple_message():
    response = jsonify({'message': 'Simple message from api!'})
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response




if __name__ == '__main__':
    # Start the server in debug mode so that static files can be updated on the fly (dev only!)
    port = int(os.environ.get('PORT', 8080))
    app.run(port=port, debug=True)
