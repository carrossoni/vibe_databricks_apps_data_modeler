#!/usr/bin/env python3
"""
Databricks Data Modeler - Databricks App

Authors: Luiz Carrossoni Neto
Revision: 1.0

This is the main entry point for the Databricks Data Modeler when running as a Databricks App.
The tool provides a visual Entity Relationship Diagram (ERD) interface for designing and managing
database schemas in Databricks Unity Catalog.

Features:
- Visual table design with drag-and-drop interface
- Import existing tables from Databricks Unity Catalog
- Generate DDL for table creation and modification
- Support for relationships, foreign keys, and constraints
- Cross-catalog and cross-schema support
- Service principal authentication for Databricks Apps

Note: This is experimental code designed for testing and educational purposes.

Startup Command (when running as Databricks App):
The app is automatically started by Databricks Apps infrastructure using: python app.py
"""

import os
import sys
import logging
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

# Add backend directory to Python path for imports
backend_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backend')
if backend_path not in sys.path:
    sys.path.insert(0, backend_path)

# Import our existing backend modules
from databricks_integration import DatabricksUnityService
from data_modeling_routes import data_modeling_bp
from models.data_modeling import DataModelProject

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_databricks_client():
    """Get authenticated Databricks client using user or app credentials"""
    try:
        # Prioritize user authorization (on-behalf-of) when available
        user_token = request.headers.get('x-forwarded-access-token') if request else None
        logger.info(f"🔍 App.py checking for user token: {'Found' if user_token else 'Not found'}")
        
        if user_token:
            logger.info("🔑 Using user authorization (on-behalf-of)")
            host = os.getenv('DATABRICKS_SERVER_HOSTNAME') or os.getenv('DATABRICKS_HOST')
            if host:
                # Temporarily clear environment variables to avoid conflicts
                original_client_id = os.environ.pop('DATABRICKS_CLIENT_ID', None)
                original_client_secret = os.environ.pop('DATABRICKS_CLIENT_SECRET', None)
                original_host = os.environ.pop('DATABRICKS_HOST', None)
                
                try:
                    # Create client with ONLY user token using PAT auth type for OBO
                    client = WorkspaceClient(host=host, token=user_token, auth_type="pat")
                    logger.info("✅ Successfully created user-authenticated client (OBO)")
                    return client
                except Exception as e:
                    logger.error(f"Failed to create OBO client: {e}")
                finally:
                    # Restore environment variables
                    if original_client_id:
                        os.environ['DATABRICKS_CLIENT_ID'] = original_client_id
                    if original_client_secret:
                        os.environ['DATABRICKS_CLIENT_SECRET'] = original_client_secret
                    if original_host:
                        os.environ['DATABRICKS_HOST'] = original_host
        
        # Fallback: Check if we're running in Databricks Apps environment with service principal
        client_id = os.getenv('DATABRICKS_CLIENT_ID')
        client_secret = os.getenv('DATABRICKS_CLIENT_SECRET')
        
        if client_id and client_secret:
            # Use app authorization (service principal)
            logger.info("🔧 Using app authorization (service principal)")
            host = os.getenv('DATABRICKS_SERVER_HOSTNAME') or os.getenv('DATABRICKS_HOST')
            try:
                config = Config(
                    host=host,
                    client_id=client_id,
                    client_secret=client_secret
                )
                client = WorkspaceClient(config=config)
                logger.info("✅ Successfully created service principal client")
                return client
            except Exception as e:
                logger.error(f"Failed to create service principal client: {e}")
        
        logger.error("No valid authentication method available")
        return None
            
    except Exception as e:
        logger.error(f"Failed to create Databricks client: {e}")
        return None

def create_app():
    """Create and configure the Flask application for Databricks Apps"""
    app = Flask(__name__, static_folder='static', static_url_path='')
    
    # Configure CORS for Databricks Apps environment
    CORS(app, origins=['*'])  # Databricks Apps handles security
    
    def get_user_token():
        """Get user access token from Databricks Apps headers"""
        return request.headers.get('x-forwarded-access-token')
    
    # Register our existing data modeling blueprint
    app.register_blueprint(data_modeling_bp, url_prefix='/api/data_modeling')
    
    # Health check endpoint
    @app.route('/api/health')
    def health_check():
        """Health check endpoint for Databricks Apps"""
        try:
            client = get_databricks_client()
            status = "healthy" if client else "unhealthy"
            return jsonify({
                "service": "bricks-data-modeler-app",
                "status": status,
                "environment": "databricks-apps"
            })
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return jsonify({
                "service": "bricks-data-modeler-app", 
                "status": "unhealthy",
                "error": str(e)
            }), 500
    
    # Serve the frontend
    @app.route('/')
    def serve_frontend():
        """Serve the main frontend application"""
        return send_from_directory('static', 'index.html')
    
    @app.route('/<path:path>')
    def serve_static_files(path):
        """Serve static frontend files"""
        return send_from_directory('static', path)
    
    # Context processor to inject Databricks client into request context
    @app.before_request
    def inject_databricks_context():
        """Inject Databricks client and user context into request"""
        request.databricks_client = get_databricks_client()
        request.user_token = get_user_token()
    
    # Error handlers
    @app.errorhandler(404)
    def not_found(error):
        return jsonify({'error': 'Not found'}), 404
    
    @app.errorhandler(500)
    def internal_error(error):
        logger.error(f"Internal server error: {error}")
        return jsonify({'error': 'Internal server error'}), 500
    
    return app

def main():
    """Main entry point for the Databricks App"""
    logger.info("Starting Bricks Data Modeler Databricks App")
    
    # Create the Flask app
    app = create_app()
    
    # Get port from environment (Databricks Apps will set this)
    port = int(os.getenv('PORT', 8080))
    host = os.getenv('HOST', '0.0.0.0')
    
    logger.info(f"Starting server on {host}:{port}")
    
    # Run the app
    app.run(
        host=host,
        port=port,
        debug=os.getenv('FLASK_DEBUG', 'False').lower() == 'true'
    )

if __name__ == '__main__':
    main()