"""
Databricks Data Modeler - API Routes

Authors: Luiz Carrossoni Neto
Revision: 1.0

This module contains all the Flask API routes for the Databricks Data Modeler tool.
It provides RESTful endpoints for managing database schemas, tables, views, and relationships
in Databricks Unity Catalog.

Key Features:
- Table import/export with relationship detection
- DDL generation and schema application
- Cross-catalog and cross-schema support
- Real-time progress streaming for long operations
- Comprehensive error handling and validation

API Endpoints:
- /api/data_modeling/catalogs - List available catalogs
- /api/data_modeling/schemas/<catalog> - List schemas in catalog
- /api/data_modeling/tables/<catalog>/<schema> - List tables in schema
- /api/data_modeling/import_existing - Import existing tables with relationships
- /api/data_modeling/project/<id>/apply_changes - Apply schema changes to Databricks

Note: This is experimental code designed for testing and educational purposes.
"""

import json
import logging
import os
import uuid
from datetime import datetime
from typing import Dict, List, Optional

# Set up logger
logger = logging.getLogger(__name__)

from flask import Blueprint, jsonify, request, Response
import time
import threading
from queue import Queue
from pydantic import ValidationError

from databricks_integration import DatabricksUnityService
from models import (
    DataModelProject, DataTable, TableField, DataModelRelationship,
    DatabricksDataType, ForeignKeyReference, ExistingTableImport,
    DataModelYAMLSerializer, MetricView, MetricViewDimension,
    MetricViewMeasure, MetricViewJoin, MetricSourceRelationship, TraditionalView
)

def serialize_join(join):
    """Recursively serialize a join object including nested joins"""
    result = {
        'id': join.id, 
        'name': join.name, 
        'joined_table_name': join.joined_table_name, 
        'join_type': join.join_type, 
        'sql_on': join.sql_on, 
        'left_columns': join.left_columns, 
        'right_columns': join.right_columns, 
        'join_operators': join.join_operators, 
        'using': join.using
    }
    
    # Add nested joins if they exist
    if join.joins and len(join.joins) > 0:
        result['joins'] = [serialize_join(nested_join) for nested_join in join.joins]
    
    return result

def deduplicate_imported_tables(imported_tables: List[dict]) -> List[dict]:
    """Deduplicate imported tables by full name (catalog.schema.table)"""
    if not imported_tables:
        return imported_tables
    
    logger.info(f"🔍 DEBUG: Before deduplication: {len(imported_tables)} tables")
    
    # Create a map to track unique tables by full name
    unique_tables = {}
    for table in imported_tables:
        table_catalog = table.get('catalog_name', 'unknown')
        table_schema = table.get('schema_name', 'unknown')
        table_name = table.get('name', 'unknown')
        full_name = f"{table_catalog}.{table_schema}.{table_name}"
        
        # Keep the first occurrence of each unique table
        if full_name not in unique_tables:
            unique_tables[full_name] = table
        else:
            logger.info(f"🔄 Removing duplicate table: {full_name}")
    
    # Replace imported_tables with deduplicated list
    deduplicated_tables = list(unique_tables.values())
    logger.info(f"🔍 DEBUG: After deduplication: {len(deduplicated_tables)} tables")
    
    return deduplicated_tables

def serialize_table_for_json(table) -> dict:
    """Serialize DataTable to dict with proper datetime handling"""
    from models.data_modeling import DataTable
    
    if isinstance(table, DataTable):
        # Use model_dump with mode='json' to get proper serialization
        table_dict = table.model_dump(mode='json')
        
        # Ensure datetime fields are in ISO format
        if 'created_at' in table_dict and table_dict['created_at']:
            if isinstance(table_dict['created_at'], str) and 'GMT' in table_dict['created_at']:
                # Convert GMT format to ISO format
                from datetime import datetime
                try:
                    dt = datetime.strptime(table_dict['created_at'], '%a, %d %b %Y %H:%M:%S GMT')
                    table_dict['created_at'] = dt.isoformat()
                except:
                    # If parsing fails, use current time
                    table_dict['created_at'] = datetime.now().isoformat()
        
        if 'updated_at' in table_dict and table_dict['updated_at']:
            if isinstance(table_dict['updated_at'], str) and 'GMT' in table_dict['updated_at']:
                # Convert GMT format to ISO format
                from datetime import datetime
                try:
                    dt = datetime.strptime(table_dict['updated_at'], '%a, %d %b %Y %H:%M:%S GMT')
                    table_dict['updated_at'] = dt.isoformat()
                except:
                    # If parsing fails, use current time
                    table_dict['updated_at'] = datetime.now().isoformat()
        
        return table_dict
    else:
        # It's already a dict
        return table
import os
from databricks.sdk import WorkspaceClient


def _parse_databricks_error_message(error_msg: str, traditional_view, project) -> str:
    """
    Parse Databricks error messages and return user-friendly messages
    """
    import re
    
    # Table or view not found
    if 'TABLE_OR_VIEW_NOT_FOUND' in error_msg:
        # Extract table name from error message
        table_match = re.search(r'The table or view `([^`]+)` cannot be found', error_msg)
        if table_match:
            missing_table = table_match.group(1)
            return f"❌ Table or view '{missing_table}' not found in catalog '{project.catalog_name}.{project.schema_name}'. Please check:\n" \
                   f"• Table name spelling: '{missing_table}'\n" \
                   f"• Table exists in catalog: '{project.catalog_name}'\n" \
                   f"• Table exists in schema: '{project.schema_name}'\n" \
                   f"• Or use fully qualified name: '{project.catalog_name}.{project.schema_name}.{missing_table}' in your SQL"
        else:
            return f"❌ A table or view referenced in the SQL query was not found in catalog '{project.catalog_name}.{project.schema_name}'. Please verify all table names exist or use fully qualified names."
    
    # Schema not found
    elif 'SCHEMA_NOT_FOUND' in error_msg:
        return f"❌ Schema '{project.schema_name}' not found in catalog '{project.catalog_name}'. Please verify the schema exists."
    
    # Catalog not found
    elif 'CATALOG_NOT_FOUND' in error_msg:
        return f"❌ Catalog '{project.catalog_name}' not found. Please verify the catalog exists and you have access."
    
    # Permission errors
    elif any(perm in error_msg for perm in ['PERMISSION_DENIED', 'ACCESS_DENIED', 'INSUFFICIENT_PRIVILEGES']):
        return f"❌ Permission denied. You don't have sufficient privileges to create views in '{project.catalog_name}.{project.schema_name}'. Please contact your Databricks administrator."
    
    # SQL syntax errors
    elif 'PARSE_SYNTAX_ERROR' in error_msg or 'SQLSTATE: 42601' in error_msg:
        return f"❌ SQL syntax error in traditional view '{traditional_view.name}'. Please check your SQL query for syntax issues."
    
    # Invalid parameter
    elif 'INVALID_PARAMETER_VALUE' in error_msg:
        return f"❌ Invalid parameter in SQL query for traditional view '{traditional_view.name}'. Please check your SQL syntax and parameters."
    
    # Table/view name conflict
    elif 'EXPECT_VIEW_NOT_TABLE' in error_msg:
        return f"❌ Cannot create view '{traditional_view.name}' because a table with the same name already exists in '{project.catalog_name}.{project.schema_name}'. Please choose a different name for your view or drop the existing table first."
    
    # Generic fallback with helpful context
    else:
        return f"❌ Failed to create traditional view '{traditional_view.name}' in '{project.catalog_name}.{project.schema_name}'. Error: {error_msg}"


def _qualify_table_references(sql_query: str, catalog_name: str, schema_name: str) -> str:
    """
    Automatically qualify unqualified table references in SQL query with catalog.schema
    Handles various scenarios:
    - table_name → catalog.schema.table_name
    - schema.table_name → catalog.schema.table_name (replaces schema)
    - catalog.schema.table_name → unchanged
    - catalog.schema.more.parts → unchanged (invalid, but don't make it worse)
    """
    import re
    
    logger.info(f"🔧 Qualifying table references in SQL with {catalog_name}.{schema_name}")
    logger.info(f"🔧 Original SQL: {sql_query}")
    
    # Pattern to match table references after FROM and JOIN clauses
    # Captures the full table reference (including dots)
    table_patterns = [
        # FROM table_reference (captures full reference including dots)
        r'\bFROM\s+(?![\(\s])([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)\b(?!\s*\()',
        # JOIN table_reference (captures full reference including dots)
        r'\bJOIN\s+(?![\(\s])([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)\b(?!\s*\()',
        # LEFT/RIGHT/INNER/OUTER JOIN table_reference
        r'\b(?:LEFT|RIGHT|INNER|OUTER)\s+JOIN\s+(?![\(\s])([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)\b(?!\s*\()',
    ]
    
    modified_sql = sql_query
    replacements_made = []
    
    for pattern in table_patterns:
        matches = re.finditer(pattern, modified_sql, re.IGNORECASE)
        
        for match in reversed(list(matches)):  # Reverse to maintain positions
            table_reference = match.group(1)
            
            # Skip SQL keywords/functions
            if table_reference.upper() in ['SELECT', 'FROM', 'WHERE', 'GROUP', 'ORDER', 'HAVING', 'UNION', 'WITH']:
                continue
            
            # Count the parts (split by dots)
            parts = table_reference.split('.')
            qualified_name = None
            
            if len(parts) == 1:
                # Unqualified: table_name → catalog.schema.table_name
                qualified_name = f"{catalog_name}.{schema_name}.{table_reference}"
                replacements_made.append(f"{table_reference} -> {qualified_name}")
            elif len(parts) == 2:
                # Schema qualified: schema.table_name → catalog.schema.table_name
                # Replace the schema part with our target schema
                table_name = parts[1]
                qualified_name = f"{catalog_name}.{schema_name}.{table_name}"
                replacements_made.append(f"{table_reference} -> {qualified_name}")
            elif len(parts) == 3:
                # Already fully qualified: catalog.schema.table_name → unchanged
                logger.info(f"🔍 Table reference already fully qualified: {table_reference}")
                continue
            else:
                # More than 3 parts - invalid, but don't make it worse
                logger.warning(f"⚠️ Invalid table reference with {len(parts)} parts: {table_reference}")
                continue
            
            # Apply the replacement
            if qualified_name:
                start, end = match.span(1)
                modified_sql = modified_sql[:start] + qualified_name + modified_sql[end:]
    
    if replacements_made:
        logger.info(f"🔧 Qualified table references: {', '.join(replacements_made)}")
    
    logger.info(f"🔧 Modified SQL: {modified_sql}")
    return modified_sql


def _validate_traditional_view_sql(sql_query: str) -> str:
    """
    Validate SQL query for traditional views to prevent dangerous operations.
    Only allows SELECT statements for view creation.
    """
    import re
    
    if not sql_query or not sql_query.strip():
        raise ValueError("SQL query cannot be empty")
    
    # Clean and normalize the SQL
    sql_clean = sql_query.strip()
    
    # Remove comments (both -- and /* */)
    sql_clean = re.sub(r'--.*$', '', sql_clean, flags=re.MULTILINE)
    sql_clean = re.sub(r'/\*.*?\*/', '', sql_clean, flags=re.DOTALL)
    sql_clean = sql_clean.strip()
    
    # Convert to uppercase for pattern matching (but return original case)
    sql_upper = sql_clean.upper()
    
    # Define dangerous patterns that should be blocked
    dangerous_patterns = [
        # DDL operations (except CREATE VIEW which we handle)
        r'\b(CREATE|ALTER|DROP)\s+(?!VIEW\b)',
        # DML operations
        r'\b(INSERT|UPDATE|DELETE|MERGE|TRUNCATE)\b',
        # DCL operations  
        r'\b(GRANT|REVOKE)\b',
        # System/Admin operations
        r'\b(EXEC|EXECUTE|CALL)\b',
        # Stored procedures/functions
        r'\b(PROCEDURE|FUNCTION)\b',
        # Transaction control
        r'\b(COMMIT|ROLLBACK|SAVEPOINT)\b',
        # Database/schema operations
        r'\b(USE|SHOW|DESCRIBE|DESC)\s+(DATABASE|SCHEMA)\b',
        # Potentially dangerous functions
        r'\b(LOAD_FILE|INTO\s+OUTFILE|INTO\s+DUMPFILE)\b',
        # SQL injection patterns
        r';\s*\w+',  # Multiple statements
        r'\bUNION\s+(?:ALL\s+)?SELECT.*\bFROM\s+(?![\w\.\[\]]+\s*(?:WHERE|GROUP|ORDER|LIMIT|$))',  # Suspicious UNION
    ]
    
    # Check for dangerous patterns
    for pattern in dangerous_patterns:
        if re.search(pattern, sql_upper):
            logger.warning(f"Blocked dangerous SQL pattern: {pattern}")
            raise ValueError(f"SQL query contains prohibited operations. Traditional views can only contain SELECT statements.")
    
    # Ensure the query starts with SELECT (after removing whitespace/comments)
    if not re.match(r'^\s*SELECT\b', sql_upper):
        raise ValueError("Traditional view SQL must start with SELECT statement")
    
    # Check for multiple statements (semicolon followed by non-whitespace)
    if re.search(r';\s*\S', sql_clean):
        raise ValueError("Multiple SQL statements are not allowed in traditional views")
    
    # Additional validation: ensure it's a proper SELECT query structure
    if not re.search(r'\bFROM\b', sql_upper):
        logger.warning("SELECT query without FROM clause - might be a calculated view")
    
    logger.info("SQL query validation passed for traditional view")
    return sql_query  # Return original query with original case


def get_sdk_client():
    """Get Databricks SDK client - robust version prioritizing injected client"""
    try:
        # ALWAYS try the injected client first - this is the most reliable
        if hasattr(request, 'databricks_client') and request.databricks_client:
            logger.info("✅ Using pre-authenticated client from app.py injection")
            return request.databricks_client
        
        logger.warning("⚠️ No injected client available, falling back to direct authentication")
        
        # Fallback: Direct user token authentication (no env var manipulation)
        user_token = request.headers.get('x-forwarded-access-token') if request else None
        if user_token:
            logger.info("🔑 Attempting direct user authentication")
            host = os.getenv('DATABRICKS_SERVER_HOSTNAME') or os.getenv('DATABRICKS_HOST')
            if host:
                try:
                    # Create client directly without manipulating env vars
                    client = WorkspaceClient(host=host, token=user_token, auth_type="pat")
                    logger.info("✅ Successfully created fallback user client")
                    return client
                except Exception as e:
                    logger.error(f"Failed to create fallback user client: {e}")
        
        # Service principal fallback (read-only access to env vars)
        logger.info("🔑 Attempting service principal authentication")
        client_id = os.getenv('DATABRICKS_CLIENT_ID')
        client_secret = os.getenv('DATABRICKS_CLIENT_SECRET')
        host = os.getenv('DATABRICKS_SERVER_HOSTNAME') or os.getenv('DATABRICKS_HOST')
        
        if client_id and client_secret and host:
            try:
                client = WorkspaceClient(
                    host=host,
                    client_id=client_id,
                    client_secret=client_secret,
                    auth_type="oauth-m2m"
                )
                logger.info("✅ Successfully created service principal client")
                return client
            except Exception as e:
                logger.error(f"Failed to create service principal client: {e}")
        
        logger.error("❌ All authentication methods failed")
        return None
        
    except Exception as e:
        logger.error(f"❌ Critical error in get_sdk_client: {e}")
        import traceback
        traceback.print_exc()
        return None


logger = logging.getLogger(__name__)

# Global dictionary to track progress sessions
progress_sessions = {}
# Cancellation flags for import sessions
import_cancellations = {}
progress_lock = threading.Lock()


def create_progress_session(session_id: str):
    """Create a new progress session"""
    with progress_lock:
        progress_sessions[session_id] = Queue()


def send_progress_update(session_id: str, update: dict):
    """Send a progress update to a session"""
    print(f"📤 Sending progress update to session {session_id}: {update}")
    with progress_lock:
        if session_id in progress_sessions:
            progress_sessions[session_id].put(update)
        else:
            logger.warning(f"Session {session_id} not found when trying to send update")


def get_progress_updates(session_id: str):
    """Generator for progress updates"""
    if session_id not in progress_sessions:
        return
    
    queue = progress_sessions[session_id]
    print(f"📡 Starting SSE stream for session {session_id}")
    
    # Send immediate ping to establish connection
    yield f"data: {json.dumps({'type': 'connected', 'session_id': session_id})}\n\n"
    
    try:
        while True:
            try:
                # Wait for update with timeout - increased for cross-catalog imports
                update = queue.get(timeout=120)  # 120 second timeout for large imports
                if update is None:  # End signal
                    print(f"📡 End signal received for session {session_id}")
                    break
                print(f"📡 Sending SSE update: {update}")
                sse_data = f"data: {json.dumps(update)}\n\n"
                yield sse_data
                # Force flush to ensure real-time delivery
                import sys
                sys.stdout.flush()
            except Exception as e:
                print(f"⚠️ SSE timeout or error for session {session_id}: {e}")
                # Timeout or session ended
                break
    finally:
        # Clean up session
        print(f"📡 Cleaning up session {session_id}")
        with progress_lock:
            if session_id in progress_sessions:
                del progress_sessions[session_id]




def _sort_tables_by_dependencies(tables):
    """
    Sort tables by dependency order using topological sort.
    Tables with no foreign keys come first, tables that reference others come last.
    """
    from collections import defaultdict, deque
    
    logger.info(f"🔍 DEPENDENCY SORTING: Processing {len(tables)} tables")
    
    # Build dependency graph
    # graph[table_id] = list of table_ids that this table depends on (references)
    graph = defaultdict(list)
    in_degree = defaultdict(int)
    table_map = {table.id: table for table in tables}
    table_name_map = {table.name: table for table in tables}
    
    # Initialize in_degree for all tables
    for table in tables:
        in_degree[table.id] = 0
    
    # Build the dependency graph
    for table in tables:
        logger.info(f"🔍 Analyzing table: {table.name}")
        for field in table.fields:
            if field.is_foreign_key and field.foreign_key_reference:
                logger.info(f"   🔗 FK field: {field.name}, FK ref: {field.foreign_key_reference}")
                # This table depends on the referenced table
                if hasattr(field.foreign_key_reference, 'referenced_table_id'):
                    referenced_table_id = field.foreign_key_reference.referenced_table_id
                    referenced_table = table_map.get(referenced_table_id)
                    referenced_table_name = referenced_table.name if referenced_table else "UNKNOWN"
                    logger.info(f"      ➡️ References table ID: {referenced_table_id} ({referenced_table_name})")
                elif isinstance(field.foreign_key_reference, str) and '.' in field.foreign_key_reference:
                    # Handle legacy string format "table_name.field_name"
                    referenced_table_name = field.foreign_key_reference.split('.')[0]
                    # Find table by name
                    referenced_table = next((t for t in tables if t.name == referenced_table_name), None)
                    referenced_table_id = referenced_table.id if referenced_table else None
                    logger.info(f"      ➡️ Legacy format - References table name: {referenced_table_name} (ID: {referenced_table_id})")
                else:
                    referenced_table_id = None
                    logger.info(f"      ❌ Could not determine referenced table")
                
                if referenced_table_id and referenced_table_id in table_map:
                    # table depends on referenced_table_id
                    graph[referenced_table_id].append(table.id)
                    in_degree[table.id] += 1
                    logger.info(f"      ✅ Added dependency: {table.name} depends on {table_map[referenced_table_id].name}")
                else:
                    logger.warning(f"      ⚠️ Referenced table not found in current batch: {referenced_table_id}")
    
    logger.info(f"🔍 DEPENDENCY GRAPH:")
    for table_id, dependents in graph.items():
        table_name = table_map[table_id].name
        dependent_names = [table_map[dep_id].name for dep_id in dependents]
        logger.info(f"   {table_name} is referenced by: {dependent_names}")
    
    logger.info(f"🔍 IN-DEGREE (dependencies count):")
    for table_id, degree in in_degree.items():
        table_name = table_map[table_id].name
        logger.info(f"   {table_name}: {degree} dependencies")
    
    # Topological sort using Kahn's algorithm
    queue = deque([table_id for table_id in in_degree if in_degree[table_id] == 0])
    sorted_table_ids = []
    
    while queue:
        current_table_id = queue.popleft()
        sorted_table_ids.append(current_table_id)
        
        # Remove edges from current table
        for dependent_table_id in graph[current_table_id]:
            in_degree[dependent_table_id] -= 1
            if in_degree[dependent_table_id] == 0:
                queue.append(dependent_table_id)
    
    # Check for circular dependencies
    if len(sorted_table_ids) != len(tables):
        logger.warning(f"Circular dependency detected in tables: sorted {len(sorted_table_ids)} of {len(tables)} tables")
        
        # Handle remaining tables with circular dependencies
        remaining_tables = [table for table in tables if table.id not in sorted_table_ids]
        
        # For self-referencing tables (like dim_categoria), place them early
        # For other circular dependencies, add them based on their dependency count
        remaining_with_deps = []
        for table in remaining_tables:
            dep_count = sum(1 for field in table.fields 
                          if field.is_foreign_key and field.foreign_key_reference 
                          and (hasattr(field.foreign_key_reference, 'referenced_table_id') 
                               and field.foreign_key_reference.referenced_table_id != table.id))
            remaining_with_deps.append((table, dep_count))
        
        # Sort remaining tables by dependency count (fewer dependencies first)
        remaining_with_deps.sort(key=lambda x: x[1])
        remaining_sorted = [table for table, _ in remaining_with_deps]
        
        # Combine sorted tables with remaining tables
        result_tables = [table_map[table_id] for table_id in sorted_table_ids] + remaining_sorted
        
        logger.info(f"Dependency sorting with circular handling: {[table.name for table in result_tables]}")
        return result_tables
    
    # Return tables in dependency order
    return [table_map[table_id] for table_id in sorted_table_ids]

# Create blueprint for data modeling routes
data_modeling_bp = Blueprint('data_modeling', __name__, url_prefix='/api/data_modeling')


@data_modeling_bp.route('/project/<project_id>/apply_progress/<session_id>', methods=['GET'])
def apply_progress_stream(project_id: str, session_id: str):
    """Server-Sent Events endpoint for streaming apply progress"""
    logger.info(f"🔍 SSE apply progress requested for session: {session_id}")
    
    def generate():
        try:
            logger.info(f"🔍 SSE generator started for session: {session_id}")
            yield "data: {\"type\": \"connected\"}\n\n"
        except Exception as e:
            logger.error(f"❌ SSE generator error at start: {e}")
            yield f"data: {{\"type\": \"error\", \"message\": \"Generator start error: {str(e)}\"}}\n\n"
            return
        
        # Wait a bit for the session to be created by the apply_changes call
        import time
        max_wait = 30  # Wait up to 30 seconds for session to be created
        wait_interval = 0.5  # Check every 500ms
        waited = 0
        
        logger.info(f"🔍 Waiting for session {session_id} to be created...")
        while session_id not in progress_sessions and waited < max_wait:
            time.sleep(wait_interval)
            waited += wait_interval
            if waited % 5 == 0:  # Log every 5 seconds
                logger.info(f"🔍 Still waiting for session {session_id}... ({waited}s)")
        
        if session_id not in progress_sessions:
            logger.error(f"❌ Session {session_id} not found after {max_wait}s")
            yield f"data: {{\"type\": \"error\", \"message\": \"Session {session_id} not found after {max_wait}s\"}}\n\n"
            return
        
        logger.info(f"✅ Session {session_id} found, starting progress stream")
        # Now stream the updates
        for update in get_progress_updates(session_id):
            yield update
    
    try:
        logger.info(f"🔍 Creating SSE response for session: {session_id}")
        response = Response(generate(), mimetype='text/event-stream')
        response.headers['Cache-Control'] = 'no-cache'
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Headers'] = 'Cache-Control'
        response.headers['Access-Control-Allow-Methods'] = 'GET'
        response.headers['X-Accel-Buffering'] = 'no'  # Disable nginx buffering
        response.headers['Connection'] = 'keep-alive'
        response.headers['Keep-Alive'] = 'timeout=300'
        response.headers['Content-Type'] = 'text/event-stream; charset=utf-8'
        logger.info(f"✅ SSE response created for session: {session_id}")
        return response
    except Exception as e:
        logger.error(f"❌ Error creating SSE response for session {session_id}: {e}")
        return jsonify({'error': str(e)}), 500


# Create a directory for saved projects
SAVED_PROJECTS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "saved_projects")
if not os.path.exists(SAVED_PROJECTS_DIR):
    os.makedirs(SAVED_PROJECTS_DIR)


@data_modeling_bp.route('/warehouses', methods=['GET'])
def list_warehouses():
    """List available SQL warehouses"""
    try:
        client = get_sdk_client()
        if not client:
            logger.warning("No Databricks client available")
            response = jsonify([])
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response
        
        unity_service = DatabricksUnityService(client)
        warehouses = unity_service.get_available_warehouses()
        
        response = jsonify(warehouses)
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
        
    except Exception as e:
        logger.error(f"Error listing warehouses: {e}")
        response = jsonify({'error': str(e)})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 500

@data_modeling_bp.route('/catalogs', methods=['GET'])
def list_catalogs():
    """List all available Databricks catalogs"""
    try:
        # Use the client injected by the main app
        client = getattr(request, 'databricks_client', None)
        if client is None:
            # Return demo/mock data when credentials are not available
            demo_catalogs = [
                {
                    'name': 'main',
                    'comment': 'Default catalog (Demo Mode - Set DATABRICKS_HOST and DATABRICKS_TOKEN to connect)',
                    'created_at': None,
                    'updated_at': None,
                    'owner': 'demo_user',
                    'type': 'MANAGED_CATALOG'
                }
            ]
            response = jsonify(demo_catalogs)
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response
            
        unity_service = DatabricksUnityService(client)
        catalogs = unity_service.list_catalogs()
        
        response = jsonify(catalogs)
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
        
    except Exception as e:
        logger.error(f"Error listing catalogs: {e}")
        response = jsonify({'error': str(e)})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 500


@data_modeling_bp.route('/schemas/<catalog_name>', methods=['GET'])
def list_schemas(catalog_name: str):
    """List all schemas in a catalog"""
    try:
        # Use the client injected by the main app
        client = getattr(request, 'databricks_client', None)
        if client is None:
            # Return demo/mock data when credentials are not available
            demo_schemas = [
                {
                    'name': 'default',
                    'catalog_name': catalog_name,
                    'comment': 'Default schema (Demo Mode)',
                    'created_at': None,
                    'updated_at': None,
                    'owner': 'demo_user'
                },
                {
                    'name': 'sample_schema',
                    'catalog_name': catalog_name,
                    'comment': 'Sample schema for demonstration',
                    'created_at': None,
                    'updated_at': None,
                    'owner': 'demo_user'
                }
            ]
            response = jsonify(demo_schemas)
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response
            
        unity_service = DatabricksUnityService(client)
        schemas = unity_service.list_schemas(catalog_name)
        
        response = jsonify(schemas)
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
        
    except Exception as e:
        logger.error(f"Error listing schemas: {e}")
        response = jsonify({'error': str(e)})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 500


@data_modeling_bp.route('/tables/<catalog_name>/<schema_name>', methods=['GET'])
def list_tables(catalog_name: str, schema_name: str):
    """List all tables in a schema"""
    try:
        # Use the client injected by the main app
        client = getattr(request, 'databricks_client', None)
        if client is None:
            # Return demo/mock data when credentials are not available
            demo_tables = [
                {
                    'name': 'customers',
                    'catalog_name': catalog_name,
                    'schema_name': schema_name,
                    'table_type': 'MANAGED',
                    'comment': 'Sample customers table (Demo Mode)',
                    'created_at': None,
                    'updated_at': None,
                    'owner': 'demo_user',
                    'storage_location': None,
                    'data_source_format': 'DELTA'
                },
                {
                    'name': 'orders',
                    'catalog_name': catalog_name,
                    'schema_name': schema_name,
                    'table_type': 'MANAGED',
                    'comment': 'Sample orders table (Demo Mode)',
                    'created_at': None,
                    'updated_at': None,
                    'owner': 'demo_user',
                    'storage_location': None,
                    'data_source_format': 'DELTA'
                }
            ]
            response = jsonify(demo_tables)
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response
            
        unity_service = DatabricksUnityService(client)
        tables = unity_service.list_tables(catalog_name, schema_name)
        
        response = jsonify(tables)
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
        
    except Exception as e:
        logger.error(f"Error listing tables: {e}")
        response = jsonify({'error': str(e)})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 500


@data_modeling_bp.route('/views/<catalog_name>/<schema_name>', methods=['GET'])
def list_views(catalog_name: str, schema_name: str):
    """List all views in a schema"""
    try:
        logger.info(f"🔍 Views API called for {catalog_name}.{schema_name}")
        # Use the client injected by the main app first, fallback to get_sdk_client if needed
        client = getattr(request, 'databricks_client', None)
        if not client:
            logger.info("🔍 No injected client, trying get_sdk_client()")
            client = get_sdk_client()
            
        if not client:
            logger.error("❌ No SDK client available for views")
            response = jsonify({'error': 'Failed to connect to Databricks'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 500
        
        logger.info(f"🚀 Listing tables in {catalog_name}.{schema_name}")
        service = DatabricksUnityService(client)
        
        # Get all tables and filter for views
        all_tables_list = list(client.tables.list(catalog_name=catalog_name, schema_name=schema_name))
        logger.info(f"📊 Found {len(all_tables_list)} objects in {catalog_name}.{schema_name}")
        
        views = []
        for table_info in all_tables_list:
            # Check if it's a view (not a table)
            is_view = False
            view_type = "VIEW"
            
            if table_info.table_type and table_info.table_type.value in ['VIEW', 'MATERIALIZED_VIEW']:
                is_view = True
                view_type = table_info.table_type.value
            elif table_info.table_type is None:
                # For objects with None type, assume they might be views (like metric views)
                # This is a workaround for Databricks SDK not properly classifying metric views
                is_view = True
                view_type = "VIEW"  # Default to VIEW for metric views
            
            if is_view:
                # Handle datetime fields that might be timestamps or datetime objects
                created_at = None
                if table_info.created_at:
                    if hasattr(table_info.created_at, 'isoformat'):
                        created_at = table_info.created_at.isoformat()
                    else:
                        created_at = str(table_info.created_at)
                
                updated_at = None
                if table_info.updated_at:
                    if hasattr(table_info.updated_at, 'isoformat'):
                        updated_at = table_info.updated_at.isoformat()
                    else:
                        updated_at = str(table_info.updated_at)
                
                views.append({
                    'name': table_info.name,
                    'type': view_type,
                    'comment': table_info.comment,
                    'created_at': created_at,
                    'updated_at': updated_at
                })
        
        response = jsonify(views)
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
        
    except Exception as e:
        logger.error(f"Error listing views: {e}")
        response = jsonify({'error': str(e)})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 500


@data_modeling_bp.route('/import_existing_cross_catalog', methods=['POST'])
def import_existing_tables_cross_catalog():
    """Import existing tables from multiple catalogs/schemas with unified progress tracking"""
    try:
        data = request.get_json()
        table_groups = data.get('table_groups', [])  # Format: [{'catalog': 'cat1', 'schema': 'sch1', 'tables': ['t1', 't2']}, ...]
        session_id = data.get('session_id')
        
        if not table_groups:
            return jsonify({'error': 'No table groups provided'}), 400
        
        if not session_id:
            return jsonify({'error': 'Session ID is required'}), 400
        
        # Calculate totals
        total_tables = sum(len(group['tables']) for group in table_groups)
        total_groups = len(table_groups)
        
        # Initialize progress session (Queue object for SSE communication)
        progress_sessions[session_id] = Queue()
        
        # Store session metadata separately
        session_metadata = {
            'status': 'started',
            'total_tables': total_tables,
            'total_groups': total_groups,
            'completed_tables': 0,
            'completed_groups': 0,
            'current_group': 0,
            'results': []
        }
        
        # Send initial progress update
        send_progress_update(session_id, {
            'type': 'started',
            'total_tables': total_tables,
            'total_groups': total_groups,
            'table_groups': [{'catalog': g['catalog'], 'schema': g['schema'], 'table_count': len(g['tables'])} for g in table_groups]
        })
        
        # Initialize project structure to collect all imported tables
        client = get_sdk_client()
        unity_service = DatabricksUnityService(client)
        all_imported_tables = []
        all_imported_relationships = []
        # Track already imported table names to avoid duplicates
        imported_table_names = set()
        
        # Process each group
        for group_index, group in enumerate(table_groups):
            # Check for cancellation at group level
            with progress_lock:
                if import_cancellations.get(session_id, False):
                    send_progress_update(session_id, {
                        'type': 'cancelled',
                        'message': 'Import cancelled by user'
                    })
                    return jsonify({'error': 'Import cancelled by user'}), 200
            
            catalog_name = group['catalog']
            schema_name = group['schema']
            table_names = group['tables']
            
            session_metadata['current_group'] = group_index + 1
            
            # Send group start update
            send_progress_update(session_id, {
                'type': 'group_started',
                'group_number': group_index + 1,
                'total_groups': total_groups,
                'catalog_name': catalog_name,
                'schema_name': schema_name,
                'table_count': len(table_names),
                'table_names': table_names
            })
            
            try:
                # Individual table processing will send started messages as they are processed
                
                # Import the group with heartbeat updates to keep SSE connection alive
                import threading
                import time
                
                # Flag to stop heartbeat when import completes
                import_complete = threading.Event()
                
                def send_heartbeats():
                    """Send periodic heartbeat updates to keep SSE connection alive"""
                    heartbeat_count = 0
                    while not import_complete.is_set():
                        if heartbeat_count % 2 == 0:  # Every 2nd heartbeat (every 4 seconds)
                            send_progress_update(session_id, {
                                'type': 'heartbeat',
                                'message': f'Processing tables from {catalog_name}.{schema_name}...',
                                'group_number': group_index + 1,
                                'heartbeat_count': heartbeat_count
                            })
                        heartbeat_count += 1
                        # Wait 2 seconds between heartbeats or until import completes
                        if import_complete.wait(2):
                            break
                
                # Start heartbeat thread
                heartbeat_thread = threading.Thread(target=send_heartbeats)
                heartbeat_thread.daemon = True
                heartbeat_thread.start()
                
                try:
                    # Process tables individually to provide real-time progress
                    group_project = DataModelProject(
                        name=f"Cross-catalog import from {catalog_name}.{schema_name}",
                        description=f"Imported from {catalog_name}.{schema_name}",
                        catalog_name=catalog_name,
                        schema_name=schema_name,
                        tables=[],
                        relationships=[],
                        metric_views=[],
                        traditional_views=[],
                        metric_relationships=[]
                    )
                    
                    # Process each table individually for real-time progress
                    for i, table_name in enumerate(table_names):
                        # Check for cancellation
                        with progress_lock:
                            if import_cancellations.get(session_id, False):
                                send_progress_update(session_id, {
                                    'type': 'cancelled',
                                    'message': f'Import cancelled during {table_name} processing'
                                })
                                return jsonify({'error': 'Import cancelled by user'}), 200
                        
                        try:
                            # Send table started message
                            send_progress_update(session_id, {
                                'type': 'table_started',
                                'table_name': table_name,
                                'progress': int((i / len(table_names)) * 100)
                            })
                            
                            # Import single table
                            single_table_project = unity_service.import_existing_tables(
                                catalog_name, 
                                schema_name, 
                                [table_name]  # Single table
                            )
                            
                            # Add imported table to group project, filtering out duplicates
                            if single_table_project.tables:
                                # Filter out tables that have already been imported
                                new_tables = []
                                for table in single_table_project.tables:
                                    table_full_name = f"{table.catalog_name}.{table.schema_name}.{table.name}"
                                    if table_full_name not in imported_table_names:
                                        new_tables.append(table)
                                        imported_table_names.add(table_full_name)
                                        
                                # Add only new tables
                                group_project.tables.extend(new_tables)
                                
                                # Collect all relationships - we'll filter them after all tables are imported
                                # This allows cross-table relationships to be resolved properly
                                for relationship in single_table_project.relationships:
                                    # Check if this relationship already exists in our group
                                    relationship_exists = any(
                                        r.source_table_id == relationship.source_table_id and 
                                        r.target_table_id == relationship.target_table_id and
                                        r.source_field_id == relationship.source_field_id and
                                        r.target_field_id == relationship.target_field_id
                                        for r in group_project.relationships
                                    )
                                    if not relationship_exists:
                                        group_project.relationships.append(relationship)
                                
                                # Send individual table completed update - find the actual target table
                                target_table = next((t for t in single_table_project.tables if t.name == table_name), None)
                                if target_table:
                                    constraints_count = len([r for r in single_table_project.relationships if r.source_table_id == target_table.id or r.target_table_id == target_table.id])
                                    send_progress_update(session_id, {
                                        'type': 'table_completed',
                                        'table_name': table_name,
                                        'progress': int(((i + 1) / len(table_names)) * 100),
                                        'result': {
                                            'table_name': table_name,
                                            'success': True,
                                            'columns_count': len(target_table.fields),
                                            'constraints_count': constraints_count
                                        }
                                    })
                                else:
                                    # Fallback if target table not found
                                    send_progress_update(session_id, {
                                        'type': 'table_completed',
                                        'table_name': table_name,
                                        'progress': int(((i + 1) / len(table_names)) * 100),
                                        'result': {
                                            'table_name': table_name,
                                            'success': True,
                                            'columns_count': 0,
                                            'constraints_count': 0
                                        }
                                    })
                            else:
                                # Table import failed
                                send_progress_update(session_id, {
                                    'type': 'table_failed',
                                    'table_name': table_name,
                                    'error': 'Failed to import table'
                                })
                                
                        except Exception as table_error:
                            # Individual table failed
                            send_progress_update(session_id, {
                                'type': 'table_failed',
                                'table_name': table_name,
                                'error': str(table_error)
                            })
                            
                finally:
                    # Signal heartbeat thread to stop
                    import_complete.set()
                    heartbeat_thread.join(timeout=1)  # Wait up to 1 second for thread cleanup
                
                # Table completion messages are already sent in the individual processing loop above
                
                # Extract tables and relationships from the group project
                group_tables = group_project.tables or []
                group_relationships = group_project.relationships or []
                
                # Debug: Log what we're adding to avoid duplicates
                logger.info(f"Group {group_index + 1} - Adding {len(group_tables)} unique tables")
                for table in group_tables:
                    logger.info(f"  - {table.catalog_name}.{table.schema_name}.{table.name}")
                
                # Add to overall collections
                all_imported_tables.extend(group_tables)
                all_imported_relationships.extend(group_relationships)
                
                # Create summary results for this group
                group_results = []
                for table in group_tables:
                    constraints_count = len([r for r in group_relationships if r.source_table_id == table.id or r.target_table_id == table.id])
                    group_results.append({
                        'table_name': table.name,
                        'success': True,
                        'columns_count': len(table.fields),
                        'constraints_count': constraints_count
                    })
                
                session_metadata['completed_groups'] += 1
                session_metadata['completed_tables'] += len(group_tables)
                session_metadata['results'].append({
                    'catalog_name': catalog_name,
                    'schema_name': schema_name,
                    'tables': group_results
                })
                
                # Send group completed update
                send_progress_update(session_id, {
                    'type': 'group_completed',
                    'group_number': group_index + 1,
                    'catalog_name': catalog_name,
                    'schema_name': schema_name,
                    'results': group_results,
                    'overall_progress': (session_metadata['completed_groups'] / total_groups) * 100
                })
                
            except Exception as group_error:
                logger.error(f"Error processing group {catalog_name}.{schema_name}: {group_error}")
                session_metadata['results'].append({
                    'catalog_name': catalog_name,
                    'schema_name': schema_name,
                    'error': str(group_error)
                })
                
                send_progress_update(session_id, {
                    'type': 'group_failed',
                    'group_number': group_index + 1,
                    'catalog_name': catalog_name,
                    'schema_name': schema_name,
                    'error': str(group_error)
                })
        
        # Filter and remap relationships to only include those where both source and target tables exist
        logger.info(f"Filtering relationships: {len(all_imported_relationships)} total relationships")
        
        # Create a mapping of table full names to table objects for relationship remapping
        table_name_map = {}
        for table in all_imported_tables:
            full_name = f"{table.catalog_name}.{table.schema_name}.{table.name}"
            table_name_map[full_name] = table
            logger.info(f"📋 Table mapping: {full_name} -> {table.id}")
        
        # Also create a mapping by table ID for direct lookups (if IDs match)
        table_id_map = {table.id: table for table in all_imported_tables}
        
        # Filter and remap relationships where both source and target tables exist in our imported set
        filtered_relationships = []
        for relationship in all_imported_relationships:
            # First try direct ID lookup (for relationships within the same import batch)
            source_table = table_id_map.get(relationship.source_table_id)
            target_table = table_id_map.get(relationship.target_table_id)
            
            # If direct ID lookup fails, try to find tables by name from the original relationship context
            if not source_table or not target_table:
                # Log the original relationship for debugging
                logger.info(f"🔍 Remapping relationship: source_id={relationship.source_table_id}, target_id={relationship.target_table_id}")
                
                # For cross-catalog imports, we need to look up tables by name since IDs change between import batches
                # Find all tables that could be the source/target for this relationship
                potential_sources = []
                potential_targets = []
                
                for table in all_imported_tables:
                    # Check if this table has a field that matches the relationship
                    for field in table.fields:
                        if hasattr(relationship, 'source_field_id') and field.is_foreign_key and field.foreign_key_reference:
                            # This could be the source table (the one with FK)
                            potential_sources.append((table, field))
                        if hasattr(relationship, 'target_field_id') and field.is_primary_key:
                            # This could be the target table (the one being referenced)
                            potential_targets.append((table, field))
                
                # Try to match by constraint name or field name patterns
                for src_table, src_field in potential_sources:
                    for tgt_table, tgt_field in potential_targets:
                        # Check if this is a valid relationship match
                        if (src_field.foreign_key_reference and 
                            src_field.foreign_key_reference.constraint_name == relationship.constraint_name):
                            
                            logger.info(f"🔄 Remapping relationship: {src_table.name}.{src_field.name} -> {tgt_table.name}.{tgt_field.name}")
                            
                            # Create a new relationship with the correct table IDs
                            new_relationship = DataModelRelationship(
                                id=relationship.id,
                                source_table_id=src_table.id,
                                target_table_id=tgt_table.id,
                                source_field_id=src_field.id,
                                target_field_id=tgt_field.id,
                                relationship_type=relationship.relationship_type,
                                constraint_name=relationship.constraint_name,
                                fk_table_id=src_table.id,
                                fk_field_id=src_field.id,
                                line_points=relationship.line_points
                            )
                            
                            filtered_relationships.append(new_relationship)
                            logger.info(f"✅ Relationship remapped: {src_table.name} -> {tgt_table.name}")
                            break
                    else:
                        continue
                    break
            else:
                # Direct ID match worked, keep the relationship as-is
                filtered_relationships.append(relationship)
                logger.info(f"✅ Relationship kept (direct match): {source_table.name} -> {target_table.name}")
        
        logger.info(f"Relationship filtering complete: {len(filtered_relationships)}/{len(all_imported_relationships)} relationships kept")
        
        # Create a consolidated project with all imported tables and filtered relationships
        consolidated_project = DataModelProject(
            id=f"cross_catalog_import_{session_id}",
            name=f"Cross-Catalog Import {total_tables} tables",
            description=f"Imported {total_tables} tables from {total_groups} catalog/schema combinations",
            catalog_name="mixed_catalogs",
            schema_name="mixed_schemas",
            tables=all_imported_tables,
            relationships=filtered_relationships,
            metric_views=[],
            traditional_views=[],
            metric_relationships=[]
        )
        
        # Clean up cancellation flag
        with progress_lock:
            import_cancellations.pop(session_id, None)
        
        # Debug: Log final counts
        logger.info(f"Final import results: {len(all_imported_tables)} unique tables, {len(filtered_relationships)} relationships")
        for table in all_imported_tables:
            logger.info(f"  Final table: {table.catalog_name}.{table.schema_name}.{table.name}")
        
        # Send final completion update with project data
        send_progress_update(session_id, {
            'type': 'completed',
            'total_tables': total_tables,
            'imported_tables': len(all_imported_tables),  # Actual imported count (may be higher due to relationships)
            'total_groups': total_groups,
            'results': session_metadata['results'],
            'overall_progress': 100,
            'project_data': {
                'tables': [table.model_dump(mode='json') for table in all_imported_tables],
                'relationships': [rel.model_dump(mode='json') for rel in filtered_relationships]
            }
        })
        
        # Clean up session after a delay
        def cleanup_session():
            time.sleep(5)
            if session_id in progress_sessions:
                del progress_sessions[session_id]
        
        threading.Thread(target=cleanup_session).start()
        
        # Apply deduplication to the final table list with proper serialization
        table_dicts = [serialize_table_for_json(table) for table in all_imported_tables]
        deduplicated_tables = deduplicate_imported_tables(table_dicts)
        
        response = jsonify({
            'success': True,
            'session_id': session_id,
            'total_tables': total_tables,
            'total_groups': total_groups,
            'message': f'Cross-catalog import completed for {len(deduplicated_tables)} unique tables from {total_groups} catalog/schema combinations',
            'tables': deduplicated_tables,
            'relationships': [rel.dict() for rel in all_imported_relationships]
        })
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
        
    except Exception as e:
        logger.error(f"Cross-catalog import failed: {e}", exc_info=True)
        
        # Clean up cancellation flag on error
        with progress_lock:
            import_cancellations.pop(session_id, None)
        
        if session_id and session_id in progress_sessions:
            send_progress_update(session_id, {
                'type': 'error',
                'message': str(e)
            })
        response = jsonify({'error': str(e)})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 500

@data_modeling_bp.route('/import_existing', methods=['POST'])
def import_existing_tables():
    """Import existing tables from Databricks"""
    try:
        data = request.get_json()
        
        # Validate the request
        import_request = ExistingTableImport(**data)
        session_id = data.get('session_id', None)  # Optional session ID for progress streaming
        existing_tables = data.get('existing_tables', [])  # Tables already in the project
        
        logger.info(f"Import request: catalog={import_request.catalog_name}, schema={import_request.schema_name}, tables={import_request.table_names}")
        if existing_tables:
            logger.info(f"Processing {len(existing_tables)} existing tables for relationship creation")
        
        client = get_sdk_client()
        unity_service = DatabricksUnityService(client)
        
        if session_id:
            # Import with progress streaming
            project = unity_service.import_existing_tables_with_progress(
                import_request.catalog_name,
                import_request.schema_name,
                import_request.table_names,
                session_id,
                existing_tables  # Pass existing tables for relationship creation
            )
        else:
            # Import without progress streaming (legacy)
            project = unity_service.import_existing_tables(
                import_request.catalog_name,
                import_request.schema_name,
                import_request.table_names
            )
        
        # Filter out duplicate tables and create relationships with existing tables if provided
        if existing_tables and project.tables:
            logger.info(f"🔍 Filtering duplicates from {len(project.tables)} imported tables")
            
            # Convert existing_tables dict format to DataTable objects for relationship creation
            existing_table_objects = []
            existing_table_names = set()
            
            for table_data in existing_tables:
                try:
                    # Create DataTable object from dict
                    from models.data_modeling import DataTable
                    existing_table = DataTable(**table_data)
                    existing_table_objects.append(existing_table)
                    
                    # Also track names for duplicate filtering
                    table_catalog = table_data.get('catalog_name', import_request.catalog_name)
                    table_schema = table_data.get('schema_name', import_request.schema_name)
                    table_name = table_data.get('name')
                    if table_name:
                        full_name = f"{table_catalog}.{table_schema}.{table_name}"
                        existing_table_names.add(full_name)
                except Exception as e:
                    logger.warning(f"⚠️ Could not convert existing table data to DataTable: {e}")
                    # Still track the name for filtering
                    table_catalog = table_data.get('catalog_name', import_request.catalog_name)
                    table_schema = table_data.get('schema_name', import_request.schema_name)
                    table_name = table_data.get('name')
                    if table_name:
                        full_name = f"{table_catalog}.{table_schema}.{table_name}"
                        existing_table_names.add(full_name)
            
            logger.info(f"🔍 Existing tables to filter: {existing_table_names}")
            logger.info(f"🔍 Converted {len(existing_table_objects)} existing tables to DataTable objects")
            
            # Filter out tables that already exist
            filtered_tables = []
            for table in project.tables:
                table_full_name = f"{table.catalog_name}.{table.schema_name}.{table.name}"
                if table_full_name not in existing_table_names:
                    filtered_tables.append(table)
                else:
                    logger.info(f"⚡ Filtered out duplicate table: {table_full_name}")
            
            # Update project with filtered tables
            project.tables = filtered_tables
            logger.info(f"✅ Filtered import result: {len(filtered_tables)} unique tables")
            
            # Now create relationships between newly imported tables and existing tables
            if existing_table_objects and filtered_tables:
                logger.info(f"🔗 Creating relationships between {len(filtered_tables)} new tables and {len(existing_table_objects)} existing tables")
                
                # Combine existing and new tables for relationship creation
                all_tables_for_relationships = existing_table_objects + filtered_tables
                
                # Create table_id_map including both existing and new tables
                table_id_map = {}
                for table in all_tables_for_relationships:
                    table_catalog = table.catalog_name or import_request.catalog_name
                    table_schema = table.schema_name or import_request.schema_name
                    full_name = f"{table_catalog}.{table_schema}.{table.name}"
                    table_id_map[full_name] = table.id
                
                logger.info(f"🔍 Table ID map for relationships: {list(table_id_map.keys())}")
                
                # Create relationships for newly imported tables only (to avoid duplicating existing relationships)
                additional_relationships = []
                for table in filtered_tables:
                    table_catalog = table.catalog_name or import_request.catalog_name
                    table_schema = table.schema_name or import_request.schema_name
                    table_name = table.name
                    
                    constraints = unity_service.get_table_constraints(table_catalog, table_schema, table_name)
                    table_full_name = f"{table_catalog}.{table_schema}.{table_name}"
                    
                    relationships = unity_service._extract_relationships_from_constraints(
                        constraints, table_id_map, table_full_name, all_tables_for_relationships
                    )
                    additional_relationships.extend(relationships)
                
                # Add the additional relationships to the project
                project.relationships.extend(additional_relationships)
                logger.info(f"✅ Created {len(additional_relationships)} additional relationships with existing tables")
                
                # Log details of the relationships for debugging
                for rel in additional_relationships:
                    logger.info(f"🔗 New relationship: {rel.source_table_id} -> {rel.target_table_id} (ID: {rel.id})")
        
        response = jsonify(project.model_dump())
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
        
    except ValidationError as e:
        response = jsonify({'error': 'Invalid request data', 'validation_errors': e.errors()})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 422
    except Exception as e:
        logger.error(f"Error importing tables: {e}")
        response = jsonify({'error': str(e)})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 500


@data_modeling_bp.route('/cancel_import/<session_id>', methods=['POST'])
def cancel_import(session_id):
    """Cancel an ongoing import operation"""
    try:
        with progress_lock:
            import_cancellations[session_id] = True
            
        # Send cancellation message to the progress stream
        send_progress_update(session_id, {
            'type': 'cancelled',
            'message': 'Import cancelled by user'
        })
        
        return jsonify({'success': True, 'message': 'Import cancellation requested'}), 200
        
    except Exception as e:
        logger.error(f"Error cancelling import {session_id}: {str(e)}")
        return jsonify({'error': str(e)}), 500

@data_modeling_bp.route('/cancel_ddl/<session_id>', methods=['POST'])
def cancel_ddl(session_id):
    """Cancel an ongoing DDL generation operation"""
    try:
        with progress_lock:
            import_cancellations[session_id] = True  # Reuse the same cancellation dict
            
        # Send cancellation message (DDL operations can also use progress updates)
        send_progress_update(session_id, {
            'type': 'cancelled',
            'message': 'DDL generation cancelled by user'
        })
        
        return jsonify({'success': True, 'message': 'DDL generation cancellation requested'}), 200
        
    except Exception as e:
        logger.error(f"Error cancelling DDL generation {session_id}: {str(e)}")
        return jsonify({'error': str(e)}), 500

@data_modeling_bp.route('/cancel_apply/<session_id>', methods=['POST'])
def cancel_apply(session_id):
    """Cancel an ongoing apply operation"""
    try:
        with progress_lock:
            import_cancellations[session_id] = True  # Reuse the same cancellation dict
            
        # Send cancellation message
        send_progress_update(session_id, {
            'type': 'cancelled',
            'message': 'Apply operation cancelled by user'
        })
        
        return jsonify({'success': True, 'message': 'Apply operation cancellation requested'}), 200
        
    except Exception as e:
        logger.error(f"Error cancelling apply operation {session_id}: {str(e)}")
        return jsonify({'error': str(e)}), 500


@data_modeling_bp.route('/import_progress/<session_id>', methods=['GET'])
def import_progress_stream(session_id: str):
    """Stream import progress updates via Server-Sent Events"""
    logger.info(f"🔍 SSE import progress requested for session: {session_id}")
    
    def generate():
        try:
            logger.info(f"🔍 SSE generator started for session: {session_id}")
            yield "data: {\"type\": \"connected\"}\n\n"
        except Exception as e:
            logger.error(f"❌ SSE generator error at start: {e}")
            yield f"data: {{\"type\": \"error\", \"message\": \"Generator start error: {str(e)}\"}}\n\n"
            return
        
        # Wait a bit for the session to be created by the import call
        import time
        max_wait = 30  # Wait up to 30 seconds for session to be created
        wait_interval = 0.5  # Check every 500ms
        waited = 0
        
        logger.info(f"🔍 Waiting for session {session_id} to be created...")
        while session_id not in progress_sessions and waited < max_wait:
            time.sleep(wait_interval)
            waited += wait_interval
            if waited % 5 == 0:  # Log every 5 seconds
                logger.info(f"🔍 Still waiting for session {session_id}... ({waited}s)")
        
        if session_id not in progress_sessions:
            logger.error(f"❌ Session {session_id} not found after {max_wait}s")
            yield f"data: {{\"type\": \"error\", \"message\": \"Session {session_id} not found after {max_wait}s\"}}\n\n"
            return
        
        logger.info(f"✅ Session {session_id} found, starting progress stream")
        # Now stream the updates using the same system as apply changes
        for update in get_progress_updates(session_id):
            yield update
    
    try:
        logger.info(f"🔍 Creating SSE response for session: {session_id}")
        response = Response(generate(), mimetype='text/event-stream')
        response.headers['Cache-Control'] = 'no-cache'
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Headers'] = 'Cache-Control'
        response.headers['Access-Control-Allow-Methods'] = 'GET'
        response.headers['X-Accel-Buffering'] = 'no'  # Disable nginx buffering
        response.headers['Connection'] = 'keep-alive'
        response.headers['Keep-Alive'] = 'timeout=300'
        response.headers['Content-Type'] = 'text/event-stream; charset=utf-8'
        logger.info(f"✅ SSE response created for session: {session_id}")
        return response
    except Exception as e:
        logger.error(f"❌ Error creating SSE response for session {session_id}: {e}")
        return jsonify({'error': str(e)}), 500


@data_modeling_bp.route('/import_views', methods=['POST'])
def import_existing_views():
    """Import existing views from Databricks with automatic dependency detection"""
    logger.info("🔍 DEBUG: import_existing_views called!")
    print("🔍 DEBUG: import_existing_views called!")
    try:
        data = request.get_json()
        catalog_name = data.get('catalog_name')
        schema_name = data.get('schema_name')
        view_names = data.get('view_names', [])
        auto_import_dependencies = data.get('auto_import_dependencies', True)
        existing_tables = data.get('existing_tables', [])
        
        logger.info(f"🔍 DEBUG: Request data - catalog: {catalog_name}, schema: {schema_name}")
        logger.info(f"🔍 DEBUG: Request data - view_names: {view_names}")
        logger.info(f"🔍 DEBUG: Request data - auto_import_dependencies: {auto_import_dependencies}")
        logger.info(f"🔍 DEBUG: Request data - existing_tables count: {len(existing_tables)}")  # Tables already in the project
        
        if not catalog_name or not schema_name:
            response = jsonify({'error': 'catalog_name and schema_name are required'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 400
        
        if not view_names:
            response = jsonify({'error': 'view_names list is required'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 400
        
        # Get Databricks client
        client = get_sdk_client()
        if not client:
            response = jsonify({'error': 'Failed to connect to Databricks'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 500
        
        service = DatabricksUnityService(client)
        
        # Import views
        imported_views = service.import_existing_views(catalog_name, schema_name, view_names)
        
        # Auto-import referenced tables if requested
        imported_tables = []
        if auto_import_dependencies:
            all_referenced_tables = set()
            for view in imported_views:
                # Handle different view types
                table_refs = []
                
                if isinstance(view, TraditionalView) and hasattr(view, 'referenced_table_names'):
                    # Traditional views have referenced_table_names
                    table_refs = view.referenced_table_names
                elif isinstance(view, MetricView) and hasattr(view, 'source_table_id'):
                    # Metric views have source_table_id
                    table_refs = [view.source_table_id]
                    # Also check joins for additional table references (recursively for nested joins)
                    if hasattr(view, 'joins') and view.joins:
                        def extract_join_table_refs_recursive(joins):
                            refs = []
                            for i, join in enumerate(joins):
                                joins_attr = getattr(join, 'joins', None)
                                joins_len = len(joins_attr) if joins_attr else 0
                                logger.info(f"🔍 Processing join {i}: name={getattr(join, 'name', 'unknown')}, has_joins={hasattr(join, 'joins')}, joins_len={joins_len}")
                                # Try joined_table_name first (full table reference)
                                if hasattr(join, 'joined_table_name') and join.joined_table_name:
                                    refs.append(join.joined_table_name)
                                    logger.info(f"🔗 Found join table: {join.joined_table_name}")
                                # If not available, try to resolve joined_table_id to table name
                                elif hasattr(join, 'joined_table_id') and join.joined_table_id:
                                    # Try to resolve table ID to table name using common patterns
                                    table_id = join.joined_table_id
                                    logger.info(f"🔍 Found join with table ID: {table_id}")
                                    
                                    # Extract table name from ID patterns like "orders-table-002" -> "orders"
                                    if '-table-' in table_id:
                                        table_name = table_id.split('-table-')[0]
                                        full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
                                        refs.append(full_table_name)
                                        logger.info(f"🔗 Resolved table ID {table_id} -> {full_table_name}")
                                    else:
                                        # If it's already a full name, use it directly
                                        refs.append(table_id)
                                
                                # Recursively process nested joins
                                if hasattr(join, 'joins') and join.joins:
                                    nested_refs = extract_join_table_refs_recursive(join.joins)
                                    refs.extend(nested_refs)
                            return refs
                        
                        table_refs.extend(extract_join_table_refs_recursive(view.joins))
                
                # Parse table references
                for table_ref in table_refs:
                    if not table_ref:
                        continue
                        
                    # Parse table reference to extract catalog.schema.table
                    parts = table_ref.split('.')
                    if len(parts) == 3:
                        ref_catalog, ref_schema, ref_table = parts
                        all_referenced_tables.add((ref_catalog, ref_schema, ref_table))
                    elif len(parts) == 2:
                        # Assume same catalog
                        ref_schema, ref_table = parts
                        all_referenced_tables.add((catalog_name, ref_schema, ref_table))
                    elif len(parts) == 1:
                        # Assume same catalog and schema
                        ref_table = parts[0]
                        all_referenced_tables.add((catalog_name, schema_name, ref_table))
                        
            logger.info(f"🔍 Found {len(all_referenced_tables)} referenced tables to import: {list(all_referenced_tables)}")
            
            # Create a set of existing table names for quick lookup
            existing_table_names = set()
            for table in existing_tables:
                table_catalog = table.get('catalog_name', catalog_name)
                table_schema = table.get('schema_name', schema_name)
                table_name = table.get('name')
                if table_name:
                    full_name = f"{table_catalog}.{table_schema}.{table_name}"
                    existing_table_names.add(full_name)
            
            logger.info(f"🔍 Existing tables in project: {existing_table_names}")
            
            # Import all referenced tables in one batch to avoid duplicates from FK following
            if all_referenced_tables:
                # Group tables by catalog/schema for efficient batch import
                catalog_schema_groups = {}
                for ref_catalog, ref_schema, ref_table in all_referenced_tables:
                    full_ref_name = f"{ref_catalog}.{ref_schema}.{ref_table}"
                    
                    logger.info(f"🔍 DEBUG: Checking if {full_ref_name} should be imported...")
                    logger.info(f"🔍 DEBUG: Exists in project? {full_ref_name in existing_table_names}")
                    
                    if full_ref_name in existing_table_names:
                        logger.info(f"⚡ Table {full_ref_name} already exists in project, skipping import")
                        continue
                    
                    # Group by catalog.schema
                    group_key = f"{ref_catalog}.{ref_schema}"
                    if group_key not in catalog_schema_groups:
                        catalog_schema_groups[group_key] = {
                            'catalog': ref_catalog,
                            'schema': ref_schema,
                            'tables': []
                        }
                    catalog_schema_groups[group_key]['tables'].append(ref_table)
                
                # Import each group in batch to avoid FK following duplicates
                for group_key, group_info in catalog_schema_groups.items():
                    try:
                        logger.info(f"🔄 Batch importing {len(group_info['tables'])} tables from {group_key}: {group_info['tables']}")
                        table_import_result = service.import_existing_tables(
                            group_info['catalog'], group_info['schema'], group_info['tables']
                        )
                        logger.info(f"📋 Table import result: {table_import_result}")
                        if table_import_result and hasattr(table_import_result, 'tables') and table_import_result.tables:
                            # Convert DataTable objects to dictionaries for JSON serialization with proper datetime handling
                            for table in table_import_result.tables:
                                imported_tables.append(serialize_table_for_json(table))
                            logger.info(f"✅ Successfully imported {len(table_import_result.tables)} tables from {group_key}")
                        else:
                            logger.warning(f"⚠️ No tables returned from import for {group_key}")
                    except Exception as e:
                        logger.error(f"❌ Could not import tables from {group_key}: {e}")
                        import traceback
                        logger.error(f"❌ Full traceback: {traceback.format_exc()}")
        
        # Deduplicate imported tables (FK following can cause duplicates)
        imported_tables = deduplicate_imported_tables(imported_tables)
        
        # Create table-to-table relationships between imported tables
        logger.info(f"🔍 DEBUG: imported_tables count: {len(imported_tables)}")
        logger.info(f"🔍 DEBUG: existing_tables count: {len(existing_tables)}")
        
        if imported_tables:
            logger.info(f"🔗 Creating table-to-table relationships between {len(imported_tables)} imported tables")
            
            # Convert imported_tables (dicts) back to DataTable objects for relationship creation
            imported_table_objects = []
            for table_dict in imported_tables:
                try:
                    # Transform type_parameters if needed
                    for field_data in table_dict.get('fields', []):
                        if 'type_parameters' in field_data and isinstance(field_data['type_parameters'], dict):
                            field_data['type_parameters'] = str(field_data['type_parameters'])
                    
                    from models.data_modeling import DataTable
                    imported_table = DataTable(**table_dict)
                    imported_table_objects.append(imported_table)
                except Exception as e:
                    logger.warning(f"⚠️ Could not convert imported table to DataTable: {e}")
            
            # Convert existing_tables to DataTable objects for relationship creation
            existing_table_objects = []
            if existing_tables:
                for table_data in existing_tables:
                    try:
                        # Transform type_parameters if needed
                        for field_data in table_data.get('fields', []):
                            if 'type_parameters' in field_data and isinstance(field_data['type_parameters'], dict):
                                field_data['type_parameters'] = str(field_data['type_parameters'])
                        
                        existing_table = DataTable(**table_data)
                        existing_table_objects.append(existing_table)
                    except Exception as e:
                        logger.warning(f"⚠️ Could not convert existing table to DataTable: {e}")
            
            # Combine existing and imported tables for relationship creation
            all_tables_for_relationships = existing_table_objects + imported_table_objects
            
            # Create table_id_map including both existing and imported tables
            table_id_map = {}
            for table in all_tables_for_relationships:
                table_catalog = table.catalog_name or catalog_name
                table_schema = table.schema_name or schema_name
                full_name = f"{table_catalog}.{table_schema}.{table.name}"
                table_id_map[full_name] = table.id
            
            logger.info(f"🔍 Table ID map for relationships: {list(table_id_map.keys())}")
            
            # Create relationships for newly imported tables
            table_to_table_relationships = []
            logger.info(f"🔗 Starting relationship creation for {len(imported_table_objects)} imported tables")
            
            for i, table in enumerate(imported_table_objects):
                table_catalog = table.catalog_name or catalog_name
                table_schema = table.schema_name or schema_name
                table_name = table.name
                
                logger.info(f"🔍 Processing table {i+1}/{len(imported_table_objects)}: {table_catalog}.{table_schema}.{table_name}")
                
                constraints = service.get_table_constraints(table_catalog, table_schema, table_name)
                logger.info(f"🔍 Found {len(constraints) if constraints else 0} constraints for {table_name}")
                
                table_full_name = f"{table_catalog}.{table_schema}.{table_name}"
                
                relationships = service._extract_relationships_from_constraints(
                    constraints, table_id_map, table_full_name, all_tables_for_relationships
                )
                logger.info(f"🔍 Extracted {len(relationships)} relationships for {table_name}")
                table_to_table_relationships.extend(relationships)
            
            logger.info(f"✅ Created {len(table_to_table_relationships)} table-to-table relationships between imported tables")
        else:
            table_to_table_relationships = []
        
        # Create relationships between views and their referenced tables
        view_relationships = []
        logger.info(f"🔍 DEBUG: imported_tables count: {len(imported_tables) if imported_tables else 0}")
        logger.info(f"🔍 DEBUG: existing_tables count: {len(existing_tables) if existing_tables else 0}")
        logger.info(f"🔍 DEBUG: imported_views count: {len(imported_views)}")
        
        # Check if we have any tables (imported or existing) to create relationships with
        if imported_tables or existing_tables:
            total_tables = len(imported_tables) + len(existing_tables)
            logger.info(f"🔗 Creating relationships between {len(imported_views)} views and {total_tables} total tables ({len(imported_tables)} imported + {len(existing_tables)} existing)")
            
            # Build a map of table names to table IDs for quick lookup
            table_name_to_id = {}
            for table_dict in imported_tables:
                table_catalog = table_dict.get('catalog_name', catalog_name)
                table_schema = table_dict.get('schema_name', schema_name)
                table_name = table_dict.get('name')
                if table_name:
                    full_name = f"{table_catalog}.{table_schema}.{table_name}"
                    table_name_to_id[full_name] = table_dict.get('id')
            
            # Also include existing tables in the lookup
            for table in existing_tables:
                table_catalog = table.get('catalog_name', catalog_name)
                table_schema = table.get('schema_name', schema_name)
                table_name = table.get('name')
                if table_name:
                    full_name = f"{table_catalog}.{table_schema}.{table_name}"
                    table_name_to_id[full_name] = table.get('id')
            
            logger.info(f"🔍 Table name to ID map: {table_name_to_id}")
            
            # Create relationships for each view
            for view in imported_views:
                if isinstance(view, TraditionalView) and hasattr(view, 'referenced_table_names'):
                    # Traditional view relationships
                    for table_ref in view.referenced_table_names:
                        if not table_ref:
                            continue
                            
                        # Parse table reference to get full name
                        parts = table_ref.split('.')
                        if len(parts) == 3:
                            full_table_name = table_ref
                        elif len(parts) == 2:
                            full_table_name = f"{catalog_name}.{table_ref}"
                        elif len(parts) == 1:
                            full_table_name = f"{catalog_name}.{schema_name}.{table_ref}"
                        else:
                            continue
                        
                        # Find the table ID
                        table_id = table_name_to_id.get(full_table_name)
                        if table_id:
                            # Create a view-to-table relationship
                            relationship = {
                                'id': str(uuid.uuid4()),
                                'source_table_id': view.id,  # View is the source
                                'target_table_id': table_id,  # Table is the target
                                'relationship_type': 'view_to_table',
                                'source_field_id': None,  # Views don't have specific fields for relationships
                                'target_field_id': None,   # We don't know which specific field is referenced
                                'constraint_name': f"view_{view.name}_references_{table_ref.split('.')[-1]}",
                                'on_delete': 'NO ACTION',
                                'on_update': 'NO ACTION'
                            }
                            view_relationships.append(relationship)
                            logger.info(f"✅ Created view-to-table relationship: {view.name} -> {full_table_name}")
                        else:
                            logger.warning(f"⚠️ Could not find table ID for {full_table_name} referenced by view {view.name}")
                
                elif isinstance(view, MetricView):
                    # Metric view relationships - source table and joins
                    table_refs = []
                    
                    # Add source table reference
                    if hasattr(view, 'source_table_id') and view.source_table_id:
                        # Check if source_table_id is a UUID (table ID) or full table reference
                        if '.' in view.source_table_id and len(view.source_table_id.split('.')) == 3:
                            # It's a full table reference like "carrossoni.corp_vendas.fact_vendas"
                            table_refs.append(view.source_table_id)
                        else:
                            # It's a UUID - try to find the table by ID
                            source_table_id = None
                            for table_dict in imported_tables:
                                if table_dict.get('id') == view.source_table_id:
                                    table_catalog = table_dict.get('catalog_name', catalog_name)
                                    table_schema = table_dict.get('schema_name', schema_name)
                                    table_name = table_dict.get('name')
                                    if table_name:
                                        table_refs.append(f"{table_catalog}.{table_schema}.{table_name}")
                                    break
                    
                    # Add join table references (recursively for nested joins)
                    if hasattr(view, 'joins') and view.joins:
                        def extract_join_table_refs(joins):
                            refs = []
                            for join in joins:
                                if hasattr(join, 'joined_table_name') and join.joined_table_name:
                                    refs.append(join.joined_table_name)
                                # Recursively process nested joins
                                if hasattr(join, 'joins') and join.joins:
                                    refs.extend(extract_join_table_refs(join.joins))
                            return refs
                        
                        table_refs.extend(extract_join_table_refs(view.joins))
                    
                    logger.info(f"🔍 Metric view {view.name} references tables: {table_refs}")
                    
                    # Create relationships for all referenced tables
                    for table_ref in table_refs:
                        if not table_ref:
                            continue
                            
                        # Parse table reference to get full name
                        parts = table_ref.split('.')
                        if len(parts) == 3:
                            full_table_name = table_ref
                        elif len(parts) == 2:
                            full_table_name = f"{catalog_name}.{table_ref}"
                        elif len(parts) == 1:
                            full_table_name = f"{catalog_name}.{schema_name}.{table_ref}"
                        else:
                            continue
                        
                        # Find the table ID
                        table_id = table_name_to_id.get(full_table_name)
                        if table_id:
                            # Create a metric-view-to-table relationship
                            relationship = {
                                'id': str(uuid.uuid4()),
                                'source_table_id': view.id,  # Metric view is the source
                                'target_table_id': table_id,  # Table is the target
                                'relationship_type': 'metric_view_to_table',
                                'source_field_id': None,  # Metric views don't have specific fields for relationships
                                'target_field_id': None,   # We don't know which specific field is referenced
                                'constraint_name': f"metric_view_{view.name}_references_{table_ref.split('.')[-1]}",
                                'on_delete': 'NO ACTION',
                                'on_update': 'NO ACTION'
                            }
                            view_relationships.append(relationship)
                            logger.info(f"✅ Created metric-view-to-table relationship: {view.name} -> {full_table_name}")
                        else:
                            logger.warning(f"⚠️ Could not find table ID for {full_table_name} referenced by metric view {view.name}")
            
            logger.info(f"✅ Created {len(view_relationships)} view-to-table relationships")
        
        # Convert views to dict format for JSON response
        views_data = []
        for view in imported_views:
            if isinstance(view, TraditionalView):
                views_data.append({
                    'id': view.id,
                    'name': view.name,
                    'type': 'traditional',
                    'description': view.description,
                    'sql_query': view.sql_query,
                    'catalog_name': view.catalog_name,
                    'schema_name': view.schema_name,
                    'referenced_table_names': view.referenced_table_names,
                    'tags': view.tags,
                    'logical_name': view.logical_name,
                    'position_x': view.position_x,
                    'position_y': view.position_y,
                    'width': view.width,
                    'height': view.height
                })
            elif isinstance(view, MetricView):
                # CRITICAL FIX: Update source_table_id to reference the imported table's ID
                source_table_id = view.source_table_id
                
                # Try to find the imported table that matches the source table name
                if imported_tables:
                    source_table_name = view.source_table_id
                    # Extract just the table name from full name like "carrossoni.tpch.customer" -> "customer"
                    if '.' in source_table_name:
                        table_name_only = source_table_name.split('.')[-1]
                    else:
                        table_name_only = source_table_name
                    
                    logger.info(f"🔍 DEBUG: Looking for source table '{table_name_only}' for metric view '{view.name}'")
                    logger.info(f"🔍 DEBUG: Available imported tables: {[getattr(t, 'name', 'NO_NAME') for t in imported_tables]}")
                    
                    # Find matching imported table
                    for imported_table in imported_tables:
                        table_name = imported_table.get('name') if hasattr(imported_table, 'get') else getattr(imported_table, 'name', None)
                        
                        if table_name == table_name_only:
                            table_id = imported_table.get('id') if hasattr(imported_table, 'get') else getattr(imported_table, 'id', None)
                            source_table_id = table_id
                            logger.info(f"🔗 Updated metric view {view.name} source_table_id: {view.source_table_id} -> {source_table_id}")
                            logger.info(f"🔍 DEBUG: Found matching table - name: {table_name}, id: {table_id}")
                            break
                
                views_data.append({
                    'id': view.id,
                    'name': view.name,
                    'type': 'metric',
                    'description': view.description,
                    'version': view.version,
                    'source_table_id': source_table_id,  # Use the updated ID
                    'catalog_name': view.catalog_name,  # Include metric view's catalog
                    'schema_name': view.schema_name,    # Include metric view's schema
                    'dimensions': [{'id': d.id, 'name': d.name, 'expr': d.expr} for d in view.dimensions],
                    'measures': [{'id': m.id, 'name': m.name, 'expr': m.expr, 'aggregation_type': m.aggregation_type} for m in view.measures],
                    'joins': [serialize_join(j) for j in view.joins],
                    'tags': view.tags,
                    'position_x': view.position_x,
                    'position_y': view.position_y,
                    'width': view.width,
                    'height': view.height
                })
        
        # Convert table-to-table relationships to dictionaries for JSON serialization
        table_relationships_data = []
        for rel in table_to_table_relationships:
            if hasattr(rel, 'model_dump'):
                # It's a Pydantic model, convert to dict
                table_relationships_data.append(rel.model_dump())
            else:
                # It's already a dict
                table_relationships_data.append(rel)
        
        result = {
            'views': views_data,
            'referenced_tables': imported_tables,
            'relationships': view_relationships + table_relationships_data,  # Add both view-to-table and table-to-table relationships
            'catalog_name': catalog_name,
            'schema_name': schema_name,
            'debug_info': {
                'auto_import_dependencies': auto_import_dependencies,
                'found_referenced_tables': list(all_referenced_tables) if auto_import_dependencies else [],
                'imported_views_count': len(imported_views),
                'imported_tables_count': len(imported_tables),
                'created_view_relationships_count': len(view_relationships),
                'created_table_relationships_count': len(table_to_table_relationships)
            }
        }
        
        response = jsonify(result)
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
        
    except Exception as e:
        logger.error(f"Error importing views: {e}")
        response = jsonify({'error': str(e)})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 500

@data_modeling_bp.route('/test_metric_view_parsing', methods=['POST'])
def test_metric_view_parsing():
    """Test endpoint to directly parse a specific metric view and see what's happening"""
    try:
        data = request.get_json()
        catalog_name = data.get('catalog_name', 'carrossoni')
        schema_name = data.get('schema_name', 'corp_views_metric')
        view_name = data.get('view_name', 'mvw_vendas')
        yaml_content = data.get('yaml_content')  # Allow direct YAML content
        
        logger.info(f"🔍 Testing metric view parsing for: {catalog_name}.{schema_name}.{view_name}")
        
        # Get Databricks client
        client = get_sdk_client()
        if not client:
            response = jsonify({'error': 'Failed to connect to Databricks'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 500
        
        service = DatabricksUnityService(client)
        
        # Step 1: Get view definition (or use provided YAML content)
        if yaml_content:
            logger.info("🔍 Step 1: Using provided YAML content...")
            view_definition = yaml_content
        else:
            logger.info("🔍 Step 1: Getting view definition from Databricks...")
            view_definition = service._get_view_definition(catalog_name, schema_name, view_name)
            
            if not view_definition:
                response = jsonify({'error': f'Could not retrieve view definition for {view_name}'})
                response.headers.add('Access-Control-Allow-Origin', '*')
                return response, 404
        
        logger.info(f"🔍 Retrieved view definition (length: {len(view_definition)})")
        logger.info(f"🔍 First 500 chars: {view_definition[:500]}...")
        logger.info(f"🔍 Last 200 chars: ...{view_definition[-200:]}")
        
        # Step 2: Detect view type
        logger.info("🔍 Step 2: Detecting view type...")
        view_type = service._detect_view_type(view_definition)
        logger.info(f"🔍 Detected view type: {view_type}")
        
        # Step 3: Check for metric view patterns
        logger.info("🔍 Step 3: Checking for metric view patterns...")
        has_as_dollar = "AS $$" in view_definition
        has_yaml_content = "version:" in view_definition or "source:" in view_definition
        logger.info(f"🔍 Has 'AS $$' pattern: {has_as_dollar}")
        logger.info(f"🔍 Has YAML content: {has_yaml_content}")
        
        # Step 4: Try parsing as metric view regardless of detection
        logger.info("🔍 Step 4: Attempting to parse as metric view...")
        try:
            metric_view = service._parse_metric_view(view_name, view_definition, catalog_name, schema_name, 0)
            if metric_view:
                logger.info(f"✅ Successfully parsed as metric view with {len(metric_view.joins)} joins")
                def serialize_join_recursive(join, index):
                    """Recursively serialize a join and its nested joins"""
                    join_info = {
                        'index': index,
                        'name': join.name,
                        'sql_on': join.sql_on,
                        'joined_table_name': join.joined_table_name,
                        'join_type': join.join_type,
                        'left_columns': join.left_columns,
                        'right_columns': join.right_columns,
                        'join_operators': join.join_operators,
                        'using': join.using
                    }
                    
                    # Add nested joins (always include the field, even if empty)
                    join_info['nested_joins'] = []
                    if join.joins and len(join.joins) > 0:
                        for nested_i, nested_join in enumerate(join.joins):
                            join_info['nested_joins'].append(serialize_join_recursive(nested_join, nested_i))
                    
                    return join_info
                
                joins_info = []
                for i, join in enumerate(metric_view.joins):
                    joins_info.append(serialize_join_recursive(join, i))
                logger.info(f"🔍 Joins: {joins_info}")
            else:
                logger.warning("⚠️ Failed to parse as metric view")
        except Exception as parse_error:
            logger.error(f"❌ Error parsing as metric view: {parse_error}")
            metric_view = None
        
        # Step 5: Try parsing as traditional view
        logger.info("🔍 Step 5: Attempting to parse as traditional view...")
        try:
            traditional_view = service._parse_traditional_view(view_name, view_definition, catalog_name, schema_name, 0)
            if traditional_view:
                logger.info(f"✅ Successfully parsed as traditional view")
            else:
                logger.warning("⚠️ Failed to parse as traditional view")
        except Exception as parse_error:
            logger.error(f"❌ Error parsing as traditional view: {parse_error}")
            traditional_view = None
        
        # Return analysis results
        result = {
            'view_name': f"{catalog_name}.{schema_name}.{view_name}",
            'view_definition_length': len(view_definition),
            'view_definition_preview': view_definition[:500] + "..." if len(view_definition) > 500 else view_definition,
            'detected_view_type': view_type,
            'has_as_dollar_pattern': has_as_dollar,
            'has_yaml_content': has_yaml_content,
            'metric_view_parsing': {
                'success': metric_view is not None,
                'joins_count': len(metric_view.joins) if metric_view else 0,
                'joins': joins_info if metric_view else []
            },
            'traditional_view_parsing': {
                'success': traditional_view is not None
            }
        }
        
        response = jsonify(result)
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
        
    except Exception as e:
        logger.error(f"Error in test_metric_view_parsing: {e}")
        response = jsonify({'error': str(e)})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 500

@data_modeling_bp.route('/test_import_with_joins', methods=['POST'])
def test_import_with_joins():
    """Test endpoint to simulate importing a metric view with joins"""
    try:
        from models.data_modeling import MetricView, MetricViewJoin, MetricViewDimension, MetricViewMeasure
        
        # Create a test metric view with joins (simulating what should come from Databricks)
        joins = [
            MetricViewJoin(
                name="orders",
                sql_on="o_orderkey = l_orderkey",
                join_type="INNER",
                joined_table_name="carrossoni.tpch.orders"
            ),
            MetricViewJoin(
                name="customer", 
                sql_on="orders.o_custkey = customer.c_custkey",
                join_type="LEFT",
                joined_table_name="carrossoni.tpch.customer"
            )
        ]
        
        test_metric_view = MetricView(
            name="test_advanced_orders_analytics",
            source_table_id="carrossoni.tpch.lineitem",
            joins=joins,
            dimensions=[
                MetricViewDimension(name="order_date", expr="orders.o_orderdate")
            ],
            measures=[
                MetricViewMeasure(name="total_revenue", expr="SUM(orders.o_totalprice)")
            ]
        )
        
        # Test the auto-import logic
        imported_views = [test_metric_view]
        catalog_name = "carrossoni"
        schema_name = "tpch"
        auto_import_dependencies = True
        
        # Run the same logic as import_views
        imported_tables = []
        if auto_import_dependencies:
            all_referenced_tables = set()
            for view in imported_views:
                table_refs = []
                
                if isinstance(view, MetricView) and hasattr(view, 'source_table_id'):
                    table_refs = [view.source_table_id]
                    if hasattr(view, 'joins') and view.joins:
                        # Recursively extract table references from nested joins
                        def extract_join_table_refs(joins):
                            refs = []
                            for join in joins:
                                if hasattr(join, 'joined_table_name') and join.joined_table_name:
                                    refs.append(join.joined_table_name)
                                # Recursively process nested joins
                                if hasattr(join, 'joins') and join.joins:
                                    refs.extend(extract_join_table_refs(join.joins))
                            return refs
                        
                        table_refs.extend(extract_join_table_refs(view.joins))
                
                for table_ref in table_refs:
                    if not table_ref:
                        continue
                    parts = table_ref.split('.')
                    if len(parts) == 3:
                        ref_catalog, ref_schema, ref_table = parts
                        all_referenced_tables.add((ref_catalog, ref_schema, ref_table))
            
            # Import referenced tables
            client = get_sdk_client()
            service = DatabricksUnityService(client)
            
            for ref_catalog, ref_schema, ref_table in all_referenced_tables:
                try:
                    table_import_result = service.import_existing_tables(
                        ref_catalog, ref_schema, [ref_table]
                    )
                    if table_import_result and hasattr(table_import_result, 'tables') and table_import_result.tables:
                        for table in table_import_result.tables:
                            imported_tables.append(table.model_dump())
                except Exception as e:
                    logger.error(f"❌ Could not import table {ref_catalog}.{ref_schema}.{ref_table}: {e}")
        
        result = {
            'test_metric_view': test_metric_view.model_dump(),
            'imported_tables': imported_tables,
            'referenced_tables_found': len(all_referenced_tables),
            'tables_imported': len(imported_tables)
        }
        
        response = jsonify(result)
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
        
    except Exception as e:
        logger.error(f"Error in test import: {e}")
        response = jsonify({'error': str(e)})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 500

@data_modeling_bp.route('/debug_view_yaml', methods=['POST'])
def debug_view_yaml():
    """Debug endpoint to see raw YAML from Databricks"""
    try:
        data = request.get_json()
        catalog_name = data.get('catalog_name')
        schema_name = data.get('schema_name')
        view_name = data.get('view_name')
        
        # Get Databricks client
        client = get_databricks_client()
        service = DatabricksUnityService(client)
        
        # Get raw view definition
        view_definition = service._get_view_definition(catalog_name, schema_name, view_name)
        
        result = {
            'view_name': view_name,
            'raw_definition': view_definition,
            'definition_length': len(view_definition) if view_definition else 0
        }
        
        response = jsonify(result)
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
        
    except Exception as e:
        logger.error(f"Error getting view definition: {e}")
        response = jsonify({'error': str(e)})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 500

@data_modeling_bp.route('/project', methods=['POST'])
def create_project():
    """Create a new data modeling project"""
    try:
        data = request.get_json()
        
        # Create a new project
        project = DataModelProject(**data)
        
        response = jsonify(project.model_dump())
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
        
    except ValidationError as e:
        response = jsonify({'error': 'Invalid project data', 'validation_errors': e.errors()})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 422
    except Exception as e:
        logger.error(f"Error creating project: {e}")
        response = jsonify({'error': str(e)})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 500


@data_modeling_bp.route('/project/<project_id>/table', methods=['POST'])
def add_table_to_project(project_id: str):
    """Add a new table to a project"""
    try:
        data = request.get_json()
        
        # Create a new table
        table = DataTable(**data)
        
        response = jsonify(table.model_dump())
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
        
    except ValidationError as e:
        response = jsonify({'error': 'Invalid table data', 'validation_errors': e.errors()})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 422
    except Exception as e:
        logger.error(f"Error adding table: {e}")
        response = jsonify({'error': str(e)})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 500


@data_modeling_bp.route('/project/<project_id>/relationship', methods=['POST'])
def add_relationship_to_project(project_id: str):
    """Add a new relationship to a project"""
    try:
        data = request.get_json()
        
        # Create a new relationship
        relationship = DataModelRelationship(**data)
        
        response = jsonify(relationship.model_dump())
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
        
    except ValidationError as e:
        response = jsonify({'error': 'Invalid relationship data', 'validation_errors': e.errors()})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 422
    except Exception as e:
        logger.error(f"Error adding relationship: {e}")
        response = jsonify({'error': str(e)})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 500


@data_modeling_bp.route('/project/save', methods=['POST'])
def save_project():
    """Save a data modeling project"""
    try:
        data = request.get_json()
        
        # Add better error handling and logging
        if not data:
            logger.error("No data received in save request")
            response = jsonify({'error': 'No data provided'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 400
        
        logger.info(f"Received save request with keys: {list(data.keys())}")
        
        project_name = data.get('name') or f"project_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Validate project name for filesystem safety
        if not project_name or project_name.strip() == '':
            project_name = f"project_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Remove invalid characters for filenames
        import re
        project_name = re.sub(r'[<>:"/\\|?*]', '_', project_name.strip())
        
        save_format = data.get('format', 'json')  # 'json' or 'yaml'
        project_data = data.get('project', {})

        logger.info(f"Saving project: {project_name}, format: {save_format}")
        logger.info(f"Project data type: {type(project_data)}")
        
        if isinstance(project_data, dict):
            logger.info(f"Project data keys: {list(project_data.keys())}")
        else:
            logger.error(f"Project data is not a dict: {project_data}")
            response = jsonify({'error': 'Invalid project data format'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 400

        # Ensure required fields have defaults if missing
        if not project_data.get('name'):
            project_data['name'] = project_name
        if not project_data.get('catalog_name'):
            project_data['catalog_name'] = 'default'
        if not project_data.get('schema_name'):
            project_data['schema_name'] = 'default'
        if 'tables' not in project_data:
            project_data['tables'] = []
        if 'relationships' not in project_data:
            project_data['relationships'] = []
        if 'metric_views' not in project_data:
            project_data['metric_views'] = []
        if 'metric_relationships' not in project_data:
            project_data['metric_relationships'] = []

        # Check for explicit overwrite permission or if it's the same project
        overwrite = data.get('overwrite', False)
        json_path = os.path.join(SAVED_PROJECTS_DIR, f"{project_name}.json")
        yaml_path = os.path.join(SAVED_PROJECTS_DIR, f"{project_name}.yaml")
        
        existing_file_path = None
        if os.path.exists(json_path):
            existing_file_path = json_path
        elif os.path.exists(yaml_path):
            existing_file_path = yaml_path
        
        if existing_file_path and not overwrite:
            # Check if this is an update to the same project (same ID)
            try:
                current_project_id = project_data.get('id')
                
                if existing_file_path.endswith('.json'):
                    with open(existing_file_path, 'r', encoding='utf-8') as f:
                        existing_data = json.load(f)
                        existing_project_id = existing_data.get('project', {}).get('id')
                else:  # YAML file
                    with open(existing_file_path, 'r', encoding='utf-8') as f:
                        existing_project = DataModelYAMLSerializer.from_yaml(f.read())
                        existing_project_id = existing_project.id
                
                # If the project IDs don't match, it's a different project with the same name
                if current_project_id and existing_project_id and current_project_id != existing_project_id:
                    response = jsonify({
                        'error': f'A different project with the name "{project_name}" already exists. Please choose a different name or enable overwrite.',
                        'code': 'DUPLICATE_NAME',
                        'existing_project_id': existing_project_id,
                        'current_project_id': current_project_id
                    })
                    response.headers.add('Access-Control-Allow-Origin', '*')
                    return response, 409
                    
            except Exception as e:
                logger.warning(f"Could not check existing project ID: {e}")
                # If we can't determine the project ID, allow the save (fallback to overwrite behavior)
                pass

        # Validate the project with better error handling
        try:
            project = DataModelProject(**project_data)
        except Exception as validation_error:
            logger.error(f"Project validation failed: {validation_error}")
            logger.error(f"Project data that failed validation: {project_data}")
            
            # Check if it's a list index error
            if "list index out of range" in str(validation_error):
                logger.error("List index out of range error - likely missing required fields")
                
            response = jsonify({
                'error': f'Project validation failed: {str(validation_error)}',
                'project_data_keys': list(project_data.keys()) if isinstance(project_data, dict) else 'Not a dict'
            })
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 422
        
        if save_format == 'yaml':
            # Save as YAML
            yaml_content = DataModelYAMLSerializer.to_yaml(project)
            save_path = os.path.join(SAVED_PROJECTS_DIR, f"{project_name}.yaml")
            
            with open(save_path, 'w', encoding='utf-8') as f:
                f.write(yaml_content)
        else:
            # Save as JSON
            save_path = os.path.join(SAVED_PROJECTS_DIR, f"{project_name}.json")
            
            # Use model_dump with mode='json' to handle datetime serialization
            project_data = project.model_dump(mode='json')
            
            with open(save_path, 'w', encoding='utf-8') as f:
                json.dump(
                    {
                        'name': project_name,
                        'saved_at': datetime.now().isoformat(),
                        'format': save_format,
                        'project': project_data
                    },
                    f,
                    indent=2,
                    ensure_ascii=False
                )
        
        response = jsonify({
            'message': f'Project saved successfully as {project_name}',
            'saved_at': datetime.now().isoformat(),
            'file_path': save_path,
            'format': save_format
        })
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response

    except ValidationError as e:
        logger.error(f"Validation error saving project: {e.errors()}")
        response = jsonify({'error': 'Invalid project data', 'validation_errors': e.errors()})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 422
    except Exception as e:
        logger.error(f"Error saving project: {e}")
        response = jsonify({'error': str(e)})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 500


@data_modeling_bp.route('/project/load/<project_name>', methods=['GET'])
def load_project(project_name: str):
    """Load a saved data modeling project"""
    try:
        # Try both JSON and YAML formats
        json_path = os.path.join(SAVED_PROJECTS_DIR, f"{project_name}.json")
        yaml_path = os.path.join(SAVED_PROJECTS_DIR, f"{project_name}.yaml")
        
        if os.path.exists(yaml_path):
            # Load YAML format
            with open(yaml_path, 'r', encoding='utf-8') as f:
                project = DataModelYAMLSerializer.from_yaml(f.read())
            
            saved_data = {
                'name': project_name,
                'saved_at': datetime.fromtimestamp(os.path.getmtime(yaml_path)).isoformat(),
                'format': 'yaml',
                'project': project.model_dump()
            }
            
        elif os.path.exists(json_path):
            # Load JSON format
            with open(json_path, 'r', encoding='utf-8') as f:
                saved_data = json.load(f)
        else:
            response = jsonify({'error': f'Project {project_name} not found'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 404
        
        response = jsonify(saved_data)
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
        
    except Exception as e:
        logger.error(f"Error loading project: {e}")
        response = jsonify({'error': str(e)})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 500


@data_modeling_bp.route('/projects', methods=['GET'])
def list_saved_projects():
    """List all saved data modeling projects"""
    try:
        saved_files = []
        if os.path.exists(SAVED_PROJECTS_DIR):
            for filename in os.listdir(SAVED_PROJECTS_DIR):
                if filename.endswith(('.json', '.yaml', '.yml')):
                    project_name = os.path.splitext(filename)[0]
                    file_path = os.path.join(SAVED_PROJECTS_DIR, filename)
                    
                    # Get file modification time
                    mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                    
                    # Determine format
                    file_format = 'yaml' if filename.endswith(('.yaml', '.yml')) else 'json'
                    
                    # Try to read the project to get its ID
                    project_id = None
                    try:
                        if file_format == 'json':
                            with open(file_path, 'r', encoding='utf-8') as f:
                                project_data = json.load(f)
                                project_id = project_data.get('project', {}).get('id')
                        elif file_format == 'yaml':
                            with open(file_path, 'r', encoding='utf-8') as f:
                                project = DataModelYAMLSerializer.from_yaml(f.read())
                                project_id = project.id
                    except Exception as e:
                        logger.warning(f"Could not read project ID from {filename}: {e}")
                        # Use filename as fallback ID
                        project_id = project_name
                    
                    saved_files.append({
                        'id': project_id,
                        'name': project_name,
                        'saved_at': mod_time.isoformat(),
                        'file_path': file_path,
                        'format': file_format
                    })
        
        # Sort by modification time, newest first
        saved_files.sort(key=lambda x: x['saved_at'], reverse=True)
        
        response = jsonify(saved_files)
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
        
    except Exception as e:
        logger.error(f"Error listing projects: {e}")
        response = jsonify({'error': str(e)})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 500


@data_modeling_bp.route('/project/<project_id>/generate_ddl', methods=['POST'])
def generate_ddl(project_id: str):
    """Generate DDL for a project or specific tables"""
    try:
        data = request.get_json()
        project_data = data.get('project', {})
        table_ids = data.get('table_ids', [])  # If empty, generate for all tables

        logger.info(f"DDL Generation - Project ID: {project_id}")
        logger.info(f"DDL Generation - Table IDs: {table_ids}")
        logger.info(f"DDL Generation - Project data keys: {list(project_data.keys())}")

        project = DataModelProject(**project_data)
        
        
        client = get_sdk_client()
        unity_service = DatabricksUnityService(client)
        
        ddl_statements = []
        
        # Filter tables if specific IDs provided
        tables_to_process = project.tables
        if table_ids:
            tables_to_process = [table for table in project.tables if table.id in table_ids]
        
        # Sort tables by dependency order (referenced tables first)
        tables_to_process = _sort_tables_by_dependencies(tables_to_process)
        logger.info(f"DDL Generation - Table order: {[table.name for table in tables_to_process]}")
        
        # Additional safety check: Move fact tables that reference other fact tables to the end
        fact_tables_with_fact_refs = []
        other_tables = []
        
        for table in tables_to_process:
            is_fact_referencing_fact = False
            if table.name.startswith('fact_'):
                for field in table.fields:
                    if field.is_foreign_key and field.foreign_key_reference:
                        # Check if this fact table references another fact table
                        if hasattr(field.foreign_key_reference, 'referenced_table_id'):
                            referenced_table_id = field.foreign_key_reference.referenced_table_id
                            referenced_table = next((t for t in tables_to_process if t.id == referenced_table_id), None)
                            if referenced_table and referenced_table.name.startswith('fact_'):
                                is_fact_referencing_fact = True
                                logger.info(f"🔄 DDL Generation - Moving {table.name} to end (references fact table {referenced_table.name})")
                                break
            
            if is_fact_referencing_fact:
                fact_tables_with_fact_refs.append(table)
            else:
                other_tables.append(table)
        
        # Reorder: other tables first, then fact tables that reference fact tables
        if fact_tables_with_fact_refs:
            tables_to_process = other_tables + fact_tables_with_fact_refs
            logger.info(f"DDL Generation - Reordered table order: {[table.name for table in tables_to_process]}")
        
        # Generate DDL for each table
        for table in tables_to_process:
            # Use table's specific catalog/schema or fall back to project defaults
            effective_catalog = project.get_effective_catalog(table.catalog_name)
            effective_schema = project.get_effective_schema(table.schema_name)
            logger.info(f"🎯 Generating DDL for table {table.name} in {effective_catalog}.{effective_schema}")
            
            ddl = unity_service.generate_ddl_for_table(
                table, effective_catalog, effective_schema, None, project.tables
            )
            if ddl:
                ddl_statements.append({
                    'table_name': table.name,
                    'table_id': table.id,
                    'ddl': ddl
                })
        
        response = jsonify({
            'catalog_name': project.catalog_name,
            'schema_name': project.schema_name,
            'ddl_statements': ddl_statements
        })
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response

    except ValidationError as e:
        logger.error(f"DDL Generation - Validation error: {e.errors()}")
        # Convert validation errors to JSON-serializable format
        serializable_errors = []
        for error in e.errors():
            serializable_error = {
                'type': error.get('type'),
                'loc': error.get('loc'),
                'msg': error.get('msg'),
                'input': str(error.get('input', ''))  # Convert to string to ensure JSON serialization
            }
            serializable_errors.append(serializable_error)
        
        response = jsonify({'error': 'Invalid project data', 'validation_errors': serializable_errors})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 422
    except Exception as e:
        logger.error(f"Error generating DDL: {e}")
        response = jsonify({'error': str(e)})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 500


@data_modeling_bp.route('/test_error_scenarios', methods=['POST'])
def test_error_scenarios():
    """Test route to simulate various error scenarios for graceful handling"""
    try:
        data = request.get_json()
        error_type = data.get('error_type', 'permission')
        
        print(f"🧪 TESTING ERROR SCENARIO: {error_type}")
        
        # Simulate different error types
        if error_type == 'permission':
            raise Exception("Access denied: User does not have permission to access catalog 'restricted_catalog'")
        elif error_type == 'readonly':
            raise Exception("Cannot modify table in read-only schema 'readonly_schema'")
        elif error_type == 'not_found':
            raise Exception("Schema 'nonexistent_schema' does not exist in catalog 'test_catalog'")
        elif error_type == 'catalog_missing':
            raise Exception("Catalog 'missing_catalog' not found")
        elif error_type == 'insufficient_privileges':
            raise Exception("Insufficient privileges to create table in schema 'protected_schema'")
        else:
            raise Exception("Unknown error type for testing")
            
    except Exception as e:
        # Test the graceful error handling logic
        error_message = str(e).lower()
        error_type = type(e).__name__
        
        # Categorize errors for graceful handling (same logic as in create_table_from_model)
        is_permission_error = any(keyword in error_message for keyword in [
            'permission', 'access denied', 'forbidden', 'unauthorized', 
            'read-only', 'readonly', 'does not exist', 'not found',
            'insufficient privileges', 'access is denied', 'cannot access'
        ])
        
        is_schema_error = any(keyword in error_message for keyword in [
            'schema', 'catalog', 'database', 'namespace'
        ]) and any(keyword in error_message for keyword in [
            'does not exist', 'not found', 'invalid'
        ])
        
        if is_permission_error or is_schema_error:
            # Graceful handling for permission/access errors
            warning_msg = f"⚠️ Access issue detected: {str(e)}"
            print(f"✅ GRACEFUL ERROR HANDLING TRIGGERED: {warning_msg}")
            
            response = jsonify({
                'success': False,
                'warning': warning_msg,
                'error': f"Access denied or resource not found: {str(e)}",
                'skipped': True,
                'graceful_handling': True,
                'error_category': 'permission/access'
            })
        else:
            # Handle other errors as hard failures
            print(f"❌ HARD ERROR (not gracefully handled): {str(e)}")
            response = jsonify({
                'success': False,
                'error': f"Hard error: {str(e)}",
                'graceful_handling': False,
                'error_category': 'other'
            })
        
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 200  # Return 200 to show graceful handling worked


@data_modeling_bp.route('/views/generate_ddl', methods=['POST'])
def generate_view_ddl():
    """Generate DDL for traditional views"""
    try:
        data = request.get_json()
        views_data = data.get('views', [])
        catalog_name = data.get('catalog_name')
        schema_name = data.get('schema_name')
        
        if not catalog_name or not schema_name:
            response = jsonify({'error': 'catalog_name and schema_name are required'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 400
        
        if not views_data:
            response = jsonify({'error': 'views data is required'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 400
        
        # Get Databricks client
        client = get_sdk_client()
        if not client:
            response = jsonify({'error': 'Failed to connect to Databricks'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 500
        
        service = DatabricksUnityService(client)
        
        # Generate DDL for each view
        ddl_statements = []
        for view_data in views_data:
            try:
                # Create TraditionalView object from data
                view = TraditionalView(**view_data)
                
                # Generate DDL
                view_ddl = service.generate_ddl_for_traditional_view(view, catalog_name, schema_name)
                if view_ddl:
                    ddl_statements.append(f"-- DDL for view: {view.name}")
                    ddl_statements.append(view_ddl)
                    ddl_statements.append("")  # Empty line separator
                    
            except Exception as e:
                logger.error(f"Error generating DDL for view {view_data.get('name', 'unknown')}: {e}")
                ddl_statements.append(f"-- ERROR generating DDL for view {view_data.get('name', 'unknown')}: {e}")
                ddl_statements.append("")
        
        result = {
            'ddl': '\n'.join(ddl_statements),
            'view_count': len(views_data)
        }
        
        response = jsonify(result)
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
        
    except Exception as e:
        logger.error(f"Error generating view DDL: {e}")
        response = jsonify({'error': str(e)})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 500


@data_modeling_bp.route('/project/<project_id>/apply_changes', methods=['POST'])
def apply_changes(project_id: str):
    """Apply changes to Databricks by creating/modifying tables"""
    try:
        data = request.get_json()
        print(f"🚀 APPLY_CHANGES - Raw request data keys: {list(data.keys()) if data else 'None'}")
        
        project_data = data.get('project', {})
        table_ids = data.get('table_ids', [])  # If empty, apply all tables
        create_only = data.get('create_only', True)  # Only create new tables, don't modify existing
        warehouse_id = data.get('warehouse_id', None)  # Optional warehouse selection
        session_id = data.get('session_id', None)  # Optional session ID for streaming progress
        
        print(f"🚀 APPLY_CHANGES called with session_id: {session_id}")
        print(f"🚀 Project ID: {project_id}")
        print(f"🚀 Table IDs: {table_ids}")
        print(f"🚀 Warehouse ID: {warehouse_id}")
        print(f"🚀 Session ID type: {type(session_id)}")
        print(f"🚀 Session ID value: '{session_id}'")
        print(f"🚀 Is individual table apply: {len(table_ids) > 0}")
        if len(table_ids) > 0:
            print(f"🚀 Individual table IDs: {table_ids}")
        
        try:
            project = DataModelProject(**project_data)
        except ValidationError as ve:
            logger.error(f"Project validation failed: {ve}")
            response = jsonify({
                'error': f'Project validation failed: {str(ve)}',
                'validation_errors': ve.errors() if hasattr(ve, 'errors') else []
            })
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 422
        
        # Debug: Log received table data to see FK fields
        for table in project.tables:
            print(f"   Table: {table.name} ({len(table.fields)} fields)")
            for field in table.fields:
                print(f"      Field: {field.name}, is_foreign_key: {field.is_foreign_key}, foreign_key_reference: {field.foreign_key_reference}")
        
        client = get_sdk_client()
        unity_service = DatabricksUnityService(client)
        
        # Log connection status
        if client is None:
            logger.warning("No Databricks client available - running in demo mode")
        else:
            logger.info("✅ Databricks client connected successfully")
        
        results = []
        
        # Filter tables if specific IDs provided
        tables_to_process = project.tables
        if table_ids:
            tables_to_process = [table for table in project.tables if table.id in table_ids]
        
        # Sort tables by dependency order (referenced tables first)
        tables_to_process = _sort_tables_by_dependencies(tables_to_process)
        logger.info(f"Apply Changes - Table order: {[table.name for table in tables_to_process]}")
        
        # Additional safety check: Move fact tables that reference other fact tables to the end
        fact_tables_with_fact_refs = []
        other_tables = []
        
        for table in tables_to_process:
            is_fact_referencing_fact = False
            if table.name.startswith('fact_'):
                for field in table.fields:
                    if field.is_foreign_key and field.foreign_key_reference:
                        # Check if this fact table references another fact table
                        if hasattr(field.foreign_key_reference, 'referenced_table_id'):
                            referenced_table_id = field.foreign_key_reference.referenced_table_id
                            referenced_table = next((t for t in tables_to_process if t.id == referenced_table_id), None)
                            if referenced_table and referenced_table.name.startswith('fact_'):
                                is_fact_referencing_fact = True
                                logger.info(f"🔄 Moving {table.name} to end (references fact table {referenced_table.name})")
                                break
            
            if is_fact_referencing_fact:
                fact_tables_with_fact_refs.append(table)
            else:
                other_tables.append(table)
        
        # Reorder: other tables first, then fact tables that reference fact tables
        if fact_tables_with_fact_refs:
            tables_to_process = other_tables + fact_tables_with_fact_refs
            logger.info(f"Apply Changes - Reordered table order: {[table.name for table in tables_to_process]}")
        
        # Create progress session if session_id provided
        if session_id:
            create_progress_session(session_id)
            send_progress_update(session_id, {
                'type': 'started',
                'total_tables': len(tables_to_process),
                'table_names': [table.name for table in tables_to_process]
            })
        
        # Step 1: Create/Update all tables first (DDL generation)
        table_results = []
        for i, table in enumerate(tables_to_process):
            try:
                # Send progress update: starting table processing
                if session_id:
                    send_progress_update(session_id, {
                        'type': 'table_started',
                        'table_index': i,
                        'table_name': table.name,
                        'table_id': table.id,
                        'progress': (i / len(tables_to_process)) * 100
                    })
                
                # Clear any previous error and warnings
                unity_service.clear_last_error()
                unity_service.clear_warnings()
                
                # Use table's specific catalog/schema or fall back to project defaults
                logger.info(f"🔍 DEBUG table {table.name}: catalog_name={table.catalog_name}, schema_name={table.schema_name}")
                effective_catalog = project.get_effective_catalog(table.catalog_name)
                effective_schema = project.get_effective_schema(table.schema_name)
                logger.info(f"🎯 Using effective catalog/schema for table {table.name}: {effective_catalog}.{effective_schema}")
                
                table_result = unity_service.create_table_from_model(
                    table, effective_catalog, effective_schema, None, warehouse_id, project.tables
                )
                
                # Handle both old boolean return and new dict return for backward compatibility
                if isinstance(table_result, bool):
                    success = table_result
                    result = {
                        'table_name': table.name,
                        'table_id': table.id,
                        'success': success,
                        'action': 'created' if success else 'failed'
                    }
                else:
                    success = table_result.get('success', False)
                    is_skipped = table_result.get('skipped', False)
                    warning_msg = table_result.get('warning', None)
                    
                    result = {
                        'table_name': table.name,
                        'table_id': table.id,
                        'success': success,
                        'action': 'created' if success else ('skipped' if is_skipped else 'failed'),
                        'ddl_executed': table_result.get('ddl_executed', False),
                        'tags': table_result.get('tags', {})
                    }
                    
                    # Add warning information for graceful failures
                    if warning_msg:
                        result['warning'] = warning_msg
                        result['skipped'] = is_skipped
                
                # Include error message if operation failed
                if not success:
                    error_message = table_result.get('error') if isinstance(table_result, dict) else unity_service.get_last_error()
                    if error_message:
                        result['error'] = error_message
                
                # Include warnings even if operation succeeded
                warnings = unity_service.get_warnings()
                if warnings:
                    result['warnings'] = warnings
                
                table_results.append(result)
                
                # Send progress update: table completed
                if session_id:
                    send_progress_update(session_id, {
                        'type': 'table_completed',
                        'table_index': i,
                        'table_name': table.name,
                        'table_id': table.id,
                        'result': result,
                        'progress': ((i + 1) / len(tables_to_process)) * 90  # Reserve 10% for constraints
                    })
                
            except Exception as e:
                logger.error(f"Error creating table {table.name}: {e}")
                error_result = {
                    'table_name': table.name,
                    'table_id': table.id,
                    'success': False,
                    'action': 'failed',
                    'error': str(e)
                }
                table_results.append(error_result)
                
                # Send progress update: table failed
                if session_id:
                    send_progress_update(session_id, {
                        'type': 'table_completed',
                        'table_index': i,
                        'table_name': table.name,
                        'table_id': table.id,
                        'result': error_result,
                        'progress': ((i + 1) / len(tables_to_process)) * 90
                    })
        
        # Step 2: Apply self-referencing constraints first (after all tables are created)
        logger.info("🔄 Applying self-referencing constraints to tables...")
        for i, table in enumerate(tables_to_process):
            if table_results[i]['success']:
                try:
                    effective_catalog = project.get_effective_catalog(table.catalog_name)
                    effective_schema = project.get_effective_schema(table.schema_name)
                    
                    self_ref_success = unity_service._apply_self_referencing_constraints(
                        table, effective_catalog, effective_schema, project.tables
                    )
                    table_results[i]['self_ref_constraints_applied'] = self_ref_success
                except Exception as e:
                    logger.error(f"Error applying self-referencing constraints to table {table.name}: {e}")
                    table_results[i]['self_ref_constraints_applied'] = False
        
        # Step 3: Apply general constraints to all tables (after self-referencing constraints)
        logger.info("🔗 Applying general constraints to all tables...")
        for i, table in enumerate(tables_to_process):
            if table_results[i]['success']:
                try:
                    constraint_success = unity_service.apply_constraints_to_table(
                        table, project.catalog_name, project.schema_name, project.relationships, warehouse_id
                    )
                    table_results[i]['constraints_applied'] = constraint_success
                except Exception as e:
                    logger.error(f"Error applying constraints to table {table.name}: {e}")
                    table_results[i]['constraints_applied'] = False
                    table_results[i]['constraint_error'] = str(e)
        
        results = table_results
        
        # Send final completion update
        if session_id:
            send_progress_update(session_id, {
                'type': 'completed',
                'progress': 100,
                'results': results,
                'catalog_name': project.catalog_name,
                'schema_name': project.schema_name
            })
            # Give frontend a moment to process the completion message before closing
            import time
            time.sleep(0.1)
            # Send end signal
            send_progress_update(session_id, None)
        
        response = jsonify({
            'catalog_name': project.catalog_name,
            'schema_name': project.schema_name,
            'results': results
        })
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
        
    except ValidationError as e:
        response = jsonify({'error': 'Invalid project data', 'validation_errors': e.errors()})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 422
    except Exception as e:
        logger.error(f"Error applying changes: {e}")
        response = jsonify({'error': str(e)})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 500


@data_modeling_bp.route('/app_state', methods=['GET'])
def get_app_state():
    """Get the current application state - whether user needs to select/create a project"""
    try:
        # Check if there are any saved projects
        has_projects = False
        project_files = []
        if os.path.exists(SAVED_PROJECTS_DIR):
            project_files = [f for f in os.listdir(SAVED_PROJECTS_DIR) 
                           if f.endswith(('.json', '.yaml', '.yml'))]
            has_projects = len(project_files) > 0
        
        app_state = {
            'needs_project_selection': True,  # Always require project selection on startup
            'has_existing_projects': has_projects,
            'project_count': len(project_files) if has_projects else 0
        }
        
        response = jsonify(app_state)
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
        
    except Exception as e:
        logger.error(f"Error getting app state: {e}")
        response = jsonify({'error': str(e)})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 500


@data_modeling_bp.route('/datatypes', methods=['GET'])
def get_supported_datatypes():
    """Get list of supported Databricks data types"""
    try:
        datatypes = [
            {
                'value': dt.value,
                'name': dt.name,
                'category': 'numeric' if dt.value in ['BIGINT', 'INT', 'SMALLINT', 'TINYINT', 'FLOAT', 'DOUBLE', 'DECIMAL'] 
                           else 'string' if dt.value in ['STRING', 'VARCHAR', 'CHAR'] 
                           else 'datetime' if dt.value in ['DATE', 'TIMESTAMP', 'TIMESTAMP_NTZ', 'INTERVAL']
                           else 'boolean' if dt.value == 'BOOLEAN'
                           else 'binary' if dt.value == 'BINARY'
                           else 'geospatial' if dt.value in ['GEOGRAPHY', 'GEOMETRY']
                           else 'semi-structured' if dt.value == 'VARIANT'
                           else 'complex',
                'requires_parameters': dt.value in ['VARCHAR', 'CHAR', 'DECIMAL', 'GEOGRAPHY', 'GEOMETRY', 'ARRAY', 'MAP', 'STRUCT', 'INTERVAL'],
                'parameter_type': 'length' if dt.value in ['VARCHAR', 'CHAR']
                                else 'precision_scale' if dt.value == 'DECIMAL'
                                else 'srid' if dt.value in ['GEOGRAPHY', 'GEOMETRY']
                                else 'element_type' if dt.value == 'ARRAY'
                                else 'key_value_types' if dt.value == 'MAP'
                                else 'struct_fields' if dt.value == 'STRUCT'
                                else 'interval_qualifier' if dt.value == 'INTERVAL'
                                else None,
                'parameter_description': 'Length (e.g., 50)' if dt.value in ['VARCHAR', 'CHAR']
                                      else 'Precision,Scale (e.g., 10,2)' if dt.value == 'DECIMAL'
                                      else 'SRID: 4326 only' if dt.value == 'GEOGRAPHY'
                                      else 'SRID (e.g., 4326, 3857, 0)' if dt.value == 'GEOMETRY'
                                      else 'Element Type (e.g., STRING)' if dt.value == 'ARRAY'
                                      else 'Key Type, Value Type (e.g., STRING, INT)' if dt.value == 'MAP'
                                      else 'Field definitions' if dt.value == 'STRUCT'
                                      else 'Interval qualifier (e.g., YEAR TO MONTH, DAY TO SECOND)' if dt.value == 'INTERVAL'
                                      else None
            }
            for dt in DatabricksDataType
        ]
        
        response = jsonify(datatypes)
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
        
    except Exception as e:
        logger.error(f"Error getting datatypes: {e}")
        response = jsonify({'error': str(e)})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 500

@data_modeling_bp.route('/project/<project_identifier>', methods=['DELETE'])
def delete_project(project_identifier: str):
    """Delete a project by ID or name"""
    try:
        logger.info(f"🗑️ Attempting to delete project: {project_identifier}")
        
        # First, try direct file name approach (most common case)
        project_file = None
        project_name = None
        project_id = None
        
        # Try JSON and YAML files with the identifier as filename
        for ext in ['.json', '.yaml', '.yml']:
            potential_file = os.path.join(SAVED_PROJECTS_DIR, f"{project_identifier}{ext}")
            if os.path.exists(potential_file):
                project_file = potential_file
                project_name = project_identifier
                project_id = project_identifier
                logger.info(f"Found project file by name: {project_file}")
                break
        
        # If not found by filename, search by ID in all files
        if not project_file and os.path.exists(SAVED_PROJECTS_DIR):
            for filename in os.listdir(SAVED_PROJECTS_DIR):
                if filename.endswith(('.json', '.yaml', '.yml')):
                    file_path = os.path.join(SAVED_PROJECTS_DIR, filename)
                    file_format = 'yaml' if filename.endswith(('.yaml', '.yml')) else 'json'
                    
                    try:
                        # Check if this file contains the project we're looking for
                        found_project = False
                        if file_format == 'json':
                            with open(file_path, 'r', encoding='utf-8') as f:
                                project_data = json.load(f)
                                stored_id = project_data.get('project', {}).get('id')
                                stored_name = project_data.get('name')
                                logger.debug(f"JSON project {filename}: id={stored_id}, name={stored_name}, looking for={project_identifier}")
                                if stored_id == project_identifier:
                                    found_project = True
                                    project_name = stored_name
                                    project_id = stored_id
                        elif file_format == 'yaml':
                            with open(file_path, 'r', encoding='utf-8') as f:
                                project = DataModelYAMLSerializer.from_yaml(f.read())
                                logger.debug(f"YAML project {filename}: id={project.id}, name={project.name}, looking for={project_identifier}")
                                if project.id == project_identifier:
                                    found_project = True
                                    project_name = project.name
                                    project_id = project.id
                        
                        if found_project:
                            project_file = file_path
                            logger.info(f"Found project to delete by ID: {project_file}")
                            break
                            
                    except Exception as e:
                        logger.warning(f"Error reading project file {filename}: {e}")
                        continue
        
        if not project_file:
            logger.warning(f"Project not found: {project_identifier}")
            response = jsonify({'error': 'Project not found'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 404
        
        # Delete the project file
        os.remove(project_file)
        logger.info(f"🗑️ Successfully deleted project: {project_name} (ID: {project_id})")
        
        response = jsonify({
            'message': f'Project "{project_name}" deleted successfully',
            'project_id': project_id,
            'project_name': project_name
        })
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
        
    except Exception as e:
        logger.error(f"Error deleting project {project_identifier}: {e}")
        response = jsonify({'error': str(e)})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 500

@data_modeling_bp.route('/project/<project_identifier>/download', methods=['GET'])
def download_project_yaml(project_identifier: str):
    """Download a project as YAML file"""
    try:
        logger.info(f"📥 Attempting to download project: {project_identifier}")
        
        # First, try direct file name approach (most common case)
        project_file = None
        project_name = None
        
        # Try JSON and YAML files with the identifier as filename
        for ext in ['.json', '.yaml', '.yml']:
            potential_file = os.path.join(SAVED_PROJECTS_DIR, f"{project_identifier}{ext}")
            if os.path.exists(potential_file):
                project_file = potential_file
                project_name = project_identifier
                logger.info(f"Found project file by name: {project_file}")
                break
        
        # If not found by filename, search by ID in all files
        if not project_file and os.path.exists(SAVED_PROJECTS_DIR):
            for filename in os.listdir(SAVED_PROJECTS_DIR):
                if filename.endswith(('.json', '.yaml', '.yml')):
                    file_path = os.path.join(SAVED_PROJECTS_DIR, filename)
                    file_format = 'yaml' if filename.endswith(('.yaml', '.yml')) else 'json'
                    
                    try:
                        # Check if this file contains the project we're looking for
                        found_project = False
                        if file_format == 'json':
                            with open(file_path, 'r', encoding='utf-8') as f:
                                project_data = json.load(f)
                                stored_id = project_data.get('project', {}).get('id')
                                stored_name = project_data.get('name')
                                if stored_id == project_identifier:
                                    found_project = True
                                    project_name = stored_name
                        elif file_format == 'yaml':
                            with open(file_path, 'r', encoding='utf-8') as f:
                                project = DataModelYAMLSerializer.from_yaml(f.read())
                                if project.id == project_identifier:
                                    found_project = True
                                    project_name = project.name
                        
                        if found_project:
                            project_file = file_path
                            logger.info(f"Found project to download by ID: {project_file}")
                            break
                            
                    except Exception as e:
                        logger.warning(f"Error reading project file {filename}: {e}")
                        continue
        
        if not project_file:
            logger.warning(f"Project not found for download: {project_identifier}")
            response = jsonify({'error': 'Project not found'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 404
        
        # Read the project file
        try:
            with open(project_file, 'r', encoding='utf-8') as f:
                if project_file.endswith('.json'):
                    # Convert JSON to YAML format
                    project_data = json.load(f)
                    project = DataModelProject(**project_data['project'])
                    yaml_content = DataModelYAMLSerializer.to_yaml(project)
                else:
                    # Already YAML format
                    yaml_content = f.read()
            
            # Create response with YAML content
            response = Response(
                yaml_content,
                mimetype='application/x-yaml',
                headers={
                    'Content-Disposition': f'attachment; filename="{project_name}.yaml"',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Expose-Headers': 'Content-Disposition'
                }
            )
            
            logger.info(f"📥 Successfully prepared download for project: {project_name}")
            return response
            
        except Exception as e:
            logger.error(f"Error reading project file for download: {e}")
            response = jsonify({'error': f'Error reading project file: {str(e)}'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 500
        
    except Exception as e:
        logger.error(f"Error downloading project {project_identifier}: {e}")
        response = jsonify({'error': str(e)})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 500

@data_modeling_bp.route('/project/create_sample', methods=['POST'])
def create_sample_project():
    """Create a comprehensive sample project with all Databricks data types for testing"""
    try:
        data = request.get_json()
        project_name = data.get('project_name', 'Databricks_DataTypes_Test')
        catalog_name = data.get('catalog_name', 'main')
        schema_name = data.get('schema_name', 'default')
        
        logger.info(f"🚀 Creating sample project: {project_name}")
        
        # Create the project
        project = DataModelProject(
            name=project_name,
            description="Comprehensive test project with all Databricks data types",
            catalog_name=catalog_name,
            schema_name=schema_name,
            tables=[],
            relationships=[],
            metric_views=[]
        )
        
        # Create comprehensive test tables
        tables_data = [
            {
                'name': 'numeric_types_test',
                'logical_name': 'Numeric Data Types Test',
                'comment': 'Table testing all numeric data types',
                'position': (100, 100),
                'fields': [
                    {'name': 'id', 'data_type': 'BIGINT', 'logical_name': 'Record ID', 'is_primary_key': True, 'nullable': False},
                    {'name': 'tiny_int_col', 'data_type': 'TINYINT', 'logical_name': 'Tiny Integer'},
                    {'name': 'small_int_col', 'data_type': 'SMALLINT', 'logical_name': 'Small Integer'},
                    {'name': 'int_col', 'data_type': 'INT', 'logical_name': 'Integer'},
                    {'name': 'big_int_col', 'data_type': 'BIGINT', 'logical_name': 'Big Integer'},
                    {'name': 'float_col', 'data_type': 'FLOAT', 'logical_name': 'Float Number'},
                    {'name': 'double_col', 'data_type': 'DOUBLE', 'logical_name': 'Double Precision'},
                    {'name': 'decimal_10_2', 'data_type': 'DECIMAL', 'type_parameters': '10,2', 'logical_name': 'Decimal 10,2'},
                    {'name': 'decimal_18_4', 'data_type': 'DECIMAL', 'type_parameters': '18,4', 'logical_name': 'Decimal 18,4'},
                    {'name': 'decimal_38_0', 'data_type': 'DECIMAL', 'type_parameters': '38', 'logical_name': 'Decimal 38,0'},
                ]
            },
            {
                'name': 'string_types_test',
                'logical_name': 'String Data Types Test',
                'comment': 'Table testing all string data types',
                'position': (400, 100),
                'fields': [
                    {'name': 'id', 'data_type': 'BIGINT', 'logical_name': 'Record ID', 'is_primary_key': True, 'nullable': False},
                    {'name': 'string_col', 'data_type': 'STRING', 'logical_name': 'String Column'},
                    {'name': 'varchar_50', 'data_type': 'VARCHAR', 'type_parameters': '50', 'logical_name': 'Variable Char 50'},
                    {'name': 'varchar_255', 'data_type': 'VARCHAR', 'type_parameters': '255', 'logical_name': 'Variable Char 255'},
                    {'name': 'char_10', 'data_type': 'CHAR', 'type_parameters': '10', 'logical_name': 'Fixed Char 10'},
                    {'name': 'char_1', 'data_type': 'CHAR', 'type_parameters': '1', 'logical_name': 'Single Character'},
                ]
            },
            {
                'name': 'datetime_types_test',
                'logical_name': 'Date Time Data Types Test',
                'comment': 'Table testing all date/time data types',
                'position': (700, 100),
                'fields': [
                    {'name': 'id', 'data_type': 'BIGINT', 'logical_name': 'Record ID', 'is_primary_key': True, 'nullable': False},
            {'name': 'date_col', 'data_type': 'DATE', 'logical_name': 'Date Column'},
            {'name': 'timestamp_col', 'data_type': 'TIMESTAMP', 'logical_name': 'Timestamp Column'},
            {'name': 'timestamp_ntz_col', 'data_type': 'TIMESTAMP_NTZ', 'logical_name': 'Timestamp No Timezone'},
            {'name': 'interval_year_month', 'data_type': 'INTERVAL', 'type_parameters': 'YEAR TO MONTH', 'logical_name': 'Year-Month Interval'},
            {'name': 'interval_day_second', 'data_type': 'INTERVAL', 'type_parameters': 'DAY TO SECOND', 'logical_name': 'Day-Second Interval'},
                ]
            },
            {
                'name': 'other_simple_types_test',
                'logical_name': 'Other Simple Data Types Test',
                'comment': 'Table testing boolean and binary data types',
                'position': (100, 400),
                'fields': [
                    {'name': 'id', 'data_type': 'BIGINT', 'logical_name': 'Record ID', 'is_primary_key': True, 'nullable': False},
                    {'name': 'boolean_col', 'data_type': 'BOOLEAN', 'logical_name': 'Boolean Column'},
                    {'name': 'binary_col', 'data_type': 'BINARY', 'logical_name': 'Binary Column'},
                ]
            },
            {
                'name': 'geospatial_types_test',
                'logical_name': 'Geospatial Data Types Test',
                'comment': 'Table testing geospatial data types',
                'position': (400, 400),
                'fields': [
                    {'name': 'id', 'data_type': 'BIGINT', 'logical_name': 'Record ID', 'is_primary_key': True, 'nullable': False},
                    {'name': 'geography_wgs84', 'data_type': 'GEOGRAPHY', 'type_parameters': '4326', 'logical_name': 'Geography WGS84 (Only Supported SRID)'},
                    {'name': 'geometry_wgs84', 'data_type': 'GEOMETRY', 'type_parameters': '4326', 'logical_name': 'Geometry WGS84'},
                    {'name': 'geometry_mercator', 'data_type': 'GEOMETRY', 'type_parameters': '3857', 'logical_name': 'Geometry Web Mercator'},
                    {'name': 'geometry_unknown', 'data_type': 'GEOMETRY', 'type_parameters': '0', 'logical_name': 'Geometry Unknown CRS'},
                ]
            },
            {
                'name': 'complex_types_test',
                'logical_name': 'Complex Data Types Test',
                'comment': 'Table testing complex data types (ARRAY, MAP, STRUCT)',
                'position': (700, 400),
                'fields': [
                    {'name': 'id', 'data_type': 'BIGINT', 'logical_name': 'Record ID', 'is_primary_key': True, 'nullable': False},
                    {'name': 'array_string', 'data_type': 'ARRAY', 'type_parameters': 'STRING', 'logical_name': 'Array of Strings'},
                    {'name': 'array_bigint', 'data_type': 'ARRAY', 'type_parameters': 'BIGINT', 'logical_name': 'Array of Big Integers'},
                    {'name': 'map_string_bigint', 'data_type': 'MAP', 'type_parameters': 'STRING,BIGINT', 'logical_name': 'Map String to BigInt'},
                    {'name': 'map_bigint_string', 'data_type': 'MAP', 'type_parameters': 'BIGINT,STRING', 'logical_name': 'Map BigInt to String'},
                    {'name': 'struct_person', 'data_type': 'STRUCT', 'type_parameters': 'name:STRING,age:BIGINT,active:BOOLEAN', 'logical_name': 'Person Struct'},
                    {'name': 'struct_address', 'data_type': 'STRUCT', 'type_parameters': 'street:STRING,city:STRING,zipcode:STRING', 'logical_name': 'Address Struct'},
                ]
            },
            {
                'name': 'semistructured_types_test',
                'logical_name': 'Semi-Structured Data Types Test',
                'comment': 'Table testing semi-structured data types',
                'position': (100, 700),
                'fields': [
                    {'name': 'id', 'data_type': 'BIGINT', 'logical_name': 'Record ID', 'is_primary_key': True, 'nullable': False},
                    {'name': 'variant_col', 'data_type': 'VARIANT', 'logical_name': 'Variant Column'},
                    {'name': 'json_col', 'data_type': 'STRING', 'logical_name': 'JSON String Column'},
                ]
            }
        ]
        
        # Create tables
        for table_data in tables_data:
            # Create fields
            fields = []
            for field_data in table_data['fields']:
                field = TableField(
                    name=field_data['name'],
                    data_type=DatabricksDataType(field_data['data_type']),
                    type_parameters=field_data.get('type_parameters'),
                    logical_name=field_data.get('logical_name'),
                    nullable=field_data.get('nullable', True),
                    is_primary_key=field_data.get('is_primary_key', False),
                    comment=field_data.get('comment')
                )
                fields.append(field)
            
            # Create table
            table = DataTable(
                name=table_data['name'],
                logical_name=table_data['logical_name'],
                comment=table_data['comment'],
                catalog_name=catalog_name,
                schema_name=schema_name,
                fields=fields,
                position_x=table_data['position'][0],
                position_y=table_data['position'][1]
            )
            project.tables.append(table)
        
        # Save the project in the correct format expected by the load function
        project_file_path = os.path.join(SAVED_PROJECTS_DIR, f"{project.name}.json")
        
        # Create the wrapper structure expected by the load function
        saved_data = {
            'name': project.name,
            'saved_at': datetime.now().isoformat(),
            'format': 'json',
            'project': project.model_dump()
        }
        
        with open(project_file_path, 'w') as f:
            json.dump(saved_data, f, indent=2, default=str)
        
        logger.info(f"✅ Sample project created successfully: {project.name}")
        
        response = jsonify({
            'success': True,
            'message': f'Sample project "{project.name}" created successfully',
            'project': project.model_dump()
        })
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
        
    except Exception as e:
        logger.error(f"Error creating sample project: {e}")
        response = jsonify({'error': str(e)})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 500

@data_modeling_bp.route('/project/import', methods=['POST'])
def import_project():
    """Import a project from YAML or JSON content and create a new project"""
    try:
        data = request.get_json()
        
        if not data:
            response = jsonify({'error': 'No data provided'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 400
        
        # Support both YAML and JSON content
        yaml_content = data.get('yaml_content')
        json_content = data.get('json_content')
        new_project_name = data.get('project_name')
        overwrite = data.get('overwrite', False)
        
        if not yaml_content and not json_content:
            response = jsonify({'error': 'No YAML or JSON content provided'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 400
        
        if not new_project_name:
            response = jsonify({'error': 'No project name provided'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 400
        
        logger.info(f"📤 Importing project with name: {new_project_name}")
        
        # Parse content based on type
        try:
            if yaml_content:
                logger.info("📄 Parsing YAML content")
                project = DataModelYAMLSerializer.from_yaml(yaml_content)
            else:
                logger.info("📄 Parsing JSON content")
                # Parse JSON and convert to DataModelProject
                project_dict = json.loads(json_content)
                project = DataModelProject(**project_dict)
        except Exception as e:
            logger.error(f"Error parsing content: {e}")
            content_type = "YAML" if yaml_content else "JSON"
            response = jsonify({'error': f'Invalid {content_type} format: {str(e)}'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 400
        
        # Update project with new name and ID
        project.name = new_project_name
        project.id = str(uuid.uuid4())  # Generate new ID
        project.created_at = datetime.now()
        project.updated_at = datetime.now()
        
        # Check if project with this name already exists
        json_file = os.path.join(SAVED_PROJECTS_DIR, f"{new_project_name}.json")
        yaml_file = os.path.join(SAVED_PROJECTS_DIR, f"{new_project_name}.yaml")
        
        existing_file = None
        if os.path.exists(json_file):
            existing_file = json_file
        elif os.path.exists(yaml_file):
            existing_file = yaml_file
        
        if existing_file and not overwrite:
            response = jsonify({
                'error': f'A project with the name "{new_project_name}" already exists. Please choose a different name or enable overwrite.',
                'code': 'DUPLICATE_NAME',
                'project_name': new_project_name
            })
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 409
        
        # Use YAML file for import (consistent format)
        project_file = yaml_file
        
        # Save the project
        try:
            # Ensure the directory exists
            os.makedirs(SAVED_PROJECTS_DIR, exist_ok=True)
            
            # Convert to YAML and save
            yaml_output = DataModelYAMLSerializer.to_yaml(project)
            with open(project_file, 'w', encoding='utf-8') as f:
                f.write(yaml_output)
            
            logger.info(f"📤 Successfully imported project: {new_project_name}")
            
            response = jsonify({
                'message': f'Project "{new_project_name}" imported successfully',
                'project_id': project.id,
                'project_name': new_project_name,
                'project': project.model_dump()
            })
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response
            
        except Exception as e:
            logger.error(f"Error saving imported project: {e}")
            response = jsonify({'error': f'Error saving project: {str(e)}'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 500
        
    except Exception as e:
        logger.error(f"Error importing project: {e}")
        response = jsonify({'error': str(e)})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 500


# ===== METRIC VIEW ROUTES =====

@data_modeling_bp.route('/project/<project_id>/metric_views', methods=['GET'])
def list_metric_views(project_id: str):
    """List all metric views in a project"""
    try:
        project_file = os.path.join(SAVED_PROJECTS_DIR, f"{project_id}.json")
        
        if not os.path.exists(project_file):
            response = jsonify({'error': 'Project not found'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 404
        
        with open(project_file, 'r') as f:
            project_data = json.load(f)
        
        project = DataModelProject(**project_data)
        
        # Return metric views with basic info
        metric_views_data = []
        for mv in project.metric_views:
            mv_data = {
                'id': mv.id,
                'name': mv.name,
                'description': mv.description,
                'version': mv.version,
                'source_table_id': mv.source_table_id,
                'dimensions_count': len(mv.dimensions),
                'measures_count': len(mv.measures),
                'position_x': mv.position_x,
                'position_y': mv.position_y,
                'created_at': mv.created_at.isoformat() if mv.created_at else None,
                'updated_at': mv.updated_at.isoformat() if mv.updated_at else None
            }
            metric_views_data.append(mv_data)
        
        response = jsonify({
            'metric_views': metric_views_data,
            'count': len(metric_views_data)
        })
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
        
    except Exception as e:
        logger.error(f"Error listing metric views: {e}")
        response = jsonify({'error': str(e)})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 500

@data_modeling_bp.route('/project/<project_id>/metric_views', methods=['POST'])
def create_metric_view(project_id: str):
    """Create a new metric view in a project"""
    try:
        data = request.get_json()
        
        # Load project
        project_file = os.path.join(SAVED_PROJECTS_DIR, f"{project_id}.json")
        if not os.path.exists(project_file):
            response = jsonify({'error': 'Project not found'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 404
        
        with open(project_file, 'r') as f:
            project_data = json.load(f)
        
        project = DataModelProject(**project_data)
        
        # Create metric view
        metric_view = MetricView(**data)
        
        # Validate source table exists
        source_table = project.get_table_by_id(metric_view.source_table_id)
        if not source_table:
            response = jsonify({'error': 'Source table not found'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 400
        
        # Add to project
        project.metric_views.append(metric_view)
        project.updated_at = datetime.now()
        
        # Save project
        with open(project_file, 'w') as f:
            json.dump(project.model_dump(), f, indent=2, default=str)
        
        logger.info(f"Created metric view: {metric_view.name}")
        
        response = jsonify({
            'message': f'Metric view {metric_view.name} created successfully',
            'metric_view': metric_view.model_dump()
        })
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
        
    except ValidationError as e:
        logger.error(f"Validation error creating metric view: {e}")
        response = jsonify({
            'error': 'Validation error',
            'details': [{'field': '.'.join(map(str, error['loc'])), 'message': error['msg']} for error in e.errors()]
        })
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 400
        
    except Exception as e:
        logger.error(f"Error creating metric view: {e}")
        response = jsonify({'error': str(e)})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 500

@data_modeling_bp.route('/project/<project_id>/metric_views/<metric_view_id>', methods=['GET'])
def get_metric_view(project_id: str, metric_view_id: str):
    """Get a specific metric view"""
    try:
        project_file = os.path.join(SAVED_PROJECTS_DIR, f"{project_id}.json")
        
        if not os.path.exists(project_file):
            response = jsonify({'error': 'Project not found'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 404
        
        with open(project_file, 'r') as f:
            project_data = json.load(f)
        
        project = DataModelProject(**project_data)
        metric_view = project.get_metric_view_by_id(metric_view_id)
        
        if not metric_view:
            response = jsonify({'error': 'Metric view not found'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 404
        
        response = jsonify(metric_view.model_dump())
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
        
    except Exception as e:
        logger.error(f"Error getting metric view: {e}")
        response = jsonify({'error': str(e)})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 500

@data_modeling_bp.route('/project/<project_id>/metric_views/<metric_view_id>', methods=['PUT'])
def update_metric_view(project_id: str, metric_view_id: str):
    """Update a metric view"""
    try:
        data = request.get_json()
        
        # Load project
        project_file = os.path.join(SAVED_PROJECTS_DIR, f"{project_id}.json")
        if not os.path.exists(project_file):
            response = jsonify({'error': 'Project not found'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 404
        
        with open(project_file, 'r') as f:
            project_data = json.load(f)
        
        project = DataModelProject(**project_data)
        
        # Find and update metric view
        for i, mv in enumerate(project.metric_views):
            if mv.id == metric_view_id:
                # Update with new data
                updated_mv = MetricView(**{**mv.model_dump(), **data})
                updated_mv.updated_at = datetime.now()
                project.metric_views[i] = updated_mv
                project.updated_at = datetime.now()
                
                # Save project
                with open(project_file, 'w') as f:
                    json.dump(project.model_dump(), f, indent=2, default=str)
                
                logger.info(f"Updated metric view: {updated_mv.name}")
                
                response = jsonify({
                    'message': f'Metric view {updated_mv.name} updated successfully',
                    'metric_view': updated_mv.model_dump()
                })
                response.headers.add('Access-Control-Allow-Origin', '*')
                return response
        
        response = jsonify({'error': 'Metric view not found'})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 404
        
    except ValidationError as e:
        logger.error(f"Validation error updating metric view: {e}")
        response = jsonify({
            'error': 'Validation error',
            'details': [{'field': '.'.join(map(str, error['loc'])), 'message': error['msg']} for error in e.errors()]
        })
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 400
        
    except Exception as e:
        logger.error(f"Error updating metric view: {e}")
        response = jsonify({'error': str(e)})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 500

@data_modeling_bp.route('/project/<project_id>/metric_views/<metric_view_id>', methods=['DELETE'])
def delete_metric_view(project_id: str, metric_view_id: str):
    """Delete a metric view"""
    try:
        # Load project
        project_file = os.path.join(SAVED_PROJECTS_DIR, f"{project_id}.json")
        if not os.path.exists(project_file):
            response = jsonify({'error': 'Project not found'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 404
        
        with open(project_file, 'r') as f:
            project_data = json.load(f)
        
        project = DataModelProject(**project_data)
        
        # Find and remove metric view
        for i, mv in enumerate(project.metric_views):
            if mv.id == metric_view_id:
                removed_mv = project.metric_views.pop(i)
                
                # Remove related metric relationships
                project.metric_relationships = [
                    rel for rel in project.metric_relationships 
                    if rel.metric_view_id != metric_view_id
                ]
                
                project.updated_at = datetime.now()
                
                # Save project
                with open(project_file, 'w') as f:
                    json.dump(project.model_dump(), f, indent=2, default=str)
                
                logger.info(f"Deleted metric view: {removed_mv.name}")
                
                response = jsonify({
                    'message': f'Metric view {removed_mv.name} deleted successfully'
                })
                response.headers.add('Access-Control-Allow-Origin', '*')
                return response
        
        response = jsonify({'error': 'Metric view not found'})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 404
        
    except Exception as e:
        logger.error(f"Error deleting metric view: {e}")
        response = jsonify({'error': str(e)})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 500

@data_modeling_bp.route('/project/<project_id>/metric_views/<metric_view_id>/yaml', methods=['GET'])
def generate_metric_view_yaml(project_id: str, metric_view_id: str):
    """Generate YAML definition for a metric view"""
    try:
        # Load project
        project_file = os.path.join(SAVED_PROJECTS_DIR, f"{project_id}.json")
        if not os.path.exists(project_file):
            response = jsonify({'error': 'Project not found'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 404
        
        with open(project_file, 'r') as f:
            project_data = json.load(f)
        
        project = DataModelProject(**project_data)
        metric_view = project.get_metric_view_by_id(metric_view_id)
        
        if not metric_view:
            response = jsonify({'error': 'Metric view not found'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 404
        
        # Get source table
        source_table = project.get_table_by_id(metric_view.source_table_id)
        if not source_table:
            response = jsonify({'error': 'Source table not found'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 400
        
        # Generate YAML using Databricks integration
        client = get_sdk_client()
        unity_service = DatabricksUnityService(client)
        
        yaml_content = unity_service.generate_metric_view_yaml(metric_view, source_table)
        
        response = jsonify({
            'yaml': yaml_content,
            'metric_view_name': metric_view.name,
            'source_table': f"{source_table.catalog_name}.{source_table.schema_name}.{source_table.name}"
        })
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
        
    except Exception as e:
        logger.error(f"Error generating YAML: {e}")
        response = jsonify({'error': str(e)})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 500

@data_modeling_bp.route('/project/<project_id>/metric_views/<metric_view_id>/apply', methods=['POST'])
def apply_metric_view(project_id: str, metric_view_id: str):
    """Apply metric view to Databricks"""
    try:
        data = request.get_json()
        create_only = data.get('create_only', False)
        project_data = data.get('project', {})
        
        # Use project data from request (same as table apply) instead of loading from file
        if not project_data:
            response = jsonify({'error': 'Project data is required in request body'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 400
        
        # Debug: Check metric view joins before validation
        if 'metric_views' in project_data and project_data['metric_views']:
            for i, mv in enumerate(project_data['metric_views']):
                if mv.get('id') == metric_view_id:
                    logger.info(f"🔍 DEBUG: Found target metric view {i} with {len(mv.get('joins', []))} joins")
                    for j, join in enumerate(mv.get('joins', [])):
                        logger.info(f"🔍 DEBUG: Join {j}: keys={list(join.keys())}, name='{join.get('name', 'MISSING')}'")
        
        try:
            project = DataModelProject(**project_data)
        except ValidationError as ve:
            logger.error(f"Project validation failed: {ve}")
            response = jsonify({
                'error': f'Project validation failed: {str(ve)}',
                'validation_errors': ve.errors() if hasattr(ve, 'errors') else []
            })
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 422
        metric_view = project.get_metric_view_by_id(metric_view_id)
        
        if not metric_view:
            response = jsonify({'error': 'Metric view not found'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 404
        
        # Debug: Show metric view source table information
        logger.info(f"🔍 DEBUG: Metric view source_table_id: '{metric_view.source_table_id}'")
        logger.info(f"🔍 DEBUG: Project tables count: {len(project.tables) if project.tables else 0}")
        if project.tables:
            for i, table in enumerate(project.tables[:3]):  # Show first 3 tables
                logger.info(f"🔍 DEBUG: Project table {i}: id='{table.id}', name='{table.name}', catalog='{table.catalog_name}', schema='{table.schema_name}'")
        
        # Get source table - try by ID first, then by name, then use as direct reference
        source_table = project.get_table_by_id(metric_view.source_table_id)
        logger.info(f"🔍 DEBUG: Source table by ID lookup: {source_table.name if source_table else 'None'}")
        
        if not source_table:
            # Try to find by name if ID lookup fails
            source_table = project.get_table_by_name(metric_view.source_table_id)
            logger.info(f"🔍 DEBUG: Source table by name lookup: {source_table.name if source_table else 'None'}")
        
        # If source table found in project, use fully qualified name
        if source_table:
            # Use the table's effective catalog and schema
            table_catalog = project.get_effective_catalog(source_table.catalog_name)
            table_schema = project.get_effective_schema(source_table.schema_name)
            source_table_reference = f"{table_catalog}.{table_schema}.{source_table.name}"
            logger.info(f"🔍 DEBUG: Using project table reference: '{source_table_reference}'")
        else:
            # If source table not found in project, use the source_table_id as direct table reference
            # This handles cases where metric view references external tables (e.g., "catalog.schema.table")
            source_table_reference = metric_view.source_table_id
            logger.info(f"🔍 DEBUG: Using direct table reference: '{source_table_reference}'")
        
        # Use metric view's own catalog/schema for creation (metric views have their own location)
        effective_catalog = project.get_effective_catalog(metric_view.catalog_name)
        effective_schema = project.get_effective_schema(metric_view.schema_name)
        logger.info(f"🎯 Applying metric view '{metric_view.name}' to its own location: {effective_catalog}.{effective_schema}")
        logger.info(f"🔍 DEBUG: metric_view.catalog_name={metric_view.catalog_name}, metric_view.schema_name={metric_view.schema_name}")
        logger.info(f"🔍 DEBUG: effective_catalog={effective_catalog}, effective_schema={effective_schema}")
        
        logger.info(f"🎯 Final: Creating metric view with source table: '{source_table_reference}'")
        
        # Apply to Databricks
        client = get_sdk_client()
        unity_service = DatabricksUnityService(client)
        
        if create_only:
            success = unity_service.create_metric_view(
                metric_view, effective_catalog, effective_schema, source_table_reference
            )
        else:
            success = unity_service.alter_metric_view(
                metric_view, effective_catalog, effective_schema, source_table_reference
            )
        
        if success:
            response = jsonify({
                'success': True,
                'message': f'Metric view {metric_view.name} applied successfully to {effective_catalog}.{effective_schema}',
                'catalog_name': effective_catalog,
                'schema_name': effective_schema,
                'metric_view_name': metric_view.name
            })
        else:
            error_msg = unity_service.get_last_error() or 'Unknown error occurred'
            
            # Check if it's a user error (table/view not found, permission issues, etc.) vs server error
            user_error_indicators = [
                'TABLE_OR_VIEW_NOT_FOUND',
                'SCHEMA_NOT_FOUND', 
                'CATALOG_NOT_FOUND',
                'PERMISSION_DENIED',
                'ACCESS_DENIED',
                'INSUFFICIENT_PRIVILEGES'
            ]
            
            is_user_error = any(indicator in error_msg for indicator in user_error_indicators)
            status_code = 400 if is_user_error else 500
            
            logger.warning(f"Metric view apply failed ({'user error' if is_user_error else 'server error'}): {error_msg}")
            
            response = jsonify({
                'success': False,
                'error': error_msg,
                'error_type': 'user_error' if is_user_error else 'server_error'
            })
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, status_code
        
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
        
    except Exception as e:
        logger.error(f"Error applying metric view: {e}")
        response = jsonify({'error': str(e)})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 500

# ===== TRADITIONAL VIEW ROUTES =====

@data_modeling_bp.route('/project/<project_id>/traditional_views/<traditional_view_id>/apply', methods=['POST'])
def apply_traditional_view(project_id: str, traditional_view_id: str):
    """Apply traditional view to Databricks"""
    try:
        data = request.get_json()
        create_only = data.get('create_only', False)
        project_data = data.get('project', {})
        
        # Use project data from request (same as metric view apply)
        if not project_data:
            response = jsonify({'error': 'Project data is required in request body'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 400
        
        try:
            project = DataModelProject(**project_data)
        except ValidationError as ve:
            logger.error(f"Project validation failed: {ve}")
            response = jsonify({
                'error': f'Project validation failed: {str(ve)}',
                'validation_errors': ve.errors() if hasattr(ve, 'errors') else []
            })
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 422
            
        # Find traditional view
        traditional_view = None
        for tv in project.traditional_views:
            if tv.id == traditional_view_id:
                traditional_view = tv
                break
        
        if not traditional_view:
            response = jsonify({'error': 'Traditional view not found'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 404
        
        # Initialize Databricks service
        client = get_sdk_client()
        if not client:
            response = jsonify({'error': 'Failed to connect to Databricks'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 500
        
        unity_service = DatabricksUnityService(client)
        
        # Use traditional view's effective catalog/schema
        effective_catalog = project.get_effective_catalog(traditional_view.catalog_name)
        effective_schema = project.get_effective_schema(traditional_view.schema_name)
        logger.info(f"🎯 Applying traditional view '{traditional_view.name}' to {effective_catalog}.{effective_schema}")
        
        # Apply traditional view
        success = unity_service.create_traditional_view(
            traditional_view, 
            effective_catalog, 
            effective_schema
        )
        
        if success:
            response = jsonify({
                'success': True,
                'message': f'Traditional view {traditional_view.name} applied successfully to {effective_catalog}.{effective_schema}',
                'catalog_name': effective_catalog,
                'schema_name': effective_schema,
                'view_name': traditional_view.name
            })
        else:
            error_msg = unity_service.get_last_error() or 'Unknown error occurred'
            
            # Parse and improve error messages for better user experience
            user_friendly_msg = _parse_databricks_error_message(error_msg, traditional_view, project)
            
            # Check if it's a user error vs server error
            user_error_indicators = [
                'TABLE_OR_VIEW_NOT_FOUND',
                'SCHEMA_NOT_FOUND', 
                'CATALOG_NOT_FOUND',
                'PERMISSION_DENIED',
                'ACCESS_DENIED',
                'INSUFFICIENT_PRIVILEGES',
                'PARSE_SYNTAX_ERROR',
                'INVALID_PARAMETER_VALUE',
                'EXPECT_VIEW_NOT_TABLE'
            ]
            
            is_user_error = any(indicator in error_msg for indicator in user_error_indicators)
            status_code = 400 if is_user_error else 500
            
            logger.warning(f"Traditional view apply failed ({'user error' if is_user_error else 'server error'}): {error_msg}")
            
            response = jsonify({
                'success': False,
                'error': user_friendly_msg,
                'original_error': error_msg,
                'error_type': 'user_error' if is_user_error else 'server_error'
            })
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, status_code
        
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
        
    except ValueError as ve:
        # Handle validation errors (from SQL validation)
        logger.warning(f"Traditional view validation error: {ve}")
        response = jsonify({
            'error': str(ve),
            'error_type': 'validation_error'
        })
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 400
    except Exception as e:
        logger.error(f"Error applying traditional view: {e}")
        response = jsonify({'error': str(e)})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 500

@data_modeling_bp.route('/project/<project_id>/traditional_views/<traditional_view_id>/generate_ddl', methods=['POST'])
def generate_traditional_view_ddl(project_id: str, traditional_view_id: str):
    """Generate DDL for traditional view"""
    try:
        data = request.get_json()
        project_data = data.get('project', {})
        
        # Use project data from request
        if not project_data:
            response = jsonify({'error': 'Project data is required in request body'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 400
        
        try:
            project = DataModelProject(**project_data)
        except ValidationError as ve:
            logger.error(f"Project validation failed: {ve}")
            response = jsonify({
                'error': f'Project validation failed: {str(ve)}',
                'validation_errors': ve.errors() if hasattr(ve, 'errors') else []
            })
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 422
            
        # Find traditional view
        traditional_view = None
        for tv in project.traditional_views:
            if tv.id == traditional_view_id:
                traditional_view = tv
                break
        
        if not traditional_view:
            response = jsonify({'error': 'Traditional view not found'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 404
        
        # Use traditional view's effective catalog/schema
        effective_catalog = project.get_effective_catalog(traditional_view.catalog_name)
        effective_schema = project.get_effective_schema(traditional_view.schema_name)
        full_view_name = f"{effective_catalog}.{effective_schema}.{traditional_view.name}"
        logger.info(f"🎯 Generating DDL for traditional view '{traditional_view.name}' in {effective_catalog}.{effective_schema}")
        
        # Build DDL with proper Databricks syntax
        ddl_parts = [
            f"-- Traditional View: {traditional_view.name}",
            f"-- Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            ""
        ]
        
        # Start CREATE VIEW statement
        create_statement = f"CREATE OR REPLACE VIEW {full_view_name}"
        
        # Extract COMMENT from SQL query if present, otherwise use description
        comment_to_use = None
        sql_query = traditional_view.sql_query.strip()
        
        # Check if SQL already contains COMMENT clause
        import re
        comment_match = re.search(r'COMMENT\s+[\"\'](.*?)[\"\']', sql_query, re.IGNORECASE)
        if comment_match:
            comment_to_use = comment_match.group(1)
            logger.info(f"Using COMMENT from SQL: {comment_to_use}")
        elif traditional_view.description and traditional_view.description.strip():
            comment_to_use = traditional_view.description
            logger.info(f"Using description as COMMENT: {comment_to_use}")
        
        # Add COMMENT if we have one
        if comment_to_use:
            escaped_comment = comment_to_use.replace("'", "\\'")
            create_statement += f"\nCOMMENT '{escaped_comment}'"
        
        # Add TBLPROPERTIES for tags if they exist
        if traditional_view.tags:
            properties = []
            for key, value in traditional_view.tags.items():
                # Escape single quotes in tag values
                escaped_value = str(value).replace("'", "\\'")
                properties.append(f"'{key}' = '{escaped_value}'")
            
            if properties:
                create_statement += f"\nTBLPROPERTIES ({', '.join(properties)})"
        
        # Validate and qualify SQL query using view's effective catalog/schema
        validated_sql = _validate_traditional_view_sql(traditional_view.sql_query)
        qualified_sql = _qualify_table_references(validated_sql, effective_catalog, effective_schema)
        
        # Add AS clause with validated and qualified SQL query
        create_statement += f"\nAS\n{qualified_sql}"
        
        # Ensure SQL query ends with semicolon
        if not create_statement.strip().endswith(';'):
            create_statement += ";"
        
        ddl_parts.append(create_statement)
        ddl = "\n".join(ddl_parts)
        
        response = jsonify({
            'success': True,
            'ddl': ddl,
            'view_name': traditional_view.name,
            'full_name': full_view_name
        })
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
        
    except ValueError as ve:
        # Handle validation errors (from SQL validation)
        logger.warning(f"Traditional view DDL validation error: {ve}")
        response = jsonify({
            'error': str(ve),
            'error_type': 'validation_error'
        })
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 400
    except Exception as e:
        logger.error(f"Error generating traditional view DDL: {e}")
        response = jsonify({'error': str(e)})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 500

@data_modeling_bp.route('/project/<project_id>/metric_relationships', methods=['POST'])
def create_metric_relationship(project_id: str):
    """Create a relationship between a table and metric view"""
    try:
        data = request.get_json()
        
        # Load project
        project_file = os.path.join(SAVED_PROJECTS_DIR, f"{project_id}.json")
        if not os.path.exists(project_file):
            response = jsonify({'error': 'Project not found'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 404
        
        with open(project_file, 'r') as f:
            project_data = json.load(f)
        
        project = DataModelProject(**project_data)
        
        # Create metric relationship
        relationship = MetricSourceRelationship(**data)
        
        # Validate source table and metric view exist
        source_table = project.get_table_by_id(relationship.source_table_id)
        metric_view = project.get_metric_view_by_id(relationship.metric_view_id)
        
        if not source_table:
            response = jsonify({'error': 'Source table not found'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 400
        
        if not metric_view:
            response = jsonify({'error': 'Metric view not found'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response, 400
        
        # Add to project
        project.metric_relationships.append(relationship)
        project.updated_at = datetime.now()
        
        # Save project
        with open(project_file, 'w') as f:
            json.dump(project.model_dump(), f, indent=2, default=str)
        
        logger.info(f"Created metric relationship: {source_table.name} -> {metric_view.name}")
        
        response = jsonify({
            'message': 'Metric relationship created successfully',
            'relationship': relationship.model_dump()
        })
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
        
    except ValidationError as e:
        logger.error(f"Validation error creating metric relationship: {e}")
        response = jsonify({
            'error': 'Validation error',
            'details': [{'field': '.'.join(map(str, error['loc'])), 'message': error['msg']} for error in e.errors()]
        })
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 400
        
    except Exception as e:
        logger.error(f"Error creating metric relationship: {e}")
        response = jsonify({'error': str(e)})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 500

