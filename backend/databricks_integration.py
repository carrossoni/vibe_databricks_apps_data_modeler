import json
import logging
import time
import uuid
from typing import Dict, List, Optional, Tuple, Any, Union
from databricks.sdk import WorkspaceClient
# Import what we need for table info retrieval and constraint reading
from databricks.sdk.service.catalog import (
    TableInfo, ColumnInfo, TableType, TableConstraint
)
from databricks.sdk.service.sql import StatementState
# Note: Not importing PrimaryKeyConstraint, ForeignKeyConstraint for table creation
# as we're using SQL execution instead of Unity Catalog API for constraint creation

from models.data_modeling import (
    DataModelProject, DataTable, TableField, DataModelRelationship,
    DatabricksDataType, ForeignKeyReference, MetricView, MetricViewDimension,
    MetricViewMeasure, MetricViewJoin, MetricSourceRelationship, TraditionalView
)

# Configure logging with proper levels
import os
log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
if hasattr(logging, log_level):
    logging.getLogger().setLevel(getattr(logging, log_level))


logger = logging.getLogger(__name__)


class DatabricksUnityService:
    """Service for interacting with Databricks Unity Catalog"""
    
    def __init__(self, workspace_client: WorkspaceClient):
        self.client = workspace_client
        self._last_error = None
        self._warnings = []
    
    def get_last_error(self) -> str:
        """Get the last error message"""
        return self._last_error
    
    def clear_last_error(self):
        """Clear the last error message"""
        self._last_error = None
    
    def get_warnings(self) -> list:
        """Get all warning messages"""
        return self._warnings.copy()
    
    def clear_warnings(self):
        """Clear all warning messages"""
        self._warnings = []
    
    def add_warning(self, warning: str):
        """Add a warning message"""
        self._warnings.append(warning)
    
    def list_catalogs(self) -> List[Dict[str, Any]]:
        """List all available catalogs"""
        try:
            catalogs = []
            for catalog in self.client.catalogs.list():
                catalogs.append({
                    'name': catalog.name,
                    'comment': catalog.comment,
                    'created_at': catalog.created_at,
                    'updated_at': catalog.updated_at,
                    'owner': catalog.owner,
                    'type': catalog.catalog_type.value if catalog.catalog_type else None
                })
            return catalogs
        except Exception as e:
            logger.error(f"Error listing catalogs: {e}")
            raise
    
    def list_schemas(self, catalog_name: str) -> List[Dict[str, Any]]:
        """List all schemas in a catalog"""
        try:
            schemas = []
            for schema in self.client.schemas.list(catalog_name=catalog_name):
                schemas.append({
                    'name': schema.name,
                    'catalog_name': schema.catalog_name,
                    'comment': schema.comment,
                    'created_at': schema.created_at,
                    'updated_at': schema.updated_at,
                    'owner': schema.owner
                })
            return schemas
        except Exception as e:
            logger.error(f"Error listing schemas for catalog {catalog_name}: {e}")
            raise
    
    def list_tables(self, catalog_name: str, schema_name: str) -> List[Dict[str, Any]]:
        """List all tables in a schema (excluding views)"""
        try:
            tables = []
            for table in self.client.tables.list(catalog_name=catalog_name, schema_name=schema_name):
                # Filter out views - only include actual tables
                if table.table_type and table.table_type.value not in ['VIEW', 'MATERIALIZED_VIEW']:
                    tables.append({
                        'name': table.name,
                        'catalog_name': table.catalog_name,
                        'schema_name': table.schema_name,
                        'table_type': table.table_type.value if table.table_type else None,
                        'comment': table.comment,
                        'created_at': table.created_at,
                        'updated_at': table.updated_at,
                        'owner': table.owner,
                        'storage_location': table.storage_location,
                        'data_source_format': table.data_source_format.value if table.data_source_format else None
                    })
            return tables
        except Exception as e:
            logger.error(f"Error listing tables for {catalog_name}.{schema_name}: {e}")
            raise
    
    def get_table_info(self, catalog_name: str, schema_name: str, table_name: str) -> Optional[TableInfo]:
        """Get detailed information about a specific table"""
        try:
            full_name = f"{catalog_name}.{schema_name}.{table_name}"
            return self.client.tables.get(full_name=full_name)
        except Exception as e:
            logger.error(f"Error getting table info for {full_name}: {e}")
            return None
    
    def check_liquid_clustering_enabled(self, catalog_name: str, schema_name: str, table_name: str) -> bool:
        """Check if a table has automatic liquid clustering enabled"""
        try:
            full_name = f"{catalog_name}.{schema_name}.{table_name}"
            
            # Get warehouse ID for SQL execution
            warehouse_id = self._get_warehouse_id()
            if not warehouse_id:
                logger.warning(f"⚠️ No warehouse available for SQL queries, cannot check clustering for {full_name}")
                return False
            
            # Use DESCRIBE TABLE EXTENDED to get table properties
            sql = f"DESCRIBE TABLE EXTENDED {full_name}"
            logger.info(f"🔍 Checking liquid clustering for {full_name}")
            
            result = self.client.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=sql,
                wait_timeout="30s"
            )
            
            if result.status.state != StatementState.SUCCEEDED:
                logger.error(f"SQL query failed: {result.status}")
                return False
            
            # Parse the results to find table properties
            if result.result and result.result.data_array:
                logger.info(f"🔍 DESCRIBE TABLE EXTENDED results for {full_name}:")
                for row in result.result.data_array:
                    if len(row) >= 2:
                        col_name = str(row[0]).strip() if row[0] else ""
                        col_value = str(row[1]).strip() if row[1] else ""
                        logger.info(f"   {col_name}: {col_value}")
                        
                        # Look for clusterByAuto in table properties (case insensitive)
                        if col_name.lower() == "clusterbyauto":
                            value = col_value.lower()
                            logger.info(f"🔗 Found clusterByAuto: {value}")
                            return value == "true"
                        
                        # Also check if it's in the table properties section
                        # Sometimes it appears as a key-value pair in a properties string
                        if "clusterbyauto" in col_value.lower():
                            logger.info(f"🔗 Found clusterByAuto in properties string: {col_value}")
                            # Parse the properties string to find clusterByAuto=true
                            if "clusterbyauto=true" in col_value.lower() or "clusterbyauto:true" in col_value.lower():
                                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking liquid clustering for {full_name}: {e}")
            return False
    
    def get_table_column_details_via_sql(self, catalog_name: str, schema_name: str, table_name: str) -> Dict[str, Dict[str, Any]]:
        """Get detailed column information using SQL DESCRIBE TABLE"""
        try:
            full_name = f"{catalog_name}.{schema_name}.{table_name}"
            
            # Get warehouse ID for SQL execution
            warehouse_id = self._get_warehouse_id()
            if not warehouse_id:
                logger.warning(f"⚠️ No warehouse available for SQL queries, skipping detailed column info for {full_name}")
                return {}
            
            # Use DESCRIBE TABLE to get detailed column information
            sql = f"DESCRIBE TABLE {full_name}"
            logger.info(f"🔍 Executing SQL: {sql} on warehouse {warehouse_id}")
            
            result = self.client.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=sql,
                wait_timeout="30s"
            )
            
            if result.status.state != StatementState.SUCCEEDED:
                logger.error(f"SQL query failed: {result.status}")
                return {}
            
            column_details = {}
            if result.result and result.result.data_array:
                for row in result.result.data_array:
                    if len(row) >= 2:
                        col_name = row[0]
                        col_type = row[1]
                        column_details[col_name] = {
                            'type_text': col_type,
                            'nullable': len(row) > 2 and row[2] != 'NOT NULL'
                        }
                        logger.info(f"📋 SQL Column '{col_name}': {col_type}")
            
            return column_details
            
        except Exception as e:
            logger.error(f"Error getting column details via SQL for {full_name}: {e}")
            return {}
    
    def get_table_constraints(self, catalog_name: str, schema_name: str, table_name: str) -> List[TableConstraint]:
        """Get constraints for a specific table"""
        try:
            full_name = f"{catalog_name}.{schema_name}.{table_name}"
            
            # Use tables.get() to get table information including constraints
            table_info = self.client.tables.get(full_name=full_name)
            
            # Extract constraints from table info
            constraints = []
            if hasattr(table_info, 'table_constraints') and table_info.table_constraints:
                constraints = table_info.table_constraints
                logger.info(f"📋 Found {len(constraints)} constraints for table {full_name}")
            else:
                logger.info(f"📋 No constraints found for table {full_name}")
            
            return constraints
        except Exception as e:
            logger.error(f"Error getting table info for {full_name}: {e}")
            return []
    
    def import_existing_tables_with_progress(self, catalog_name: str, schema_name: str, 
                                           table_names: List[str], session_id: str, existing_tables: List[dict] = None) -> DataModelProject:
        """Import existing tables with progress streaming"""
        print(f"🚀 STREAMING IMPORT CALLED - catalog={catalog_name}, schema={schema_name}, tables={table_names}")
        if existing_tables:
            logger.info(f"Processing {len(existing_tables)} existing tables for relationship creation")
        from data_modeling_routes import send_progress_update, create_progress_session
        
        # Create progress session
        create_progress_session(session_id)
        
        try:
            # Send initial progress
            send_progress_update(session_id, {
                'type': 'started',
                'total_tables': len(table_names),
                'table_names': table_names,
                'catalog_name': catalog_name,
                'schema_name': schema_name
            })
            
            # Import tables with progress updates
            project = DataModelProject(
                name=f"Imported from {catalog_name}.{schema_name}",
                description=f"Tables imported from {catalog_name}.{schema_name}",
                catalog_name=catalog_name,
                schema_name=schema_name,
                tables=[],
                relationships=[],
                metric_views=[],
                traditional_views=[],
                metric_relationships=[]
            )
            
            # Track all tables to import using full qualified names (like import_existing_tables)
            all_tables_to_import = set(f"{catalog_name}.{schema_name}.{name}" for name in table_names)
            tables_processed = set()
            table_id_map = {}  # Maps full_qualified_name -> table_id for reference resolution
            position_index = 0
            results = []
            
            logger.info(f"🔍 Starting import with initial tables: {table_names}")
            
            # Process tables iteratively to follow FK references
            while all_tables_to_import - tables_processed:
                current_batch = list(all_tables_to_import - tables_processed)
                logger.info(f"🔄 Processing batch: {current_batch}")
                
                for i, full_table_name in enumerate(current_batch):
                    if full_table_name in tables_processed:
                        continue
                    
                    # Parse catalog.schema.table
                    parts = full_table_name.split('.')
                    if len(parts) != 3:
                        logger.warning(f"⚠️ Invalid table name format: {full_table_name}, expected catalog.schema.table")
                        tables_processed.add(full_table_name)
                        continue
                    
                    table_catalog, table_schema, table_name = parts
                    logger.info(f"📋 Processing table: {table_name} from {table_catalog}.{table_schema}")
                    
                    # Calculate progress
                    progress = int((len(tables_processed) / len(all_tables_to_import)) * 100)
                    
                    # Send table started update
                    send_progress_update(session_id, {
                        'type': 'table_started',
                        'table_name': table_name,
                        'progress': progress
                    })
                    
                    logger.info(f"📋 Processing table: {table_name}")
                    
                    try:
                        # Get table info and constraints
                        table_info = self.get_table_info(table_catalog, table_schema, table_name)
                        if not table_info:
                            logger.warning(f"⚠️ Could not get info for table {table_name}")
                            results.append({
                                'table_name': table_name,
                                'success': False,
                                'error': 'Could not retrieve table information'
                            })
                            tables_processed.add(full_table_name)
                            continue
                        
                        # Get detailed column information via SQL
                        column_details = self.get_table_column_details_via_sql(table_catalog, table_schema, table_name)
                        
                        # Get constraints to detect PK/FK
                        constraints = self.get_table_constraints(table_catalog, table_schema, table_name)
                        
                        # Convert table with PK/FK detection
                        data_table = self._convert_table_info_to_data_table_with_constraints(
                            table_info, constraints, position_index, column_details
                        )
                        
                        # Set source catalog and schema on imported table (use actual source, not project defaults)
                        data_table.catalog_name = table_catalog
                        data_table.schema_name = table_schema
                        project.tables.append(data_table)
                        table_id_map[full_table_name] = data_table.id
                        tables_processed.add(full_table_name)
                        position_index += 1
                        
                        # Find FK-referenced tables and add them to import list
                        referenced_tables = self._extract_referenced_tables_from_constraints(
                            constraints, table_catalog, table_schema
                        )
                        
                        for ref_table in referenced_tables:
                            if ref_table not in all_tables_to_import:
                                all_tables_to_import.add(ref_table)
                                logger.info(f"🔗 Added referenced table to import: {ref_table}")
                        
                        # Send table completed update
                        send_progress_update(session_id, {
                            'type': 'table_completed',
                            'table_name': table_name,
                            'progress': int(((len(tables_processed)) / len(all_tables_to_import)) * 100),
                            'result': {
                                'table_name': table_name,
                                'success': True,
                                'columns_count': len(data_table.fields),
                                'constraints_count': len(constraints)
                            }
                        })
                        
                        results.append({
                            'table_name': table_name,
                            'success': True,
                            'columns_count': len(data_table.fields),
                            'constraints_count': len(constraints)
                        })
                        
                    except Exception as e:
                        logger.error(f"Error importing table {table_name}: {e}")
                        results.append({
                            'table_name': table_name,
                            'success': False,
                            'error': str(e)
                        })
                        tables_processed.add(full_table_name)
                        
                        # Send table completed with error
                        send_progress_update(session_id, {
                            'type': 'table_completed',
                            'table_name': table_name,
                            'progress': int(((len(tables_processed)) / len(all_tables_to_import)) * 100),
                            'result': {
                                'table_name': table_name,
                                'success': False,
                                'error': str(e)
                            }
                        })
            
            # Convert temporary FK references to proper object format
            self._convert_temporary_fk_references(project.tables, table_id_map)
            
            # Handle existing tables for relationship creation (similar to non-streaming version)
            existing_table_objects = []
            if existing_tables:
                logger.info(f"Processing {len(existing_tables)} existing tables for relationship creation")
                for table_data in existing_tables:
                    try:
                        from models.data_modeling import DataTable
                        
                        # Transform type_parameters from dict to string format (like frontend does)
                        if 'fields' in table_data:
                            for field in table_data['fields']:
                                if 'type_parameters' in field and isinstance(field['type_parameters'], dict):
                                    params = field['type_parameters']
                                    if 'precision' in params:
                                        # DECIMAL format: "precision,scale" or "precision"
                                        if 'scale' in params:
                                            field['type_parameters'] = f"{params['precision']},{params['scale']}"
                                        else:
                                            field['type_parameters'] = str(params['precision'])
                                    else:
                                        # For other dict formats, stringify
                                        field['type_parameters'] = str(params)
                        
                        existing_table = DataTable(**table_data)
                        existing_table_objects.append(existing_table)
                        
                        # Add to table_id_map for relationship creation
                        # Use project defaults when existing table has None catalog/schema
                        table_catalog = table_data.get('catalog_name') or catalog_name
                        table_schema = table_data.get('schema_name') or schema_name
                        table_name = table_data.get('name')
                        if table_name:
                            full_name = f"{table_catalog}.{table_schema}.{table_name}"
                            table_id_map[full_name] = existing_table.id
                    except Exception as e:
                        logger.warning(f"Could not convert existing table data to DataTable: {e}")
            
            # Combine existing and imported tables for relationship creation
            all_tables_for_relationships = existing_table_objects + project.tables
            
            # Extract relationships from constraints
            all_relationships = []
            logger.info(f"Creating relationships for {len(project.tables)} imported tables with {len(existing_table_objects)} existing tables in context")
            for table in project.tables:
                # Use table's actual catalog/schema, not project defaults
                table_catalog = table.catalog_name or catalog_name
                table_schema = table.schema_name or schema_name
                
                constraints = self.get_table_constraints(table_catalog, table_schema, table.name)
                # Use full qualified name for table_id_map lookup
                table_full_name = f"{table_catalog}.{table_schema}.{table.name}"
                table_relationships = self._extract_relationships_from_constraints(
                    constraints, table_id_map, table_full_name, all_tables_for_relationships
                )
                all_relationships.extend(table_relationships)
                
                # Log relationship details for debugging
                for rel in table_relationships:
                    logger.info(f"🔗 Created relationship: {rel.source_table_id} -> {rel.target_table_id} (ID: {rel.id})")
            
            project.relationships = all_relationships
            logger.info(f"✅ Total relationships created: {len(all_relationships)}")
            
            # Send completion update
            send_progress_update(session_id, {
                'type': 'completed',
                'progress': 100,
                'results': results,
                'catalog_name': catalog_name,
                'schema_name': schema_name
            })
            
            # Give frontend a moment to process the completion message before closing
            import time
            time.sleep(0.1)
            # Send end signal to close the SSE stream
            send_progress_update(session_id, None)
            
            return project
            
        except Exception as e:
            logger.error(f"Error in import with progress: {e}")
            send_progress_update(session_id, {
                'type': 'error',
                'message': str(e)
            })
            # Send end signal even on error
            import time
            time.sleep(0.1)
            send_progress_update(session_id, None)
            raise
    
    def import_existing_tables(self, catalog_name: str, schema_name: str, 
                             table_names: List[str]) -> DataModelProject:
        """Import existing tables from Databricks into a data model project with recursive FK following"""
        try:
            project = DataModelProject(
                name=f"{catalog_name}_{schema_name}_import",
                description=f"Imported from {catalog_name}.{schema_name}",
                catalog_name=catalog_name,
                schema_name=schema_name
            )
            
            # Track all tables to import (including FK-referenced tables) using full qualified names
            # Convert table names to full qualified names for proper tracking
            all_tables_to_import = set(f"{catalog_name}.{schema_name}.{name}" for name in table_names)
            tables_processed = set()
            table_id_map = {}  # Maps full_qualified_name -> table_id for reference resolution
            position_index = 0
            
            logger.info(f"🔍 Starting import with initial tables: {table_names}")
            
            # Process tables iteratively to follow FK references
            while all_tables_to_import - tables_processed:
                current_batch = list(all_tables_to_import - tables_processed)
                logger.info(f"🔄 Processing batch: {current_batch}")
                
                for full_table_name in current_batch:
                    if full_table_name in tables_processed:
                        continue
                    
                    # Parse catalog.schema.table
                    parts = full_table_name.split('.')
                    if len(parts) != 3:
                        logger.warning(f"⚠️ Invalid table name format: {full_table_name}, expected catalog.schema.table")
                        tables_processed.add(full_table_name)
                        continue
                    
                    table_catalog, table_schema, table_name = parts
                    logger.info(f"📋 Processing table: {table_name} from {table_catalog}.{table_schema}")
                    
                    # Check if this table already exists in the project (same catalog.schema.name)
                    existing_table = None
                    for existing in project.tables:
                        existing_catalog = existing.catalog_name or catalog_name  # fallback to project catalog
                        existing_schema = existing.schema_name or schema_name    # fallback to project schema
                        if (existing.name == table_name and 
                            existing_catalog == table_catalog and 
                            existing_schema == table_schema):
                            existing_table = existing
                            break
                    
                    if existing_table:
                        logger.info(f"📋 Table {full_table_name} already exists in project, skipping")
                        table_id_map[full_table_name] = existing_table.id
                        tables_processed.add(full_table_name)
                        continue
                    
                    # Get table info and constraints
                    table_info = self.get_table_info(table_catalog, table_schema, table_name)
                    if not table_info:
                        logger.warning(f"⚠️ Could not get info for table {table_name}")
                        tables_processed.add(full_table_name)
                        continue
                    
                    # Get detailed column information via SQL
                    column_details = self.get_table_column_details_via_sql(table_catalog, table_schema, table_name)
                    
                    # Get constraints to detect PK/FK
                    constraints = self.get_table_constraints(table_catalog, table_schema, table_name)
                    
                    # Convert table with PK/FK detection
                    data_table = self._convert_table_info_to_data_table_with_constraints(
                        table_info, constraints, position_index, column_details
                    )
                    
                    # Set source catalog and schema on imported table (use actual source, not project defaults)
                    data_table.catalog_name = table_catalog
                    data_table.schema_name = table_schema
                    project.tables.append(data_table)
                    table_id_map[full_table_name] = data_table.id
                    tables_processed.add(full_table_name)
                    position_index += 1
                    
                    # Find FK-referenced tables and add them to import list
                    referenced_tables = self._extract_referenced_tables_from_constraints(
                        constraints, table_catalog, table_schema
                    )
                    
                    for ref_table_full_name in referenced_tables:
                        if ref_table_full_name not in all_tables_to_import:
                            logger.info(f"🔗 Found FK reference to {ref_table_full_name}, adding to import list")
                            all_tables_to_import.add(ref_table_full_name)
            
            # Convert temporary FK references to proper ForeignKeyReference objects
            logger.info(f"🔄 Converting FK references for {len(project.tables)} tables")
            self._convert_temporary_fk_references(project.tables, table_id_map)
            
            # Create relationships after all tables are imported
            logger.info(f"🔗 Creating relationships for {len(project.tables)} tables")
            for table in project.tables:
                # Use table's actual catalog/schema, not project defaults
                table_catalog = table.catalog_name or catalog_name
                table_schema = table.schema_name or schema_name
                table_name = table.name
                
                constraints = self.get_table_constraints(table_catalog, table_schema, table_name)
                # Use full qualified name for table_id_map lookup
                table_full_name = f"{table_catalog}.{table_schema}.{table_name}"
                relationships = self._extract_relationships_from_constraints(
                    constraints, table_id_map, table_full_name, project.tables
                )
                project.relationships.extend(relationships)
            
            logger.info(f"✅ Import complete: {len(project.tables)} tables, {len(project.relationships)} relationships")
            return project
            
        except Exception as e:
            logger.error(f"Error importing tables: {e}")
            raise
    
    def create_table_from_model(self, data_table: DataTable, catalog_name: str, schema_name: str, source_catalog: str = None, warehouse_id: str = None, all_tables: List[DataTable] = None) -> dict:
        """Create or update a table in Databricks using SQL execution"""
        try:
            full_name = f"{catalog_name}.{schema_name}.{data_table.name}"
            logger.info(f"🚀 Processing table {full_name}")
            print(f"🚀🚀🚀 CREATE_TABLE_FROM_MODEL CALLED FOR: {full_name}")
            print(f"   📋 Table ID: {data_table.id}")
            print(f"   📋 Table fields count: {len(data_table.fields)}")
            print(f"   📋 Warehouse ID: {warehouse_id}")
            print(f"   📋 Source catalog: {source_catalog}")
            
            # Check if table exists to determine if we're creating or altering
            table_exists = self._table_exists(catalog_name, schema_name, data_table.name)
            print(f"   📋 Table exists: {table_exists}")
            if table_exists:
                print(f"   🔄 WILL GENERATE ALTER TABLE DDL")
            else:
                print(f"   🆕 WILL GENERATE CREATE TABLE DDL")

            # Check if client is available (None means no credentials)
            if not self.client:
                logger.info(f"🚀 No Databricks client - DDL would be executed for {full_name} (demo mode)")
                # Generate DDL for demo purposes
                ddl = self.generate_ddl_for_table(data_table, catalog_name, schema_name, source_catalog, all_tables)
                if ddl:
                    logger.info(f"📋 Generated DDL for table {full_name}:")
                    for line in ddl.split('\n'):
                        if line.strip():
                            logger.info(f"   {line}")
                
                # In demo mode, simulate tag processing
                tag_changes = self._generate_tag_changes(data_table, catalog_name, schema_name, None)
                tag_result = {
                    'tag_changes_count': len(tag_changes) if tag_changes else 0,
                    'tag_success': True,
                    'tag_details': []
                }
                
                if tag_changes:
                    table_tags = [tc for tc in tag_changes if tc['entity_type'] == 'tables']
                    column_tags = [tc for tc in tag_changes if tc['entity_type'] == 'columns']
                    tag_result['table_tags_count'] = len(table_tags)
                    tag_result['column_tags_count'] = len(column_tags)
                    
                    for change in tag_changes:
                        entity_name = change['entity_id'].split('.')[-1] if change['entity_type'] == 'columns' else 'table'
                        tag_result['tag_details'].append({
                            'entity_type': change['entity_type'],
                            'entity_name': entity_name,
                            'tag_key': change['tag_key'],
                            'action': change['action'],
                            'tag_value': change.get('tag_value', '')
                        })
                else:
                    tag_result['table_tags_count'] = 0
                    tag_result['column_tags_count'] = 0
                
                return {
                    'success': True,
                    'ddl_executed': True,  # Simulated execution
                    'tags': tag_result
                }
            
            # Skip tables with no fields
            if not data_table.fields or len(data_table.fields) == 0:
                logger.info(f"⏭️ Skipping table {data_table.name} - no fields defined")
                return {
                    'success': True,
                    'ddl_executed': False,
                    'tags': {'tag_changes_count': 0, 'tag_success': True, 'tag_details': []}
                }
            
            # Generate DDL for the table (without tags for apply process)
            if table_exists:
                ddl = self._generate_alter_table_ddl(data_table, catalog_name, schema_name, all_tables, include_tags=False)
            else:
                ddl = self._generate_create_table_ddl(data_table, catalog_name, schema_name, all_tables, include_tags=False)
            
            if not ddl or not ddl.strip():
                logger.warning(f"⚠️ No DDL generated for table {data_table.name}")
                return True
            
            # Execute DDL using SQL statements API
            try:
                logger.info(f"🔧 Executing DDL for table {full_name}")
                logger.info(f"📋 DDL Statement:")
                for line in ddl.split('\n'):
                    if line.strip():
                        logger.info(f"   {line}")
                
                # Check if table DDL is already up to date (no actual DDL to execute)
                ddl_execution_needed = True
                if "is already up to date" in ddl or ddl.strip().startswith("--"):
                    logger.info(f"✅ Table {full_name} DDL is already up to date - skipping DDL execution")
                    ddl_execution_needed = False
                else:
                    # Execute DDL statements - split and execute individually for ALTER statements
                    ddl_statements = [stmt.strip() for stmt in ddl.split(';') if stmt.strip() and not stmt.strip().startswith('--')]
                    
                    # If no valid DDL statements after filtering, table DDL is up to date
                    if not ddl_statements:
                        logger.info(f"✅ No DDL statements to execute for {full_name} - table DDL is up to date")
                        ddl_execution_needed = False
                
                # Execute DDL if needed
                if ddl_execution_needed:
                    # Get warehouse ID for SQL execution
                    selected_warehouse_id = self._get_warehouse_id(warehouse_id)
                    if not selected_warehouse_id:
                        logger.error(f"❌ No SQL warehouse available for DDL execution")
                        self._last_error = "No SQL warehouse available for DDL execution"
                        return False
                    
                    for i, statement in enumerate(ddl_statements):
                        logger.info(f"🔧 Executing DDL statement {i+1}/{len(ddl_statements)}: {statement[:50]}...")
                        
                        response = self.client.statement_execution.execute_statement(
                            warehouse_id=selected_warehouse_id,
                            statement=statement,
                            wait_timeout="50s"  # Maximum allowed timeout (5-50 seconds)
                        )
                        
                        # Poll for completion of this statement
                        statement_id = response.statement_id
                        max_polls = 60  # 5 minutes with 5-second intervals
                        poll_count = 0
                        
                        while response.status.state in [StatementState.PENDING, StatementState.RUNNING] and poll_count < max_polls:
                            logger.info(f"⏳ DDL statement {i+1} in progress (state: {response.status.state}) - poll {poll_count + 1}/{max_polls}")
                            time.sleep(5)  # Wait 5 seconds between polls
                            
                            try:
                                response = self.client.statement_execution.get_statement(statement_id)
                                poll_count += 1
                            except Exception as poll_error:
                                logger.error(f"❌ Error polling statement status: {poll_error}")
                                break
                        
                        # Check final status for this statement
                        logger.info(f"🔍 DDL statement {i+1} final status: {response.status.state}")
                        if response.status.state != StatementState.SUCCEEDED:
                            error_details = ""
                            if hasattr(response.status, 'error') and response.status.error:
                                error_details = str(response.status.error)
                                logger.error(f"   Error details: {error_details}")
                            
                            # Check if this is a permission/access error
                            error_message = error_details.lower()
                            is_permission_error = any(keyword in error_message for keyword in [
                                'permission', 'access denied', 'forbidden', 'unauthorized', 
                                'read-only', 'readonly', 'does not exist', 'not found',
                                'insufficient privileges', 'access is denied', 'cannot access',
                                'schema', 'catalog', 'database', 'namespace'
                            ])
                            
                            if is_permission_error:
                                warning_msg = f"⚠️ DDL execution access issue for table {full_name}: {error_details}"
                                logger.warning(warning_msg)
                                print(f"⚠️ DDL Permission/Access Warning: {warning_msg}")
                                
                                # Add to warnings list
                                if not hasattr(self, '_warnings'):
                                    self._warnings = []
                                self._warnings.append(warning_msg)
                                
                                # Return graceful failure instead of hard error
                                return {
                                    'success': False,
                                    'warning': warning_msg,
                                    'error': f"DDL access denied: {error_details}",
                                    'ddl_executed': False,
                                    'skipped': True,
                                    'tags': {'tag_changes_count': 0, 'tag_success': False, 'tag_details': []}
                                }
                            else:
                                logger.error(f"❌ DDL statement {i+1} failed: {response.status.state}")
                                return False
                    
                    # All DDL statements executed successfully
                    logger.info(f"✅ All DDL statements executed successfully for table {full_name}")
                
                # Process tags regardless of whether DDL was executed or not
                logger.info(f"✅ Table {full_name} DDL processing completed")
                
                # Execute tags using EntityTagAssignments API after table creation/modification
                print(f"🏷️ APPLYING TAGS AFTER TABLE PROCESSING for {full_name}")
                
                # For newly created tables, current_table_info should be None (no existing tags)
                # For existing tables, we get current table info for comparison
                current_table_info = None
                if table_exists:  # Only get current info if table existed before
                    current_table_info = self._get_table_info(catalog_name, schema_name, data_table.name)
                
                tag_changes = self._generate_tag_changes(data_table, catalog_name, schema_name, current_table_info)
                tag_result = {
                    'tag_changes_count': len(tag_changes) if tag_changes else 0,
                    'tag_success': True,
                    'tag_details': []
                }
                
                if tag_changes:
                    print(f"🏷️ Executing {len(tag_changes)} tag changes for table {full_name}")
                    
                    # Separate table and column tags for better reporting
                    table_tags = [tc for tc in tag_changes if tc['entity_type'] == 'tables']
                    column_tags = [tc for tc in tag_changes if tc['entity_type'] == 'columns']
                    
                    tag_result['table_tags_count'] = len(table_tags)
                    tag_result['column_tags_count'] = len(column_tags)
                    
                    # Add details about tag changes
                    for change in tag_changes:
                        entity_name = change['entity_id'].split('.')[-1] if change['entity_type'] == 'columns' else 'table'
                        tag_result['tag_details'].append({
                            'entity_type': change['entity_type'],
                            'entity_name': entity_name,
                            'tag_key': change['tag_key'],
                            'action': change['action'],
                            'tag_value': change.get('tag_value', '')
                        })
                    
                    success = self._execute_tag_changes_via_sql(tag_changes, data_table.name)
                    tag_result['tag_success'] = success
                    
                    if not success:
                        logger.warning(f"⚠️ Table {full_name} processed but some tags failed to apply")
                    else:
                        logger.info(f"All tags applied successfully for table {full_name}")
                else:
                    print(f"ℹ️ No tag changes needed for table {full_name}")
                    tag_result['table_tags_count'] = 0
                    tag_result['column_tags_count'] = 0
                
                return {
                    'success': True,
                    'ddl_executed': ddl_execution_needed,
                    'tags': tag_result
                }
            except Exception as sql_error:
                logger.error(f"❌ Error executing DDL for {full_name}: {sql_error}")
                self._last_error = f"DDL execution error: {str(sql_error)}"
                return {
                    'success': False,
                    'error': f"DDL execution error: {str(sql_error)}",
                    'ddl_executed': False,
                    'tags': {'tag_changes_count': 0, 'tag_success': False, 'tag_details': []}
                }
            
        except Exception as e:
            error_message = str(e).lower()
            error_type = type(e).__name__
            
            # Categorize errors for graceful handling
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
                warning_msg = f"⚠️ Access issue with table {data_table.name}: {str(e)}"
                logger.warning(warning_msg)
                print(f"⚠️⚠️⚠️ PERMISSION/ACCESS WARNING FOR {data_table.name}: {e}")
                print(f"   📋 Error type: {error_type}")
                print(f"   📋 This table will be skipped, but processing will continue with other tables")
                
                # Add to warnings list for reporting
                if not hasattr(self, '_warnings'):
                    self._warnings = []
                self._warnings.append(warning_msg)
                
                return {
                    'success': False,
                    'warning': warning_msg,
                    'error': f"Access denied or resource not found: {str(e)}",
                    'ddl_executed': False,
                    'skipped': True,
                    'tags': {'tag_changes_count': 0, 'tag_success': False, 'tag_details': []}
                }
            else:
                # Handle other errors as before
                logger.error(f"❌ Error processing table {data_table.name}: {e}")
                print(f"❌❌❌ EXCEPTION IN CREATE_TABLE_FROM_MODEL FOR {data_table.name}: {e}")
                print(f"   📋 Exception type: {error_type}")
                print(f"   📋 Exception details: {str(e)}")
                import traceback
                print(f"   📋 Traceback: {traceback.format_exc()}")
                self._last_error = f"Table processing error: {str(e)}"
                return {
                    'success': False,
                    'error': f"Table processing error: {str(e)}",
                    'ddl_executed': False,
                    'tags': {'tag_changes_count': 0, 'tag_success': False, 'tag_details': []}
                }
    
    
    def apply_constraints_to_table(self, data_table: DataTable, catalog_name: str, 
                                 schema_name: str, relationships: List[DataModelRelationship], warehouse_id: str = None) -> bool:
        """Apply constraints (PK/FK) to an existing table using SQL execution"""
        try:
            full_name = f"{catalog_name}.{schema_name}.{data_table.name}"
            
            # Check if client is available
            if not self.client:
                logger.info(f"🔗 No Databricks client - constraints would be applied to {full_name} (demo mode)")
                # In demo mode, just log what constraints would be applied
                pk_field = data_table.get_primary_key_field()
                if pk_field:
                    logger.info(f"   📋 Would apply PK constraint: ALTER TABLE {full_name} ADD CONSTRAINT pk_{data_table.name} PRIMARY KEY ({pk_field.name})")
                
                table_relationships = [r for r in relationships if r.target_table_id == data_table.id]
                for relationship in table_relationships:
                    logger.info(f"   📋 Would apply FK constraint: {relationship.constraint_name}")
                
                return True  # Return success for demo mode
            
            logger.info(f"🔗 Applying constraints to table {full_name}")
            
            # Get warehouse ID for SQL execution
            selected_warehouse_id = self._get_warehouse_id(warehouse_id)
            if not selected_warehouse_id:
                logger.error(f"❌ No SQL warehouse available for constraint application")
                self._last_error = "No SQL warehouse available for constraint application"
                return False
            
            # Primary key constraints are already included in CREATE OR REPLACE TABLE DDL
            pk_field = data_table.get_primary_key_field()
            if pk_field:
                logger.info(f"ℹ️ PK constraint for {full_name}.{pk_field.name} was included in CREATE TABLE DDL")
            
            # Foreign key constraints are already included in CREATE OR REPLACE TABLE DDL
            table_relationships = [r for r in relationships if r.target_table_id == data_table.id]
            if table_relationships:
                logger.info(f"ℹ️ FK constraints for {full_name} were included in CREATE TABLE DDL ({len(table_relationships)} relationships)")
            else:
                logger.info(f"ℹ️ No FK constraints to apply for {full_name}")
            
            logger.info(f"✅ Constraints processing completed for {full_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error applying constraints to {data_table.name}: {e}")
            self._last_error = f"Constraint application error: {str(e)}"
            return False
    
    def generate_ddl_for_table(self, data_table: DataTable, catalog_name: str, schema_name: str, source_catalog: str = None, all_tables: List[DataTable] = None) -> str:
        """Generate DDL for a DataTable - CREATE or ALTER based on existence and catalog
        
        This method generates DDL text for download/preview and INCLUDES tags as separate statements.
        Tags are included here for complete DDL but executed separately during 'Apply Changes'.
        """
        try:
            full_name = f"{catalog_name}.{schema_name}.{data_table.name}"
            print(f"🚀🚀🚀 NEW VERSION - DDL Generation for table: {full_name} 🚀🚀🚀")
            logger.info(f"🚀🚀🚀 NEW VERSION - DDL Generation for table: {full_name} 🚀🚀🚀")
            logger.info(f"🔍 Source catalog: {source_catalog}, Target catalog: {catalog_name}")

            # If importing from a different catalog, always CREATE (don't check existence)
            if source_catalog and source_catalog != catalog_name:
                logger.info(f"Importing from different catalog {source_catalog} to {catalog_name}, will CREATE table")
                return self._generate_create_table_ddl(data_table, catalog_name, schema_name, all_tables, include_tags=True)

            # Check if table exists in the target catalog
            logger.info(f"🔍 Checking table existence for: {full_name}")
            
            # First, let's list tables in the schema to see what exists
            try:
                if self.client:
                    tables_api = self.client.tables
                    tables_list = tables_api.list(catalog_name=catalog_name, schema_name=schema_name)
                    existing_table_names = [table.name for table in tables_list]
                    logger.info(f"🔍 Existing tables in {catalog_name}.{schema_name}: {existing_table_names}")
                else:
                    print("⚠️ NO DATABRICKS CLIENT AVAILABLE")
                    logger.warning("⚠️ No Databricks client available for listing tables")
            except Exception as e:
                logger.warning(f"⚠️ Could not list tables in {catalog_name}.{schema_name}: {e}")
            
            table_exists = self._table_exists(catalog_name, schema_name, data_table.name)
            logger.info(f"🔍 Table existence result: {table_exists}")

            if table_exists:
                print(f"🔄 TABLE EXISTS - GENERATING ALTER DDL (warnings will be checked)")
                logger.info(f"🔄 Table {full_name} exists, generating ALTER statements with commented CREATE")
                
                # For existing tables, include both commented CREATE and ALTER statements
                create_ddl = self._generate_create_table_ddl(data_table, catalog_name, schema_name, all_tables, include_tags=True)
                alter_ddl = self._generate_alter_table_ddl(data_table, catalog_name, schema_name, all_tables, include_tags=True)
                
                # Comment out the CREATE TABLE statement
                commented_create = "\n".join([f"-- {line}" if line.strip() else "--" for line in create_ddl.split("\n")])
                
                # Combine with header
                combined_ddl = []
                combined_ddl.append(f"-- DDL for {data_table.name}")
                combined_ddl.append(f"-- Table exists, showing current CREATE statement (commented) and ALTER statements")
                combined_ddl.append("")
                combined_ddl.append("-- Current CREATE TABLE statement (for reference):")
                combined_ddl.append(commented_create)
                
                if alter_ddl.strip():
                    combined_ddl.append("")
                    combined_ddl.append("-- ALTER statements to apply changes:")
                    combined_ddl.append(alter_ddl)
                else:
                    combined_ddl.append("")
                    combined_ddl.append("-- No changes needed - table structure matches the model")
                
                return "\n".join(combined_ddl)
            else:
                print(f"🆕 TABLE DOES NOT EXIST - GENERATING CREATE DDL (no warnings for new tables)")
                logger.info(f"🆕 Table {full_name} does not exist, generating CREATE statement")
                create_ddl = self._generate_create_table_ddl(data_table, catalog_name, schema_name, all_tables, include_tags=True)
                
                # For new tables, add header
                ddl_with_header = []
                ddl_with_header.append(f"-- DDL for {data_table.name}")
                ddl_with_header.append(f"-- New table - CREATE statement")
                ddl_with_header.append("")
                ddl_with_header.append(create_ddl)
                
                return "\n".join(ddl_with_header)

        except Exception as e:
            logger.error(f"Error generating DDL for {data_table.name}: {e}")
            return ""

    def generate_ddl_for_traditional_view(self, view: TraditionalView, catalog_name: str, schema_name: str) -> str:
        """Generate DDL for a traditional view"""
        full_name = f"{catalog_name}.{schema_name}.{view.name}"
        
        ddl_statements = []
        
        # Add comment about the view
        if view.description:
            ddl_statements.append(f"-- {view.description}")
        
        # Generate CREATE VIEW statement
        create_view_sql = f"""CREATE VIEW {full_name} AS
{view.sql_query}"""
        
        ddl_statements.append(create_view_sql)
        
        return "\n\n".join(ddl_statements)

    def _generate_create_table_ddl(self, data_table: DataTable, catalog_name: str, schema_name: str, all_tables: List[DataTable] = None, include_tags: bool = False) -> str:
        """Generate CREATE TABLE DDL statement"""
        full_name = f"{catalog_name}.{schema_name}.{data_table.name}"

        # Skip empty tables (tables with no fields)
        if not data_table.fields or len(data_table.fields) == 0:
            logger.info(f"Skipping DDL generation for empty table: {data_table.name}")
            return f"-- Skipping empty table: {full_name} (no fields defined)"

        column_defs = []
        for field in data_table.fields:
            column_def = f"  {field.name} {self._get_column_type_text(field)}"

            if not field.nullable:
                column_def += " NOT NULL"

            if field.default_value:
                column_def += f" DEFAULT {field.default_value}"

            if field.comment:
                column_def += f" COMMENT '{field.comment}'"

            column_defs.append(column_def)

        # Build DDL
        ddl = f"CREATE OR REPLACE TABLE {full_name} (\n"
        ddl += ",\n".join(column_defs)

        # Add primary key constraint
        pk_field = data_table.get_primary_key_field()
        if pk_field:
            ddl += f",\n  CONSTRAINT pk_{data_table.name} PRIMARY KEY ({pk_field.name})"

        # Add foreign key constraints
        if all_tables:
            # Debug: Log all fields and their FK status
            print(f"🔍 DEBUG: Checking fields for table {data_table.name}:")
            logger.info(f"🔍 Checking fields for table {data_table.name}:")
            for field in data_table.fields:
                print(f"   Field: {field.name}, is_foreign_key: {field.is_foreign_key}, foreign_key_reference: {field.foreign_key_reference}")
                logger.info(f"   Field: {field.name}, is_foreign_key: {field.is_foreign_key}, foreign_key_reference: {field.foreign_key_reference}")
            
            fk_fields = [f for f in data_table.fields if f.is_foreign_key and f.foreign_key_reference]
            print(f"🔍 DEBUG: Found {len(fk_fields)} FK fields for table {data_table.name}")
            logger.info(f"🔍 Found {len(fk_fields)} FK fields for table {data_table.name}")
            for fk_field in fk_fields:
                ref_table_name = None
                ref_field_name = None
                constraint_name = f"fk_{data_table.name}_{fk_field.name}"
                
                # Handle object format (ForeignKeyReference)
                if hasattr(fk_field.foreign_key_reference, 'referenced_table_id'):
                    # Find referenced table and field by ID
                    ref_table = next((t for t in all_tables if t.id == fk_field.foreign_key_reference.referenced_table_id), None)
                    if ref_table:
                        ref_field = next((f for f in ref_table.fields if f.id == fk_field.foreign_key_reference.referenced_field_id), None)
                        if ref_field:
                            ref_table_name = ref_table.name
                            ref_field_name = ref_field.name
                            # Use constraint name from FK reference if available
                            if fk_field.foreign_key_reference.constraint_name:
                                constraint_name = fk_field.foreign_key_reference.constraint_name
                
                # Handle legacy string format (format: "table_name.field_name")
                elif isinstance(fk_field.foreign_key_reference, str) and '.' in fk_field.foreign_key_reference:
                    ref_table_name, ref_field_name = fk_field.foreign_key_reference.split('.', 1)
                
                # Generate DDL if we have valid reference
                if ref_table_name and ref_field_name:
                    # Check if this is a self-referencing constraint
                    is_self_reference = (ref_table_name == data_table.name)
                    
                    if is_self_reference:
                        # Skip self-referencing constraints during CREATE TABLE
                        # They will be added later with ALTER TABLE to avoid circular dependency
                        logger.info(f"🔄 Skipping self-referencing FK constraint {constraint_name} in CREATE TABLE (will be added with ALTER TABLE)")
                    else:
                        # Find the referenced table to get its effective catalog/schema
                        ref_table = next((t for t in all_tables if t.name == ref_table_name), None)
                        if ref_table:
                            # Use a simple approach to get project context for effective catalog/schema resolution
                            # Since we don't have direct access to the DataModelProject here, we'll use the table's own catalog/schema
                            # or fall back to the provided catalog/schema if the table doesn't have its own
                            ref_catalog = ref_table.catalog_name if ref_table.catalog_name else catalog_name
                            ref_schema = ref_table.schema_name if ref_table.schema_name else schema_name
                            ref_table_full_name = f"{ref_catalog}.{ref_schema}.{ref_table_name}"
                        else:
                            # Fallback to current catalog/schema if referenced table not found in all_tables
                            ref_table_full_name = f"{catalog_name}.{schema_name}.{ref_table_name}"
                        
                        fk_constraint = f",\n  CONSTRAINT {constraint_name} FOREIGN KEY ({fk_field.name}) REFERENCES {ref_table_full_name}({ref_field_name})"
                        logger.info(f"🔗 Adding FK constraint to DDL: {constraint_name}")
                        ddl += fk_constraint
                else:
                    logger.warning(f"⚠️ Could not resolve FK reference for {fk_field.name}: ref_table={ref_table_name}, ref_field={ref_field_name}")

        ddl += "\n)"

        # Add liquid clustering if enabled
        if hasattr(data_table, 'cluster_by_auto') and data_table.cluster_by_auto:
            ddl += "\nCLUSTER BY AUTO"
            logger.info(f"🔗 Adding CLUSTER BY AUTO to {data_table.name}")

        # Add table properties - REMOVE location for managed tables
        if data_table.file_format and data_table.file_format != "DELTA":
            ddl += f"\nUSING {data_table.file_format}"

        # Remove location for managed tables - not needed for managed
        # if data_table.storage_location:
        #     ddl += f"\nLOCATION '{data_table.storage_location}'"

        if data_table.comment:
            ddl += f"\nCOMMENT '{data_table.comment}'"

        # Conditionally add tag statements for DDL generation (download/preview)
        if include_tags:
            tag_statements = self._generate_tag_statements(data_table, catalog_name, schema_name)
            if tag_statements:
                ddl += "\n\n-- Tag statements (execute after table creation)\n" + tag_statements
        
        # NOTE: Tag statements are handled separately via _execute_tag_changes_via_sql during apply
        # to avoid SQL syntax errors when mixing CREATE TABLE with SET TAG statements
        # NOTE: Self-referencing constraints are handled separately via _apply_self_referencing_constraints
        # to avoid circular dependency issues during table creation
        
        return ddl

    def _generate_alter_table_ddl(self, data_table: DataTable, catalog_name: str, schema_name: str, all_tables: List[DataTable] = None, include_tags: bool = False) -> str:
        """Generate ALTER TABLE DDL statements for existing table"""
        try:
            full_name = f"{catalog_name}.{schema_name}.{data_table.name}"
            print(f"🔄 GENERATING ALTER DDL FOR: {full_name}")
            
            # Get current table structure from Databricks
            current_table_info = self._get_table_info(catalog_name, schema_name, data_table.name)
            if not current_table_info:
                print(f"❌ COULD NOT GET TABLE INFO FOR: {full_name}")
                logger.error(f"❌ Could not retrieve table info for {full_name}")
                return f"-- Error: Could not retrieve table info for {full_name}"
            
            print(f"✅ GOT TABLE INFO - Columns: {len(current_table_info.columns or [])}, Constraints: {len(getattr(current_table_info, 'table_constraints', []) or [])}")
            print(f"🔍 TABLE INFO DEBUG:")
            print(f"   📋 Table info type: {type(current_table_info)}")
            print(f"   📋 Table info attributes: {dir(current_table_info)}")
            if hasattr(current_table_info, 'columns') and current_table_info.columns:
                print(f"   📋 First column type: {type(current_table_info.columns[0])}")
                print(f"   📋 First column attributes: {dir(current_table_info.columns[0])}")
            logger.info(f"🔍 Retrieved table info for {full_name}:")
            logger.info(f"   Columns: {len(current_table_info.columns or [])}")
            logger.info(f"   Constraints: {len(getattr(current_table_info, 'table_constraints', []) or [])}")
            
            alter_statements = []
            
            # Compare columns and generate ALTER statements
            current_columns = {col.name: col for col in current_table_info.columns or []}
            desired_columns = {field.name: field for field in data_table.fields}
            
            # Detect column renames by matching field IDs
            column_renames = self._detect_column_renames(current_table_info, data_table)
            
            # 1. RENAME columns first (before other operations)
            if column_renames:
                # Enable Column Mapping if we need to rename columns
                print(f"🔧 COLUMN RENAMES DETECTED FOR {full_name}:")
                for old_name, new_name in column_renames.items():
                    print(f"   📝 {old_name} → {new_name}")
                print(f"🔧 ENABLING COLUMN MAPPING (delta.columnMapping.mode = 'name')")
                logger.info(f"🔧 Column renames detected - enabling Column Mapping for {full_name}")
                alter_statements.append(f"ALTER TABLE {full_name} SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');")
                
                for old_name, new_name in column_renames.items():
                    logger.info(f"🔄 Renaming column: {old_name} → {new_name}")
                    alter_statements.append(f"ALTER TABLE {full_name} RENAME COLUMN {old_name} TO {new_name};")
                    # Update current_columns mapping for subsequent operations
                    if old_name in current_columns:
                        current_columns[new_name] = current_columns.pop(old_name)
            
            # 2. ADD new columns
            for field_name, field in desired_columns.items():
                if field_name not in current_columns:
                    logger.info(f"➕ Adding new column: {field_name}")
                    column_def = f"{field.name} {self._get_column_type_text(field)}"
                    
                    # Add comment if present
                    if field.comment:
                        column_def += f" COMMENT '{field.comment}'"
                    
                    # Handle Delta table NOT NULL limitation
                    if not field.nullable:
                        # Delta tables don't support adding NOT NULL columns directly
                        warning_text = f"⚠️ DELTA LIMITATION: Cannot add NOT NULL column '{field.name}' directly to existing Delta table. Column will be added as nullable. Manual steps required to make it NOT NULL."
                        logger.warning(warning_text)
                        self.add_warning(warning_text)
                        print(f"🚨 {warning_text}")
                        
                        # Add as nullable for now
                        if include_tags:
                            # For DDL generation, include commented manual approach
                            alter_statements.append(f"ALTER TABLE {full_name} ADD COLUMN {column_def}; -- Added as nullable due to Delta limitation")
                            alter_statements.append(f"-- Manual steps required for NOT NULL:")
                            alter_statements.append(f"-- 1. UPDATE {full_name} SET {field.name} = <default_value> WHERE {field.name} IS NULL;")
                            alter_statements.append(f"-- 2. ALTER TABLE {full_name} ALTER COLUMN {field.name} SET NOT NULL;")
                        else:
                            # For apply process, just add as nullable
                            alter_statements.append(f"ALTER TABLE {full_name} ADD COLUMN {column_def};")
                    else:
                        # Nullable columns are fine
                        alter_statements.append(f"ALTER TABLE {full_name} ADD COLUMN {column_def};")
            
            # 3. ALTER existing columns (type changes, comments, nullability)
            for field_name, field in desired_columns.items():
                if field_name in current_columns:
                    current_col = current_columns[field_name]
                    changes = []
                    
                    # Check if data type can be changed
                    current_type = current_col.type_text.upper()
                    desired_type = self._get_column_type_text(field).upper()
                    
                    print(f"🔍 TYPE COMPARISON for {field_name}:")
                    print(f"   Current (Databricks): '{current_type}'")
                    print(f"   Desired (System): '{desired_type}'")
                    print(f"   Are they different? {current_type != desired_type}")
                    
                    if current_type != desired_type:
                        if self._can_alter_column_type(current_col, field):
                            new_type = self._get_column_type_text(field)
                            logger.info(f"🔄 Changing column type: {field_name} from {current_col.type_text} to {new_type}")
                            
                            # Check if this requires Type Widening (for any supported type changes)
                            if self._requires_type_widening(current_type, desired_type):
                                # Enable Type Widening if needed for supported type changes
                                if not hasattr(self, '_type_widening_enabled'):
                                    self._type_widening_enabled = set()
                                if full_name not in self._type_widening_enabled:
                                    print(f"🔧 TYPE WIDENING DETECTED ({current_type} → {desired_type}) - enabling Type Widening for {full_name}")
                                    logger.info(f"🔧 Type widening change detected ({current_type} → {desired_type}) - enabling Type Widening for {full_name}")
                                    alter_statements.append(f"ALTER TABLE {full_name} SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true');")
                                    self._type_widening_enabled.add(full_name)
                            
                            changes.append(f"TYPE {new_type}")
                        else:
                            # Invalid type change - log warning with specific message
                            base_current = current_type.split('(')[0]
                            base_desired = desired_type.split('(')[0]
                            
                            if current_type.startswith('DECIMAL') and desired_type.startswith('DECIMAL'):
                                warning_msg = f"⚠️ INVALID DECIMAL TYPE WIDENING for column {field_name}: {current_type} → {desired_type}"
                                warning_text = f"Column {field_name}: Cannot change DECIMAL from {current_type} to {desired_type} - violates Type Widening rules (precision must increase proportionally with scale)"
                            elif base_desired == 'DECIMAL':
                                # Specific message for numeric to DECIMAL conversions
                                min_precision = 10 if base_current in ['TINYINT', 'BYTE', 'SMALLINT', 'SHORT', 'INT', 'INTEGER'] else 20 if base_current in ['BIGINT', 'LONG'] else 7
                                warning_msg = f"⚠️ INVALID NUMERIC TO DECIMAL CONVERSION for column {field_name}: {current_type} → {desired_type}"
                                warning_text = f"Column {field_name}: Cannot convert {base_current} to {desired_type} - minimum precision for {base_current} → DECIMAL is {min_precision}"
                            else:
                                warning_msg = f"⚠️ INVALID TYPE CHANGE for column {field_name}: {current_type} → {desired_type} (not supported by Databricks type widening rules)"
                                warning_text = f"Column {field_name}: Cannot change type from {current_type} to {desired_type} - not supported by Databricks type widening rules"
                            
                            print(warning_msg)
                            logger.warning(warning_msg)
                            # Store warning for later retrieval
                            self.add_warning(warning_text)
                            print(f"🚨 WARNING ADDED: {warning_text}")
                            print(f"🚨 Total warnings now: {len(self._warnings)}")
                    
                    # Check comment changes
                    current_comment = current_col.comment or ""
                    desired_comment = field.comment or ""
                    if current_comment != desired_comment:
                        logger.info(f"💬 Updating comment for column: {field_name}")
                        changes.append(f"COMMENT '{desired_comment}'")
                    
                    # Check nullability changes
                    current_nullable = current_col.nullable if hasattr(current_col, 'nullable') else True
                    
                    # IMPORTANT: Primary Key fields should ALWAYS be NOT NULL
                    desired_nullable = field.nullable
                    if field.is_primary_key:
                        desired_nullable = False  # PK must be NOT NULL
                        print(f"🔑 PK FIELD {field_name}: Forcing nullable=False (PK must be NOT NULL)")
                    
                    print(f"🔍 NULLABILITY CHECK for {field_name}:")
                    print(f"   Current (Databricks): {current_nullable} (type: {type(current_nullable)})")
                    print(f"   Desired (System): {desired_nullable} (type: {type(desired_nullable)}) {'[PK CORRECTED]' if field.is_primary_key else ''}")
                    print(f"   Are they equal? {current_nullable == desired_nullable}")
                    logger.info(f"🔍 Nullability comparison for {field_name}: current={current_nullable}, desired={desired_nullable}")
                    
                    if current_nullable != desired_nullable:
                        if desired_nullable:
                            logger.info(f"🔓 Making column nullable: {field_name}")
                            changes.append("DROP NOT NULL")
                        else:
                            logger.info(f"🔒 Making column NOT NULL: {field_name}")
                            changes.append("SET NOT NULL")
                    else:
                        logger.info(f"✅ Nullability unchanged for {field_name}: {current_nullable}")
                    
                    # Apply changes if any - each change needs its own ALTER statement
                    if changes:
                        for change in changes:
                            alter_statements.append(f"ALTER TABLE {full_name} ALTER COLUMN {field_name} {change};")
            
            # 4. DROP columns that no longer exist
            columns_to_drop = []
            for col_name in current_columns:
                if col_name not in desired_columns:
                    current_col = current_columns[col_name]
                    
                    # Safety checks before dropping columns
                    safety_warnings = []
                    
                    # Check if column is part of primary key
                    current_pk_cols = set()
                    if current_table_info.table_constraints:
                        for constraint in current_table_info.table_constraints:
                            if constraint.primary_key_constraint:
                                current_pk_cols.update(constraint.primary_key_constraint.child_columns or [])
                    
                    if col_name in current_pk_cols:
                        safety_warnings.append(f"Column '{col_name}' is part of the primary key")
                    
                    # Check if column might be referenced by foreign keys (we can't easily detect this)
                    # This would require checking all other tables, so we'll add a warning
                    safety_warnings.append(f"Column '{col_name}' may be referenced by foreign keys in other tables")
                    
                    if safety_warnings:
                        warning_text = f"⚠️ DROPPING COLUMN '{col_name}' - SAFETY WARNINGS: {'; '.join(safety_warnings)}"
                        logger.warning(warning_text)
                        self.add_warning(warning_text)
                        print(f"🚨 {warning_text}")
                    
                    logger.info(f"➖ Dropping column: {col_name}")
                    print(f"➖ COLUMN TO DROP: {col_name} ({current_col.type_name})")
                    columns_to_drop.append(col_name)
            
            # Generate DROP COLUMN statements
            for col_name in columns_to_drop:
                alter_statements.append(f"ALTER TABLE {full_name} DROP COLUMN {col_name};")
            
            # 4.5. Check table comment changes
            current_table_comment = current_table_info.comment or ""
            desired_table_comment = data_table.comment or ""
            if current_table_comment != desired_table_comment:
                logger.info(f"💬 Updating table comment for: {full_name}")
                print(f"💬 TABLE COMMENT CHANGE DETECTED:")
                print(f"   Current: '{current_table_comment}'")
                print(f"   Desired: '{desired_table_comment}'")
                alter_statements.append(f"ALTER TABLE {full_name} SET TBLPROPERTIES ('comment' = '{desired_table_comment}');")
            
            # 5. Handle liquid clustering changes
            if hasattr(data_table, 'cluster_by_auto'):
                current_cluster_enabled = self.check_liquid_clustering_enabled(catalog_name, schema_name, data_table.name)
                desired_cluster_enabled = data_table.cluster_by_auto
                
                if current_cluster_enabled != desired_cluster_enabled:
                    if desired_cluster_enabled:
                        logger.info(f"🔗 Enabling CLUSTER BY AUTO for: {full_name}")
                        alter_statements.append(f"ALTER TABLE {full_name} CLUSTER BY AUTO;")
                    else:
                        logger.info(f"🔗 Disabling CLUSTER BY AUTO for: {full_name}")
                        alter_statements.append(f"ALTER TABLE {full_name} CLUSTER BY NONE;")
            
            # 6. Handle constraints (PK, FK)
            constraint_statements = self._generate_constraint_alter_statements(data_table, catalog_name, schema_name, current_table_info, all_tables)
            alter_statements.extend(constraint_statements)
            
            # 6. Handle Primary Key propagation to Foreign Keys
            self._propagate_pk_changes_to_fks(data_table, all_tables)
            
            # Tag statements are now included in DDL generation as separate statements
            
            # Log summary of changes
            if alter_statements:
                print(f"📋 GENERATED {len(alter_statements)} ALTER STATEMENTS:")
                for i, stmt in enumerate(alter_statements, 1):
                    print(f"   {i}. {stmt}")
                logger.info(f"📋 Generated {len(alter_statements)} ALTER statements for {full_name}")
                for i, stmt in enumerate(alter_statements, 1):
                    logger.info(f"   {i}. {stmt}")
                print(f"⚠️ WARNINGS GENERATED: {len(self._warnings)}")
                if self._warnings:
                    for i, warning in enumerate(self._warnings, 1):
                        print(f"   {i}. {warning}")
                
                # Combine ALTER statements
                ddl_result = "\n".join(alter_statements)
                
                # Conditionally add tag statements for DDL generation (download/preview)
                if include_tags:
                    tag_statements = self._generate_tag_statements(data_table, catalog_name, schema_name, current_table_info)
                    if tag_statements:
                        ddl_result += "\n\n-- Tag statements\n" + tag_statements
                
                return ddl_result
            else:
                print(f"✅ TABLE {full_name} IS UP TO DATE - NO CHANGES NEEDED")
                logger.info(f"✅ Table {full_name} is already up to date - no changes needed")
                print(f"⚠️ WARNINGS GENERATED: {len(self._warnings)}")
                if self._warnings:
                    for i, warning in enumerate(self._warnings, 1):
                        print(f"   {i}. {warning}")
                
                # Even if table structure is up to date, check for tag changes
                ddl_result = f"-- Table {full_name} is already up to date"
                
                # Conditionally add tag statements for DDL generation (download/preview)
                if include_tags:
                    tag_statements = self._generate_tag_statements(data_table, catalog_name, schema_name, current_table_info)
                    if tag_statements:
                        ddl_result += "\n\n-- Tag statements\n" + tag_statements
                
                return ddl_result
                
        except Exception as e:
            logger.error(f"❌ Error generating ALTER DDL for {data_table.name}: {e}")
            return f"-- Error generating ALTER DDL for {data_table.name}: {str(e)}"

    def _generate_tag_statements(self, data_table: DataTable, catalog_name: str, schema_name: str, current_table_info=None) -> str:
        """Generate SET TAG and UNSET TAG statements for fields with tags"""
        tag_statements = []
        full_table_name = f"{catalog_name}.{schema_name}.{data_table.name}"
        
        print(f"🏷️ GENERATING TAG STATEMENTS for {full_table_name}")
        
        # Get current tags using EntityTagAssignments API
        current_tags = self._get_column_tags_from_information_schema(catalog_name, schema_name, data_table.name)
        
        for field in data_table.fields:
            column_name = f"{full_table_name}.{field.name}"
            desired_tags = getattr(field, 'tags', {}) or {}
            
            
            # Automatically include logical_name as a tag if it exists
            if hasattr(field, 'logical_name') and field.logical_name and field.logical_name.strip():
                desired_tags['logical_name'] = field.logical_name.strip()
                print(f"   📋 Auto-added logical_name tag: {field.logical_name.strip()}")
            else:
                print(f"   📋 No logical_name to add (hasattr: {hasattr(field, 'logical_name')}, value: {getattr(field, 'logical_name', 'N/A')})")
            
            current_field_tags = current_tags.get(field.name, {})
            
            print(f"🔍 TAG COMPARISON for field {field.name}:")
            print(f"   Current tags (Databricks): {current_field_tags}")
            print(f"   Desired tags (System): {desired_tags}")
            
            # SET new/updated tags
            for tag_key, tag_value in desired_tags.items():
                if tag_key and tag_key.strip():
                    current_value = current_field_tags.get(tag_key)
                    if tag_value != current_value:  # Only set if different
                        print(f"   ➕ SET TAG: {tag_key} = '{tag_value}' (was: '{current_value}')")
                        if tag_value and tag_value.strip():
                            tag_statements.append(f"SET TAG ON COLUMN {column_name} `{tag_key}` = `{tag_value}`;")
                        else:
                            tag_statements.append(f"SET TAG ON COLUMN {column_name} `{tag_key}`;")
                    else:
                        print(f"   ✅ TAG UNCHANGED: {tag_key} = '{tag_value}'")
            
            # UNSET tags that are no longer desired
            for tag_key in current_field_tags:
                if tag_key not in desired_tags:
                    print(f"   ➖ UNSET TAG: {tag_key} (was: '{current_field_tags[tag_key]}')")
                    tag_statements.append(f"UNSET TAG ON COLUMN {column_name} `{tag_key}`;")
                else:
                    print(f"   ✅ TAG STILL DESIRED: {tag_key}")
            
            if not current_field_tags and not desired_tags:
                print(f"   ℹ️ No tags for field {field.name}")
        
        # Handle table-level tags
        print(f"🏷️ CHECKING TABLE-LEVEL TAGS for {full_table_name}")
        
        # Get current table tags using Information Schema
        current_table_tags = self._get_table_tags_from_information_schema(catalog_name, schema_name, data_table.name)
        
        # Get desired table tags
        desired_table_tags = getattr(data_table, 'tags', {}) or {}
        
        # Automatically include table logical_name as a tag if it exists
        if hasattr(data_table, 'logical_name') and data_table.logical_name and data_table.logical_name.strip():
            desired_table_tags['logical_name'] = data_table.logical_name.strip()
            print(f"   📋 Auto-added table logical_name tag: {data_table.logical_name.strip()}")
        
        print(f"🔍 TABLE TAG COMPARISON:")
        print(f"   Current table tags (Databricks): {current_table_tags}")
        print(f"   Desired table tags (System): {desired_table_tags}")
        
        # SET new/updated table tags
        for tag_key, tag_value in desired_table_tags.items():
            if tag_key and tag_key.strip():
                current_value = current_table_tags.get(tag_key)
                if tag_value != current_value:  # Only set if different
                    print(f"   ➕ SET TABLE TAG: {tag_key} = '{tag_value}' (was: '{current_value}')")
                    if tag_value and tag_value.strip():
                        tag_statements.append(f"SET TAG ON TABLE {full_table_name} `{tag_key}` = `{tag_value}`;")
                    else:
                        tag_statements.append(f"SET TAG ON TABLE {full_table_name} `{tag_key}`;")
                else:
                    print(f"   ✅ TABLE TAG UNCHANGED: {tag_key} = '{tag_value}'")
        
        # UNSET table tags that are no longer desired
        for tag_key in current_table_tags:
            if tag_key not in desired_table_tags:
                print(f"   ➖ UNSET TABLE TAG: {tag_key} (was: '{current_table_tags[tag_key]}')")
                tag_statements.append(f"UNSET TAG ON TABLE {full_table_name} `{tag_key}`;")
            else:
                print(f"   ✅ TABLE TAG STILL DESIRED: {tag_key}")
        
        if tag_statements:
            print(f"🏷️ GENERATED {len(tag_statements)} TAG STATEMENTS:")
            for i, stmt in enumerate(tag_statements, 1):
                print(f"   {i}. {stmt}")
        else:
            print(f"🏷️ NO TAG CHANGES DETECTED")
        
        return "\n".join(tag_statements) if tag_statements else ""

    def _generate_tag_changes(self, data_table: DataTable, catalog_name: str, schema_name: str, current_table_info=None) -> list:
        """Generate tag changes as data structures for EntityTagAssignments API"""
        tag_changes = []
        full_table_name = f"{catalog_name}.{schema_name}.{data_table.name}"
        
        print(f"🏷️ GENERATING TAG CHANGES for {full_table_name}")
        
        # Get current tags using EntityTagAssignments API (only if table exists)
        current_tags = {}
        current_table_tags = {}
        if current_table_info is not None:
            # Table exists, get current tags
            current_tags = self._get_column_tags_from_information_schema(catalog_name, schema_name, data_table.name)
            current_table_tags = self._get_table_tags_from_information_schema(catalog_name, schema_name, data_table.name)
        else:
            # New table, no existing tags
            print(f"🆕 New table - no existing tags to compare against")
        
        # Handle table-level tags first
        print(f"🏷️ PROCESSING TABLE-LEVEL TAGS for {full_table_name}")
        
        # Get desired table-level tags
        desired_table_tags = getattr(data_table, 'tags', {}) or {}
        
        # Automatically include logical_name as a table tag if it exists
        if hasattr(data_table, 'logical_name') and data_table.logical_name and data_table.logical_name.strip():
            desired_table_tags['logical_name'] = data_table.logical_name.strip()
            print(f"   📋 Auto-added table logical_name tag: {data_table.logical_name.strip()}")
        
        print(f"🔍 TABLE TAG COMPARISON:")
        print(f"   Current table tags (Databricks): {current_table_tags}")
        print(f"   Desired table tags (System): {desired_table_tags}")
        
        # SET new/updated table tags
        for tag_key, tag_value in desired_table_tags.items():
            if tag_key and tag_key.strip():
                current_value = current_table_tags.get(tag_key)
                if tag_value != current_value:  # Only set if different
                    if current_value is None:
                        # Tag doesn't exist, CREATE it
                        print(f"   ➕ CREATE TABLE TAG: {tag_key} = '{tag_value}'")
                        tag_changes.append({
                            'action': 'CREATE',
                            'entity_type': 'tables',
                            'entity_id': full_table_name,
                            'tag_key': tag_key,
                            'tag_value': tag_value or ''
                        })
                    else:
                        # Tag exists with different value, UPDATE it
                        print(f"   🔄 UPDATE TABLE TAG: {tag_key} = '{tag_value}' (was: '{current_value}')")
                        tag_changes.append({
                            'action': 'UPDATE',
                            'entity_type': 'tables',
                            'entity_id': full_table_name,
                            'tag_key': tag_key,
                            'tag_value': tag_value or ''
                        })
                else:
                    print(f"   ✅ TABLE TAG UNCHANGED: {tag_key} = '{tag_value}'")
        
        # UNSET table tags that are no longer desired
        for tag_key in current_table_tags:
            if tag_key not in desired_table_tags:
                print(f"   ➖ UNSET TABLE TAG: {tag_key} (was: '{current_table_tags[tag_key]}') - tag removed from table")
                tag_changes.append({
                    'action': 'UNSET',
                    'entity_type': 'tables',
                    'entity_id': full_table_name,
                    'tag_key': tag_key
                })
            else:
                print(f"   ✅ TABLE TAG STILL DESIRED: {tag_key}")
        
        if not current_table_tags and not desired_table_tags:
            print(f"   ℹ️ No table-level tags")
        
        # Handle column-level tags
        print(f"🏷️ PROCESSING COLUMN-LEVEL TAGS for {full_table_name}")
        
        for field in data_table.fields:
            column_full_name = f"{full_table_name}.{field.name}"
            desired_tags = getattr(field, 'tags', {}) or {}
            
            # Automatically include logical_name as a tag if it exists
            if hasattr(field, 'logical_name') and field.logical_name and field.logical_name.strip():
                desired_tags['logical_name'] = field.logical_name.strip()
                print(f"   📋 Auto-added logical_name tag: {field.logical_name.strip()}")
            
            current_field_tags = current_tags.get(field.name, {})
            
            print(f"🔍 TAG COMPARISON for field {field.name}:")
            print(f"   Current tags (Databricks): {current_field_tags}")
            print(f"   Desired tags (System): {desired_tags}")
            
            # SET new/updated tags
            for tag_key, tag_value in desired_tags.items():
                if tag_key and tag_key.strip():
                    current_value = current_field_tags.get(tag_key)
                    if tag_value != current_value:  # Only set if different
                        if current_value is None:
                            # Tag doesn't exist, CREATE it
                            print(f"   ➕ CREATE TAG: {tag_key} = '{tag_value}'")
                            tag_changes.append({
                                'action': 'CREATE',
                                'entity_type': 'columns',
                                'entity_id': column_full_name,
                                'tag_key': tag_key,
                                'tag_value': tag_value or ''
                            })
                        else:
                            # Tag exists with different value, UPDATE it using PATCH
                            print(f"   🔄 UPDATE TAG: {tag_key} = '{tag_value}' (was: '{current_value}')")
                            tag_changes.append({
                                'action': 'UPDATE',
                                'entity_type': 'columns',
                                'entity_id': column_full_name,
                                'tag_key': tag_key,
                                'tag_value': tag_value or ''
                            })
                    else:
                        print(f"   ✅ TAG UNCHANGED: {tag_key} = '{tag_value}'")
            
            # UNSET tags that are no longer desired (removed from frontend)
            for tag_key in current_field_tags:
                if tag_key not in desired_tags:
                    print(f"   ➖ UNSET TAG: {tag_key} (was: '{current_field_tags[tag_key]}') - tag removed from table")
                    tag_changes.append({
                        'action': 'UNSET',
                        'entity_type': 'columns',
                        'entity_id': column_full_name,
                        'tag_key': tag_key
                    })
                else:
                    print(f"   ✅ TAG STILL DESIRED: {tag_key}")
            
            if not current_field_tags and not desired_tags:
                print(f"   ℹ️ No tags for field {field.name}")
        
        if tag_changes:
            print(f"🏷️ GENERATED {len(tag_changes)} TAG CHANGES:")
            for i, change in enumerate(tag_changes, 1):
                action = change['action']
                entity_id = change['entity_id']
                tag_key = change['tag_key']
                if action == 'SET':
                    tag_value = change['tag_value']
                    print(f"   {i}. {action} TAG: {entity_id}.{tag_key} = '{tag_value}'")
                else:
                    print(f"   {i}. {action} TAG: {entity_id}.{tag_key}")
        else:
            print(f"🏷️ NO TAG CHANGES DETECTED")
        
        return tag_changes

    def _execute_tag_changes_via_api(self, tag_changes: list, table_name: str) -> bool:
        """Execute tag changes using EntityTagAssignments API"""
        if not tag_changes:
            return True
        
        print(f"🏷️ EXECUTING {len(tag_changes)} TAG CHANGES via EntityTagAssignments API for {table_name}")
        
        import requests
        
        # Get Databricks host and token from client config
        host = self.client.config.host
        token = self.client.config.token
        
        success_count = 0
        total_count = len(tag_changes)
        
        for i, change in enumerate(tag_changes):
            try:
                action = change['action']
                entity_type = change['entity_type']
                entity_id = change['entity_id']
                tag_key = change['tag_key']
                
                print(f"🏷️ Executing tag change {i+1}/{total_count}: {action} {entity_id}.{tag_key}")
                
                if action == 'CREATE':
                    # Create new tag assignment
                    tag_value = change['tag_value']
                    url = f"{host}/api/2.1/unity-catalog/entity-tag-assignments"
                    
                    payload = {
                        'entity_type': entity_type,
                        'entity_name': entity_id,
                        'tag_key': tag_key,
                        'tag_value': tag_value
                    }
                    
                    headers = {
                        'Authorization': f'Bearer {token}',
                        'Content-Type': 'application/json'
                    }
                    
                    response = requests.post(url, json=payload, headers=headers)
                    
                    if response.status_code in [200, 201]:
                        print(f"   ✅ CREATE TAG successful: {tag_key} = '{tag_value}'")
                        success_count += 1
                    else:
                        print(f"   ❌ CREATE TAG failed: {response.status_code} - {response.text}")
                
                elif action == 'UPDATE':
                    # Update existing tag assignment using PATCH
                    tag_value = change['tag_value']
                    
                    # Build URL based on entity type (tables vs columns)
                    if entity_type == 'tables':
                        base_url = f"{host}/api/2.1/unity-catalog/entity-tag-assignments/tables/{entity_id}/tags/{tag_key}"
                    else:  # columns
                        base_url = f"{host}/api/2.1/unity-catalog/entity-tag-assignments/columns/{entity_id}/tags/{tag_key}"
                    
                    # update_mask as query parameter, not in payload
                    url = f"{base_url}?update_mask=tag_value"
                    
                    payload = {
                        'tag_value': tag_value
                    }
                    
                    headers = {
                        'Authorization': f'Bearer {token}',
                        'Content-Type': 'application/json'
                    }
                    
                    print(f"   🔄 PATCH URL: {url}")
                    print(f"   🔄 PATCH payload: {payload}")
                    
                    response = requests.patch(url, json=payload, headers=headers)
                    
                    if response.status_code in [200, 201]:
                        print(f"   ✅ UPDATE TAG successful: {tag_key} = '{tag_value}'")
                        success_count += 1
                    else:
                        print(f"   ❌ UPDATE TAG failed: {response.status_code} - {response.text}")
                
                elif action == 'UNSET':
                    # Delete tag assignment (when tag is removed from table/column)
                    
                    # Build URL based on entity type (tables vs columns)
                    if entity_type == 'tables':
                        url = f"{host}/api/2.1/unity-catalog/entity-tag-assignments/tables/{entity_id}/tags/{tag_key}"
                    else:  # columns
                        url = f"{host}/api/2.1/unity-catalog/entity-tag-assignments/columns/{entity_id}/tags/{tag_key}"
                    
                    headers = {
                        'Authorization': f'Bearer {token}',
                        'Content-Type': 'application/json'
                    }
                    
                    response = requests.delete(url, headers=headers)
                    
                    if response.status_code in [200, 204]:
                        print(f"   ✅ UNSET TAG successful: {tag_key}")
                        success_count += 1
                    else:
                        print(f"   ❌ UNSET TAG failed: {response.status_code} - {response.text}")
                        
            except Exception as e:
                print(f"   ❌ Exception executing tag change {i+1}: {e}")
                logger.error(f"❌ Exception executing tag change {i+1}: {e}")
        
        print(f"🏷️ Tag changes summary for {table_name}: {success_count}/{total_count} successful")
        return success_count == total_count

    def _execute_tag_changes_via_sql(self, tag_changes: list, table_name: str) -> bool:
        """Execute tag changes using SQL SET TAG / UNSET TAG statements"""
        if not tag_changes:
            return True
        
        print(f"🏷️ EXECUTING {len(tag_changes)} TAG CHANGES via SQL for {table_name}")
        
        # Get warehouse for SQL execution
        warehouse_id = self._get_warehouse_id()
        if not warehouse_id:
            print(f"⚠️ No warehouse available for SQL tag operations")
            return False
        
        success_count = 0
        total_count = len(tag_changes)
        
        for i, change in enumerate(tag_changes):
            try:
                action = change['action']
                entity_type = change['entity_type']
                entity_id = change['entity_id']
                tag_key = change['tag_key']
                
                print(f"🏷️ Executing tag change {i+1}/{total_count}: {action} {entity_id}.{tag_key}")
                
                if action == 'CREATE' or action == 'UPDATE':
                    # Use SET TAG for both create and update
                    tag_value = change['tag_value']
                    
                    if entity_type == 'tables':
                        sql_statement = f"SET TAG ON TABLE {entity_id} `{tag_key}` = `{tag_value}`"
                    else:  # columns
                        sql_statement = f"SET TAG ON COLUMN {entity_id} `{tag_key}` = `{tag_value}`"
                    
                elif action == 'UNSET':
                    # Use UNSET TAG for removal
                    if entity_type == 'tables':
                        sql_statement = f"UNSET TAG ON TABLE {entity_id} `{tag_key}`"
                    else:  # columns
                        sql_statement = f"UNSET TAG ON COLUMN {entity_id} `{tag_key}`"
                
                print(f"   🔍 Executing SQL: {sql_statement}")
                
                # Execute the SQL statement
                statement_response = self.client.statement_execution.execute_statement(
                    warehouse_id=warehouse_id,
                    statement=sql_statement,
                    wait_timeout='30s'
                )
                
                # Check if the statement succeeded (handle both string and enum values)
                state_str = str(statement_response.status.state)
                if state_str in ['SUCCEEDED', 'StatementState.SUCCEEDED'] or statement_response.status.state.name == 'SUCCEEDED':
                    print(f"   ✅ {action} TAG successful: {tag_key}")
                    success_count += 1
                else:
                    print(f"   ❌ {action} TAG failed: {state_str}")
                    if statement_response.status.error:
                        print(f"   ❌ Error: {statement_response.status.error}")
                        
            except Exception as e:
                print(f"   ❌ Exception executing tag change {i+1}: {e}")
                logger.error(f"❌ Exception executing tag change {i+1}: {e}")
        
        print(f"🏷️ Tag changes summary for {table_name}: {success_count}/{total_count} successful")
        return success_count == total_count

    def _apply_self_referencing_constraints(self, data_table: DataTable, catalog_name: str, schema_name: str, all_tables: List[DataTable] = None) -> bool:
        """Apply self-referencing foreign key constraints using ALTER TABLE statements"""
        full_table_name = f"{catalog_name}.{schema_name}.{data_table.name}"
        
        # Find self-referencing foreign key fields
        self_ref_constraints = []
        for field in data_table.fields:
            if field.is_foreign_key and field.foreign_key_reference:
                ref_table_name = None
                ref_field_name = None
                constraint_name = f"fk_{data_table.name}_{field.name}"
                
                # Handle object format (ForeignKeyReference)
                if hasattr(field.foreign_key_reference, 'referenced_table_id'):
                    # Find referenced table and field by ID
                    ref_table = next((t for t in all_tables if t.id == field.foreign_key_reference.referenced_table_id), None)
                    if ref_table:
                        ref_field = next((f for f in ref_table.fields if f.id == field.foreign_key_reference.referenced_field_id), None)
                        if ref_field:
                            ref_table_name = ref_table.name
                            ref_field_name = ref_field.name
                            # Use constraint name from FK reference if available
                            if field.foreign_key_reference.constraint_name:
                                constraint_name = field.foreign_key_reference.constraint_name
                
                # Handle legacy string format (format: "table_name.field_name")
                elif isinstance(field.foreign_key_reference, str) and '.' in field.foreign_key_reference:
                    ref_table_name, ref_field_name = field.foreign_key_reference.split('.', 1)
                
                # Check if this is a self-referencing constraint
                if ref_table_name == data_table.name and ref_field_name:
                    self_ref_constraints.append({
                        'field_name': field.name,
                        'ref_field_name': ref_field_name,
                        'constraint_name': constraint_name
                    })
        
        if not self_ref_constraints:
            return True  # No self-referencing constraints to apply
        
        print(f"🔄 Applying {len(self_ref_constraints)} self-referencing constraints to {full_table_name}")
        
        # Get warehouse for SQL execution
        warehouse_id = self._get_warehouse_id()
        if not warehouse_id:
            print(f"⚠️ No warehouse available for constraint operations")
            return False
        
        success_count = 0
        for i, constraint in enumerate(self_ref_constraints):
            try:
                # Generate ALTER TABLE ADD CONSTRAINT statement
                alter_sql = f"""ALTER TABLE {full_table_name} 
ADD CONSTRAINT {constraint['constraint_name']} 
FOREIGN KEY ({constraint['field_name']}) 
REFERENCES {full_table_name}({constraint['ref_field_name']})"""
                
                print(f"🔗 Adding self-referencing constraint {i+1}/{len(self_ref_constraints)}: {constraint['constraint_name']}")
                print(f"   SQL: {alter_sql}")
                
                # Execute the ALTER TABLE statement
                statement_response = self.client.statement_execution.execute_statement(
                    warehouse_id=warehouse_id,
                    statement=alter_sql,
                    wait_timeout='30s'
                )
                
                # Check if the statement succeeded
                state_str = str(statement_response.status.state)
                if state_str in ['SUCCEEDED', 'StatementState.SUCCEEDED'] or statement_response.status.state.name == 'SUCCEEDED':
                    print(f"   ✅ Self-referencing constraint {constraint['constraint_name']} added successfully")
                    success_count += 1
                else:
                    print(f"   ❌ Failed to add constraint {constraint['constraint_name']}: {state_str}")
                    if statement_response.status.error:
                        print(f"   ❌ Error: {statement_response.status.error}")
                        
            except Exception as e:
                print(f"   ❌ Exception adding constraint {constraint['constraint_name']}: {e}")
                logger.error(f"❌ Exception adding self-referencing constraint {constraint['constraint_name']}: {e}")
        
        print(f"🔗 Self-referencing constraints summary for {data_table.name}: {success_count}/{len(self_ref_constraints)} successful")
        return success_count == len(self_ref_constraints)

    def _execute_tag_statements(self, tag_statements: str, warehouse_id: str, table_name: str) -> bool:
        """Execute SET TAG statements as separate SQL commands"""
        if not tag_statements or not tag_statements.strip():
            return True
        
        # Split tag statements into individual commands
        individual_statements = [stmt.strip() for stmt in tag_statements.split(';') if stmt.strip()]
        
        success_count = 0
        total_count = len(individual_statements)
        
        for i, statement in enumerate(individual_statements):
            try:
                logger.info(f"🏷️ Executing tag statement {i+1}/{total_count}: {statement}")
                
                response = self.client.statement_execution.execute_statement(
                    warehouse_id=warehouse_id,
                    statement=statement,
                    wait_timeout="30s"
                )
                
                # Poll for completion
                statement_id = response.statement_id
                max_polls = 30  # 2.5 minutes with 5-second intervals
                poll_count = 0
                
                while response.status.state in [StatementState.PENDING, StatementState.RUNNING] and poll_count < max_polls:
                    time.sleep(5)
                    try:
                        response = self.client.statement_execution.get_statement(statement_id)
                        poll_count += 1
                    except Exception as poll_error:
                        logger.error(f"❌ Error polling tag statement: {poll_error}")
                        break
                
                if response.status.state == StatementState.SUCCEEDED:
                    logger.info(f"✅ Tag statement {i+1} executed successfully")
                    success_count += 1
                else:
                    logger.error(f"❌ Tag statement {i+1} failed: {response.status.state}")
                    if hasattr(response.status, 'error') and response.status.error:
                        logger.error(f"   Error details: {response.status.error}")
                        
            except Exception as e:
                logger.error(f"❌ Exception executing tag statement {i+1}: {e}")
        
        logger.info(f"🏷️ Tag execution summary for {table_name}: {success_count}/{total_count} successful")
        return success_count == total_count

    def _propagate_pk_changes_to_fks(self, pk_table: DataTable, all_tables: List[DataTable]) -> None:
        """Propagate Primary Key changes to all Foreign Key fields that reference it"""
        if not all_tables:
            return
        
        pk_field = pk_table.get_primary_key_field()
        if not pk_field:
            print(f"ℹ️ No PK field in table {pk_table.name} - skipping FK propagation")
            return
        
        print(f"🔄 PROPAGATING PK CHANGES from {pk_table.name}.{pk_field.name} to all FK references")
        
        propagated_count = 0
        
        # Find all FK fields that reference this PK
        for table in all_tables:
            if table.id == pk_table.id:
                continue  # Skip self
                
            for field in table.fields:
                if not field.is_foreign_key or not field.foreign_key_reference:
                    continue
                
                # Check if this FK references our PK table
                references_pk_table = False
                
                if hasattr(field.foreign_key_reference, 'referenced_table_id'):
                    # Object format - check table ID
                    if field.foreign_key_reference.referenced_table_id == pk_table.id:
                        # Also check if it references the PK field specifically
                        if field.foreign_key_reference.referenced_field_id == pk_field.id:
                            references_pk_table = True
                elif isinstance(field.foreign_key_reference, str):
                    # String format - check table name and field name
                    if '.' in field.foreign_key_reference:
                        ref_table_name, ref_field_name = field.foreign_key_reference.split('.', 1)
                        if ref_table_name == pk_table.name and ref_field_name == pk_field.name:
                            references_pk_table = True
                
                if references_pk_table:
                    print(f"   🔗 Propagating to FK: {table.name}.{field.name}")
                    
                    # Propagate ALL properties from PK to FK (except name for different-named fields)
                    changes_made = []
                    
                    # 1. Data type and parameters
                    if field.data_type != pk_field.data_type:
                        old_type = field.data_type
                        field.data_type = pk_field.data_type
                        changes_made.append(f"data_type: {old_type} → {pk_field.data_type}")
                    
                    if field.type_parameters != pk_field.type_parameters:
                        old_params = field.type_parameters
                        field.type_parameters = pk_field.type_parameters
                        changes_made.append(f"type_parameters: {old_params} → {pk_field.type_parameters}")
                    
                    # 2. Nullable (FK should match PK nullability)
                    if field.nullable != pk_field.nullable:
                        old_nullable = field.nullable
                        field.nullable = pk_field.nullable
                        changes_made.append(f"nullable: {old_nullable} → {pk_field.nullable}")
                    
                    # 3. Comment (propagate if FK doesn't have one or if different)
                    if field.comment != pk_field.comment:
                        old_comment = field.comment
                        field.comment = pk_field.comment
                        changes_made.append(f"comment: '{old_comment}' → '{pk_field.comment}'")
                    
                    # 4. Tags (merge PK tags into FK tags, but don't overwrite existing FK tags)
                    if pk_field.tags:
                        original_fk_tags = field.tags.copy() if field.tags else {}
                        if not field.tags:
                            field.tags = {}
                        
                        for tag_key, tag_value in pk_field.tags.items():
                            if tag_key not in field.tags:  # Only add if FK doesn't already have this tag
                                field.tags[tag_key] = tag_value
                                changes_made.append(f"added tag: {tag_key} = '{tag_value}'")
                    
                    # 5. Logical name (only propagate if FK doesn't have one)
                    if pk_field.logical_name and not field.logical_name:
                        field.logical_name = pk_field.logical_name
                        changes_made.append(f"logical_name: → '{pk_field.logical_name}'")
                    
                    if changes_made:
                        print(f"     ✅ Propagated: {', '.join(changes_made)}")
                        propagated_count += 1
                    else:
                        print(f"     ℹ️ No changes needed - FK already matches PK")
        
        if propagated_count > 0:
            print(f"✅ PK propagation completed: {propagated_count} FK fields updated")
        else:
            print(f"ℹ️ No FK fields needed updates from PK {pk_table.name}.{pk_field.name}")

    def _get_column_tags_from_information_schema(self, catalog_name: str, schema_name: str, table_name: str) -> dict:
        """Get column tags using Information Schema SQL query"""
        try:
            logger.debug(f"Querying INFORMATION_SCHEMA.COLUMN_TAGS for {catalog_name}.{schema_name}.{table_name}")
            
            # SQL query to get column tags from Information Schema
            sql_query = f"""
            SELECT 
                column_name,
                tag_name,
                tag_value
            FROM {catalog_name}.information_schema.column_tags 
            WHERE catalog_name = '{catalog_name}' 
              AND schema_name = '{schema_name}' 
              AND table_name = '{table_name}'
            """
            
            logger.debug(f"SQL Query: {sql_query}")
            
            # Execute the query using Databricks SQL execution API
            warehouse_id = self._get_warehouse_id()
            if not warehouse_id:
                print(f"⚠️ No warehouse available for tag query")
                return {}
            
            logger.debug(f"Using warehouse ID: {warehouse_id}")
            
            # Execute SQL statement
            statement_response = self.client.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=sql_query,
                wait_timeout="30s"
            )
            
            logger.debug(f"Tag query statement ID: {statement_response.statement_id}")
            logger.debug(f"Tag query status: {statement_response.status.state}")
            
            # Parse results
            current_tags = {}
            if statement_response.result and statement_response.result.data_array:
                logger.debug(f"Found {len(statement_response.result.data_array)} tag rows")
                for row in statement_response.result.data_array:
                    if len(row) >= 3:
                        column_name = row[0]
                        tag_name = row[1] 
                        tag_value = row[2]
                        
                        if column_name not in current_tags:
                            current_tags[column_name] = {}
                        current_tags[column_name][tag_name] = tag_value
                        
                        print(f"   📋 Found tag: {column_name}.{tag_name} = '{tag_value}'")
            else:
                logger.debug("No tag data returned from query")
                if statement_response.result:
                    logger.debug("Result object exists but no data_array")
                    logger.debug(f"Result: {statement_response.result}")
            
            logger.debug(f"Final current tags: {current_tags}")
            return current_tags
            
        except Exception as e:
            print(f"❌ ERROR querying column tags: {e}")
            logger.error(f"❌ Error querying column tags for {catalog_name}.{schema_name}.{table_name}: {e}")
            return {}

    def _get_table_tags_from_information_schema(self, catalog_name: str, schema_name: str, table_name: str) -> dict:
        """Get table-level tags using Information Schema SQL query"""
        try:
            full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
            # print(f"🔍 QUERYING INFORMATION_SCHEMA.TABLE_TAGS for {full_table_name}")
            
            # SQL query to get table tags from Information Schema
            sql_query = f"""
            SELECT 
                tag_name,
                tag_value
            FROM {catalog_name}.information_schema.table_tags 
            WHERE catalog_name = '{catalog_name}' 
              AND schema_name = '{schema_name}' 
              AND table_name = '{table_name}'
            """
            
            logger.debug(f"SQL Query: {sql_query}")
            
            # Execute the query using Databricks SQL execution API
            warehouse_id = self._get_warehouse_id()
            if not warehouse_id:
                print(f"⚠️ No warehouse available for table tag query")
                return {}
            
            logger.debug(f"Using warehouse ID: {warehouse_id}")
            
            # Execute SQL statement
            statement_response = self.client.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=sql_query,
                wait_timeout="30s"
            )
            
            # print(f"🔍 Table tag query statement ID: {statement_response.statement_id}")
            # print(f"🔍 Table tag query status: {statement_response.status.state}")
            
            # Parse results
            table_tags = {}
            if statement_response.result and statement_response.result.data_array:
                # print(f"🔍 Found {len(statement_response.result.data_array)} table tag rows")
                for row in statement_response.result.data_array:
                    if len(row) >= 2:
                        tag_name = row[0]
                        tag_value = row[1] if row[1] is not None else ''
                        
                        table_tags[tag_name] = tag_value
                        print(f"   📋 Found table tag: {tag_name} = '{tag_value}'")
            else:
                print(f"🔍 No table tag data returned from query")
                if statement_response.result:
                    print(f"🔍 Result object exists but no data_array")
                    print(f"🔍 Result: {statement_response.result}")
            
            # print(f"🔍 FINAL TABLE TAGS: {table_tags}")
            return table_tags
            
        except Exception as e:
            print(f"❌ ERROR querying table tags: {e}")
            logger.error(f"❌ Error querying table tags for {catalog_name}.{schema_name}.{table_name}: {e}")
            return {}

    def _get_column_tags_from_entity_api(self, catalog_name: str, schema_name: str, table_name: str) -> dict:
        """Get column tags using EntityTagAssignments REST API"""
        try:
            print(f"🔍 QUERYING ENTITY TAG ASSIGNMENTS API for {catalog_name}.{schema_name}.{table_name}")
            
            # Get table columns first to know which columns to check
            current_table_info = self._get_table_info(catalog_name, schema_name, table_name)
            if not current_table_info or not current_table_info.columns:
                print(f"⚠️ No table info or columns found")
                return {}
            
            current_tags = {}
            
            # Get Databricks host and token from client config
            import requests
            
            # Extract host and token from the client
            host = self.client.config.host
            token = self.client.config.token
            
            print(f"🔍 Using Databricks host: {host}")
            
            # Check tags for each column using EntityTagAssignments API
            for column in current_table_info.columns:
                column_full_name = f"{catalog_name}.{schema_name}.{table_name}.{column.name}"
                print(f"🔍 Checking tags for column: {column_full_name}")
                
                try:
                    # Use correct Unity Catalog EntityTagAssignments LIST API endpoint
                    url = f"{host}/api/2.1/unity-catalog/entity-tag-assignments/columns/{column_full_name}/tags"
                    params = {}
                    headers = {
                        'Authorization': f'Bearer {token}',
                        'Content-Type': 'application/json'
                    }
                    
                    print(f"🔍 EntityTagAssignments URL: {url}")
                    print(f"🔍 API params: {params}")
                    
                    response = requests.get(url, headers=headers)
                    print(f"🔍 API response status: {response.status_code}")
                    
                    if response.status_code == 200:
                        data = response.json()
                        print(f"🔍 API response data: {data}")
                        
                        # Parse tags from Unity Catalog response
                        # Response contains tag_assignments array
                        tag_assignments = data.get('tag_assignments', [])
                        print(f"🔍 Found {len(tag_assignments)} tag assignments for {column.name}")
                        
                        if tag_assignments:
                            column_tags = {}
                            for assignment in tag_assignments:
                                tag_key = assignment.get('tag_key')
                                tag_value = assignment.get('tag_value', '')
                                if tag_key:
                                    column_tags[tag_key] = tag_value
                                    # Logging tag discovery (commented out for cleaner output)
                            
                            if column_tags:
                                current_tags[column.name] = column_tags
                        else:
                            # No tags found for column
                            pass
                    elif response.status_code == 404:
                        # No tags found for column (404)
                        pass
                    else:
                        print(f"⚠️ API error for column {column.name}: {response.status_code}")
                        print(f"⚠️ Response: {response.text}")
                        
                except Exception as col_e:
                    print(f"⚠️ Error getting tags for column {column.name}: {col_e}")
                    # Continue with other columns even if one fails
                    continue
            
            logger.debug(f"Final current tags: {current_tags}")
            return current_tags
            
        except Exception as e:
            print(f"❌ ERROR querying column tags via EntityTagAssignments API: {e}")
            logger.error(f"❌ Error querying column tags for {catalog_name}.{schema_name}.{table_name}: {e}")
            return {}

    def _get_table_tags_from_entity_api(self, catalog_name: str, schema_name: str, table_name: str) -> dict:
        """Get table-level tags using EntityTagAssignments REST API"""
        try:
            full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
            print(f"🔍 QUERYING TABLE TAG ASSIGNMENTS API for {full_table_name}")
            
            # Get Databricks host and token from client config
            import requests
            
            # Extract host and token from the client
            host = self.client.config.host
            token = self.client.config.token
            
            print(f"🔍 Using Databricks host: {host}")
            
            try:
                # Use Unity Catalog EntityTagAssignments LIST API endpoint for tables
                url = f"{host}/api/2.1/unity-catalog/entity-tag-assignments/tables/{full_table_name}/tags"
                headers = {
                    'Authorization': f'Bearer {token}',
                    'Content-Type': 'application/json'
                }
                
                print(f"🔍 Table EntityTagAssignments URL: {url}")
                
                response = requests.get(url, headers=headers)
                print(f"🔍 Table API response status: {response.status_code}")
                
                if response.status_code == 200:
                    data = response.json()
                    print(f"🔍 Table API response data: {data}")
                    
                    # Parse tags from Unity Catalog response
                    tag_assignments = data.get('tag_assignments', [])
                    print(f"🔍 Found {len(tag_assignments)} table tag assignments")
                    
                    table_tags = {}
                    if tag_assignments:
                        for assignment in tag_assignments:
                            tag_key = assignment.get('tag_key')
                            tag_value = assignment.get('tag_value', '')
                            if tag_key:
                                table_tags[tag_key] = tag_value
                                # Logging table tag discovery (commented out for cleaner output)
                    else:
                        # No tags found for table
                        pass
                    
                    # print(f"🔍 FINAL TABLE TAGS: {table_tags}")
                    return table_tags
                    
                elif response.status_code == 404:
                    # No tags found for table (404)
                    return {}
                else:
                    print(f"⚠️ Table API error: {response.status_code}")
                    print(f"⚠️ Response: {response.text}")
                    return {}
                    
            except Exception as api_e:
                print(f"⚠️ Error getting table tags: {api_e}")
                return {}
            
        except Exception as e:
            print(f"❌ ERROR querying table tags via EntityTagAssignments API: {e}")
            logger.error(f"❌ Error querying table tags for {catalog_name}.{schema_name}.{table_name}: {e}")
            return {}

    def _get_table_info(self, catalog_name: str, schema_name: str, table_name: str):
        """Get detailed table information from Databricks including column tags"""
        try:
            full_name = f"{catalog_name}.{schema_name}.{table_name}"
            tables_api = self.client.tables
            
            print(f"🔍 GETTING TABLE INFO WITH TAGS for: {full_name}")
            
            try:
                # Get table info with full_name (include_browse parameter is not supported)
                print(f"🔍 Getting table info with full_name...")
                result = tables_api.get(full_name=full_name)
                print(f"🔍 Got table info successfully")
                return result
            except TypeError as te:
                    logger.warning(f"⚠️ TypeError with full_name parameter in _get_table_info: {te}")
                    # Try alternative parameter names if the first fails
                    try:
                        print(f"🔍 Trying tables_api.get with name parameter...")
                        result = tables_api.get(name=full_name)
                        print(f"🔍 Got table info with name parameter")
                        return result
                    except TypeError as te2:
                        logger.warning(f"⚠️ TypeError with name parameter in _get_table_info: {te2}")
                        # Try positional argument
                        print(f"🔍 Trying tables_api.get with positional argument...")
                        result = tables_api.get(full_name)
                        print(f"🔍 Got table info with positional argument")
                        return result
        except Exception as e:
            logger.error(f"❌ Error getting table info for {catalog_name}.{schema_name}.{table_name}: {e}")
            print(f"❌ FINAL ERROR getting table info: {e}")
            return None

    def _can_alter_column_type(self, current_col, desired_field) -> bool:
        """Check if column type can be altered based on Databricks rules"""
        current_type = current_col.type_text.upper()
        desired_type = self._get_column_type_text(desired_field).upper()
        
        print(f"🔍 _CAN_ALTER_COLUMN_TYPE called:")
        print(f"   Current: '{current_type}' → Desired: '{desired_type}'")
        
        # If types are the same, no change needed
        if current_type == desired_type:
            print(f"   ✅ Types are the same - no change needed")
            return False
        
        # Define allowed type changes based on official Databricks Type Widening documentation
        # Source: https://docs.databricks.com/aws/en/delta/type-widening
        # 
        # IMPORTANT: According to the docs, type changes from byte, short, int, or long 
        # to decimal or double must be manually committed to avoid accidental promotion
        # of integers to decimals. This system handles that by requiring explicit user action.
        type_widening_rules = {
            # byte -> short, int, long, decimal, double
            'TINYINT': ['SMALLINT', 'INT', 'BIGINT', 'DECIMAL', 'DOUBLE'],
            'BYTE': ['SMALLINT', 'INT', 'BIGINT', 'DECIMAL', 'DOUBLE'],
            
            # short -> int, long, decimal, double  
            'SMALLINT': ['INT', 'BIGINT', 'DECIMAL', 'DOUBLE'],
            'SHORT': ['INT', 'BIGINT', 'DECIMAL', 'DOUBLE'],
            
            # int -> long, decimal, double
            'INT': ['BIGINT', 'DECIMAL', 'DOUBLE'],
            'INTEGER': ['BIGINT', 'DECIMAL', 'DOUBLE'],
            
            # long -> decimal (note: NOT double according to official docs)
            'BIGINT': ['DECIMAL'],
            'LONG': ['DECIMAL'],
            
            # float -> double
            'FLOAT': ['DOUBLE'],
            
            # date -> timestampNTZ
            'DATE': ['TIMESTAMP_NTZ'],
            
            # decimal -> decimal with greater precision and scale (handled separately)
        }
        
        # Handle VARCHAR/CHAR size increases
        if current_type.startswith('VARCHAR') and desired_type.startswith('VARCHAR'):
            return self._can_increase_varchar_size(current_type, desired_type)
        
        if current_type.startswith('CHAR') and desired_type.startswith('VARCHAR'):
            return True  # CHAR to VARCHAR is always allowed
        
        if (current_type.startswith('CHAR') or current_type.startswith('VARCHAR')) and desired_type == 'STRING':
            return True  # CHAR/VARCHAR to STRING is always allowed
        
        # Handle DECIMAL precision/scale changes
        if current_type.startswith('DECIMAL') and desired_type.startswith('DECIMAL'):
            # Databricks supports DECIMAL widening via Type Widening feature
            return self._can_increase_decimal_precision(current_type, desired_type)
        
        # Check type widening rules
        base_current_type = current_type.split('(')[0]  # Remove parameters
        base_desired_type = desired_type.split('(')[0]
        
        print(f"   🔍 Base types: '{base_current_type}' → '{base_desired_type}'")
        
        # Special handling for numeric types to DECIMAL with minimum precision rules
        if base_desired_type == 'DECIMAL':
            return self._can_convert_to_decimal(current_type, desired_type, base_current_type)
        
        if base_current_type in type_widening_rules:
            allowed = base_desired_type in type_widening_rules[base_current_type]
            print(f"   📋 Type widening rule found for '{base_current_type}': {type_widening_rules[base_current_type]}")
            print(f"   ✅ Change allowed: {allowed}")
            if allowed:
                print(f"   🔧 This change will require Type Widening to be enabled")
            return allowed
        
        print(f"   ❌ No type widening rule found for '{base_current_type}' - change not allowed")
        print(f"   📋 Supported source types: {list(type_widening_rules.keys())}")
        return False

    def _can_convert_to_decimal(self, current_type: str, desired_type: str, base_current_type: str) -> bool:
        """
        Check if numeric type can be converted to DECIMAL following Databricks Type Widening rules:
        
        1. When changing any numeric type to decimal, the total precision must be equal to or greater than the starting precision
        2. If you also increase the scale, the total precision must increase by a corresponding amount
        3. The minimum target for byte, short, and int types is decimal(10,0)
        4. The minimum target for long is decimal(20,0)
        5. If you want to add two decimal places to a field with decimal(10,1), the minimum target is decimal(12,3)
        """
        try:
            import re
            
            # Extract target DECIMAL precision and scale
            desired_match = re.search(r'DECIMAL\((\d+)(?:,(\d+))?\)', desired_type)
            if not desired_match:
                print(f"   ❌ Invalid DECIMAL format: {desired_type}")
                return False
                
            target_precision = int(desired_match.group(1))
            target_scale = int(desired_match.group(2) or 0)
            
            print(f"   🔍 Converting {base_current_type} to DECIMAL({target_precision},{target_scale})")
            
            # Check minimum precision requirements based on source type
            if base_current_type in ['TINYINT', 'BYTE', 'SMALLINT', 'SHORT', 'INT', 'INTEGER']:
                min_precision = 10
                print(f"   📋 Minimum precision for {base_current_type} → DECIMAL: {min_precision}")
                
                if target_precision < min_precision:
                    print(f"   ❌ Target precision {target_precision} < minimum {min_precision}")
                    return False
                    
            elif base_current_type in ['BIGINT', 'LONG']:
                min_precision = 20
                print(f"   📋 Minimum precision for {base_current_type} → DECIMAL: {min_precision}")
                
                if target_precision < min_precision:
                    print(f"   ❌ Target precision {target_precision} < minimum {min_precision}")
                    return False
                    
            elif base_current_type == 'FLOAT':
                # FLOAT can convert to DECIMAL but needs sufficient precision
                min_precision = 7  # FLOAT has ~7 decimal digits of precision
                print(f"   📋 Minimum precision for FLOAT → DECIMAL: {min_precision}")
                
                if target_precision < min_precision:
                    print(f"   ❌ Target precision {target_precision} < minimum {min_precision}")
                    return False
                    
            elif base_current_type == 'DOUBLE':
                # DOUBLE can convert to DECIMAL but needs sufficient precision  
                min_precision = 15  # DOUBLE has ~15 decimal digits of precision
                print(f"   📋 Minimum precision for DOUBLE → DECIMAL: {min_precision}")
                
                if target_precision < min_precision:
                    print(f"   ❌ Target precision {target_precision} < minimum {min_precision}")
                    return False
                    
            else:
                print(f"   ❌ Unsupported conversion from {base_current_type} to DECIMAL")
                return False
            
            print(f"   ✅ Conversion {base_current_type} → DECIMAL({target_precision},{target_scale}) is valid")
            print(f"   🔧 This change will require Type Widening to be enabled")
            return True
            
        except Exception as e:
            print(f"   ❌ Error checking numeric to DECIMAL conversion: {e}")
            return False

    def _can_increase_varchar_size(self, current_type: str, desired_type: str) -> bool:
        """Check if VARCHAR size can be increased"""
        try:
            import re
            current_match = re.search(r'VARCHAR\((\d+)\)', current_type)
            desired_match = re.search(r'VARCHAR\((\d+)\)', desired_type)
            
            if current_match and desired_match:
                current_size = int(current_match.group(1))
                desired_size = int(desired_match.group(1))
                return desired_size >= current_size
        except:
            pass
        return False

    def _can_increase_decimal_precision(self, current_type: str, desired_type: str) -> bool:
        """Check if DECIMAL precision/scale can be increased according to Databricks Type Widening rules"""
        try:
            import re
            current_match = re.search(r'DECIMAL\((\d+)(?:,(\d+))?\)', current_type)
            desired_match = re.search(r'DECIMAL\((\d+)(?:,(\d+))?\)', desired_type)
            
            if current_match and desired_match:
                current_precision = int(current_match.group(1))
                current_scale = int(current_match.group(2) or 0)
                desired_precision = int(desired_match.group(1))
                desired_scale = int(desired_match.group(2) or 0)
                
                print(f"   🔍 DECIMAL Type Widening Check:")
                print(f"      Current: DECIMAL({current_precision},{current_scale})")
                print(f"      Desired: DECIMAL({desired_precision},{desired_scale})")
                
                # Databricks Type Widening rules for DECIMAL:
                # 1. Precision must be equal or greater
                # 2. Scale can increase
                # 3. If scale increases, precision must increase proportionally
                
                precision_valid = desired_precision >= current_precision
                scale_valid = desired_scale >= current_scale
                
                # If scale increases, check if precision increased enough
                if desired_scale > current_scale:
                    scale_increase = desired_scale - current_scale
                    min_precision_increase = scale_increase
                    actual_precision_increase = desired_precision - current_precision
                    proportional_valid = actual_precision_increase >= min_precision_increase
                    
                    print(f"      Scale increase: {scale_increase}")
                    print(f"      Min precision increase needed: {min_precision_increase}")
                    print(f"      Actual precision increase: {actual_precision_increase}")
                    print(f"      Proportional increase valid: {proportional_valid}")
                    
                    result = precision_valid and scale_valid and proportional_valid
                else:
                    result = precision_valid and scale_valid
                
                print(f"      ✅ Type widening allowed: {result}")
                return result
                
        except Exception as e:
            print(f"   ❌ Error checking DECIMAL type widening: {e}")
        return False

    def _requires_type_widening(self, current_type: str, desired_type: str) -> bool:
        """Check if a type change requires Type Widening feature to be enabled"""
        # Type widening is required for all supported type changes according to Databricks docs
        # except for VARCHAR/CHAR size increases which don't require it
        
        # VARCHAR/CHAR size increases don't require Type Widening
        if ((current_type.startswith('VARCHAR') and desired_type.startswith('VARCHAR')) or
            (current_type.startswith('CHAR') and desired_type.startswith('CHAR')) or
            (current_type.startswith('CHAR') and desired_type.startswith('VARCHAR')) or
            ((current_type.startswith('CHAR') or current_type.startswith('VARCHAR')) and desired_type == 'STRING')):
            return False
        
        # DECIMAL precision/scale changes require Type Widening
        if current_type.startswith('DECIMAL') and desired_type.startswith('DECIMAL'):
            return True
        
        # All other type widening changes require the feature
        base_current_type = current_type.split('(')[0]
        base_desired_type = desired_type.split('(')[0]
        
        # Check if this is a supported type widening change
        type_widening_rules = {
            'TINYINT': ['SMALLINT', 'INT', 'BIGINT', 'DECIMAL', 'DOUBLE'],
            'BYTE': ['SMALLINT', 'INT', 'BIGINT', 'DECIMAL', 'DOUBLE'],
            'SMALLINT': ['INT', 'BIGINT', 'DECIMAL', 'DOUBLE'],
            'SHORT': ['INT', 'BIGINT', 'DECIMAL', 'DOUBLE'],
            'INT': ['BIGINT', 'DECIMAL', 'DOUBLE'],
            'INTEGER': ['BIGINT', 'DECIMAL', 'DOUBLE'],
            'BIGINT': ['DECIMAL'],
            'LONG': ['DECIMAL'],
            'FLOAT': ['DOUBLE'],
            'DATE': ['TIMESTAMP_NTZ'],
        }
        
        if base_current_type in type_widening_rules:
            return base_desired_type in type_widening_rules[base_current_type]
        
        return False

    def _generate_constraint_alter_statements(self, data_table: DataTable, catalog_name: str, schema_name: str, current_table_info, all_tables: List[DataTable] = None) -> List[str]:
        """Generate ALTER statements for constraints (PK, FK) - detailed comparison"""
        statements = []
        full_name = f"{catalog_name}.{schema_name}.{data_table.name}"
        
        try:
            # Get current constraints
            current_constraints = getattr(current_table_info, 'table_constraints', []) or []
            print(f"🔍 CURRENT CONSTRAINTS COUNT: {len(current_constraints)}")
            current_pk_columns = set()
            current_fk_constraints = {}
            
            for i, constraint in enumerate(current_constraints):
                print(f"🔍 CONSTRAINT {i+1}:")
                
                # Check for PRIMARY KEY constraint
                if hasattr(constraint, 'primary_key_constraint') and constraint.primary_key_constraint:
                    pk_constraint = constraint.primary_key_constraint
                    pk_cols = getattr(pk_constraint, 'child_columns', []) or []
                    current_pk_columns.update(pk_cols)
                    print(f"   📌 PRIMARY KEY: {pk_constraint.name} - columns: {pk_cols}")
                
                # Check for FOREIGN KEY constraint
                elif hasattr(constraint, 'foreign_key_constraint') and constraint.foreign_key_constraint:
                    fk_constraint = constraint.foreign_key_constraint
                    constraint_name = getattr(fk_constraint, 'name', None)
                    fk_cols = getattr(fk_constraint, 'child_columns', []) or []
                    parent_table = getattr(fk_constraint, 'parent_table', '')
                    parent_cols = getattr(fk_constraint, 'parent_columns', []) or []
                    
                    print(f"   🔗 FOREIGN KEY: {constraint_name}")
                    print(f"       Child columns: {fk_cols}")
                    print(f"       Parent table: {parent_table}")
                    print(f"       Parent columns: {parent_cols}")
                    
                    if constraint_name:
                        current_fk_constraints[constraint_name] = fk_constraint
                
                # Check for other named constraints
                elif hasattr(constraint, 'named_table_constraint') and constraint.named_table_constraint:
                    named_constraint = constraint.named_table_constraint
                    print(f"   📋 NAMED CONSTRAINT: {getattr(named_constraint, 'name', 'Unknown')}")
                
                else:
                    print(f"   ❓ UNKNOWN CONSTRAINT TYPE")
            
            # 1. Handle PRIMARY KEY changes
            desired_pk_field = data_table.get_primary_key_field()
            desired_pk_columns = {desired_pk_field.name} if desired_pk_field else set()
            
            print(f"🔍 PK COMPARISON - Current: {current_pk_columns}, Desired: {desired_pk_columns}")
            logger.info(f"🔍 PK Comparison - Current: {current_pk_columns}, Desired: {desired_pk_columns}")
            
            # Only make changes if PK is actually different
            if current_pk_columns != desired_pk_columns:
                print(f"❗ PK MISMATCH DETECTED - WILL GENERATE ALTER STATEMENTS")
                # Drop existing PK if it exists and is different
                if current_pk_columns:
                    logger.info(f"🔑 Dropping existing primary key: {current_pk_columns}")
                    statements.append(f"ALTER TABLE {full_name} DROP PRIMARY KEY;")
                
                # Add new PK if desired
                if desired_pk_field:
                    logger.info(f"🔑 Adding primary key constraint: {desired_pk_field.name}")
                    statements.append(f"ALTER TABLE {full_name} ADD CONSTRAINT pk_{data_table.name} PRIMARY KEY ({desired_pk_field.name});")
            else:
                logger.info(f"✅ Primary key is unchanged: {current_pk_columns}")
            
            # 2. Handle FOREIGN KEY changes
            if all_tables:
                # Get desired FK constraints
                desired_fk_constraints = {}
                fk_fields = [f for f in data_table.fields if f.is_foreign_key and f.foreign_key_reference]
                
                for fk_field in fk_fields:
                    if hasattr(fk_field.foreign_key_reference, 'constraint_name'):
                        constraint_name = fk_field.foreign_key_reference.constraint_name
                        desired_fk_constraints[constraint_name] = {
                            'field': fk_field,
                            'reference': fk_field.foreign_key_reference
                        }
                
                print(f"🔍 FK COMPARISON - Current: {list(current_fk_constraints.keys())}, Desired: {list(desired_fk_constraints.keys())}")
                logger.info(f"🔍 FK Comparison - Current: {list(current_fk_constraints.keys())}, Desired: {list(desired_fk_constraints.keys())}")
                
                # Drop FK constraints that no longer exist
                for constraint_name in current_fk_constraints:
                    if constraint_name not in desired_fk_constraints:
                        print(f"❗ FK TO DROP: {constraint_name}")
                        logger.info(f"🔗 Dropping foreign key constraint: {constraint_name}")
                        statements.append(f"ALTER TABLE {full_name} DROP CONSTRAINT {constraint_name};")
                
                # Add new FK constraints
                for constraint_name, fk_info in desired_fk_constraints.items():
                    if constraint_name not in current_fk_constraints:
                        print(f"❗ FK TO ADD: {constraint_name}")
                        fk_field = fk_info['field']
                        fk_ref = fk_info['reference']
                        
                        # Find referenced table and field
                        ref_table = next((t for t in all_tables if t.id == fk_ref.referenced_table_id), None)
                        if ref_table:
                            ref_field = next((f for f in ref_table.fields if f.id == fk_ref.referenced_field_id), None)
                            if ref_field:
                                logger.info(f"🔗 Adding foreign key constraint: {constraint_name}")
                                # Use the referenced table's actual catalog and schema, not the current table's
                                ref_catalog = ref_table.catalog_name if ref_table.catalog_name else catalog_name
                                ref_schema = ref_table.schema_name if ref_table.schema_name else schema_name
                                ref_full_name = f"{ref_catalog}.{ref_schema}.{ref_table.name}"
                                statements.append(f"ALTER TABLE {full_name} ADD CONSTRAINT {constraint_name} FOREIGN KEY ({fk_field.name}) REFERENCES {ref_full_name}({ref_field.name});")
                
                # Log if no FK changes needed
                if set(current_fk_constraints.keys()) == set(desired_fk_constraints.keys()):
                    logger.info(f"✅ Foreign key constraints are unchanged")
            
        except Exception as e:
            logger.error(f"❌ Error generating constraint ALTER statements: {e}")
        
        return statements

    def _detect_column_renames(self, current_table_info, data_table: DataTable) -> dict:
        """Detect column renames by matching field positions and types"""
        renames = {}
        
        try:
            current_columns = list(current_table_info.columns or [])
            desired_fields = list(data_table.fields)
            
            # Simple heuristic: match by position and type for potential renames
            for i, current_col in enumerate(current_columns):
                if i < len(desired_fields):
                    desired_field = desired_fields[i]
                    
                    # Check if names are different but types match (potential rename)
                    if (current_col.name != desired_field.name and 
                        current_col.type_text.upper() == self._get_column_type_text(desired_field).upper()):
                        
                        # Additional check: ensure the desired name doesn't already exist
                        current_names = {col.name for col in current_columns}
                        if desired_field.name not in current_names:
                            logger.info(f"🔍 Detected potential column rename: {current_col.name} → {desired_field.name}")
                            renames[current_col.name] = desired_field.name
            
            return renames
            
        except Exception as e:
            logger.error(f"❌ Error detecting column renames: {e}")
            return {}

    def _table_exists(self, catalog_name: str, schema_name: str, table_name: str) -> bool:
        """Check if a table exists in the catalog"""
        try:
            full_name = f"{catalog_name}.{schema_name}.{table_name}"
            logger.info(f"🔍 Checking if table exists: {full_name}")
            
            # Check if client is available
            if not self.client:
                logger.warning(f"⚠️ No Databricks client available - assuming table {full_name} does not exist")
                return False
            
            logger.info(f"🔗 Databricks client available: {type(self.client)}")
            
            tables_api = self.client.tables
            logger.info(f"🔗 Tables API available: {type(tables_api)}")
            
            # Try to get the table - if it exists, this won't raise an exception
            logger.info(f"🔍 Calling tables_api.get(full_name='{full_name}')")
            try:
                # Try the standard parameter name first
                table_info = tables_api.get(full_name=full_name)
            except TypeError as te:
                logger.warning(f"⚠️ TypeError with full_name parameter: {te}")
                # Try alternative parameter names if the first fails
                try:
                    table_info = tables_api.get(name=full_name)
                    logger.info(f"✅ Success with 'name' parameter")
                except TypeError as te2:
                    logger.warning(f"⚠️ TypeError with name parameter: {te2}")
                    # Try positional argument
                    table_info = tables_api.get(full_name)
                    logger.info(f"✅ Success with positional argument")
            logger.info(f"✅ Table {full_name} exists! Type: {type(table_info)}")
            return True
        except Exception as e:
            error_str = str(e)
            logger.info(f"❌ Table {full_name} does not exist or error occurred: {error_str}")
            
            # Check for specific error types that indicate table doesn't exist
            if any(keyword in error_str.lower() for keyword in ['not found', 'does not exist', 'table_or_view_not_found']):
                logger.info(f"🔍 Confirmed: Table {full_name} does not exist")
                return False
            else:
                logger.warning(f"⚠️ Unexpected error checking table existence for {full_name}: {error_str}")
                # In case of unexpected errors, assume table doesn't exist to be safe
                return False

    def _get_existing_columns(self, catalog_name: str, schema_name: str, table_name: str) -> set:
        """Get existing column names for a table"""
        try:
            tables_api = self.client.tables
            table_info = tables_api.get(full_name=f"{catalog_name}.{schema_name}.{table_name}")
            return {col.name for col in table_info.columns or []}
        except Exception:
            return set()
    
    def _convert_table_info_to_data_table(self, table_info: TableInfo, position_index: int) -> DataTable:
        """Convert Databricks TableInfo to DataTable model"""
        # Convert columns to fields
        fields = []
        for column in table_info.columns or []:
            # Get the raw type name
            raw_type_name = column.type_name.value if hasattr(column.type_name, 'value') else str(column.type_name)
            
            # Extract type and parameters from the type name
            data_type, type_parameters = self._extract_type_and_parameters(raw_type_name)
            
            # For DECIMAL, also check precision/scale fields if parameters not extracted from type name
            if data_type == DatabricksDataType.DECIMAL and not type_parameters:
                if column.type_precision and column.type_scale:
                    type_parameters = f"{column.type_precision},{column.type_scale}"
                elif column.type_precision:
                    type_parameters = str(column.type_precision)
            
            field = TableField(
                id=str(uuid.uuid4()),
                name=column.name,
                data_type=data_type,
                type_parameters=type_parameters,
                nullable=column.nullable if column.nullable is not None else True,
                comment=column.comment
            )
            fields.append(field)
        
        # Position tables in a grid layout
        grid_cols = 3
        x = (position_index % grid_cols) * 250 + 100
        y = (position_index // grid_cols) * 200 + 100
        
        return DataTable(
            name=table_info.name,
            schema_name=table_info.schema_name,
            catalog_name=table_info.catalog_name,
            comment=table_info.comment,
            table_type=table_info.table_type.value if table_info.table_type else "MANAGED",
            storage_location=table_info.storage_location,
            file_format=table_info.data_source_format.value if table_info.data_source_format else "DELTA",
            fields=fields,
            position_x=x,
            position_y=y
        )

    def _convert_table_info_to_data_table_with_constraints(self, table_info: TableInfo, 
                                                         constraints: List[TableConstraint], 
                                                         position_index: int,
                                                         column_details: Dict[str, Dict[str, Any]] = None) -> DataTable:
        """Convert Databricks TableInfo to DataTable model with PK/FK detection from constraints"""
        # Convert columns to fields
        fields = []
        
        # Extract table and column tags from Databricks
        table_tags = {}
        column_tags = {}
        try:
            # Get table-level tags
            table_tags = self._get_table_tags_from_information_schema(
                table_info.catalog_name, table_info.schema_name, table_info.name
            )
            logger.info(f"🏷️ Retrieved {len(table_tags)} table tags for {table_info.name}")
            
            # Get column-level tags
            column_tags = self._get_column_tags_from_information_schema(
                table_info.catalog_name, table_info.schema_name, table_info.name
            )
            logger.info(f"🏷️ Retrieved column tags for {len(column_tags)} columns in {table_info.name}")
            
        except Exception as e:
            logger.warning(f"⚠️ Could not retrieve tags for table {table_info.name}: {e}")
        
        # Extract PK and FK info from constraints
        pk_columns = set()
        fk_info = {}  # column_name -> ForeignKeyReference
        
        for constraint in constraints:
            if constraint.primary_key_constraint:
                # Primary key constraint
                pk_constraint = constraint.primary_key_constraint
                if pk_constraint.child_columns:
                    pk_columns.update(pk_constraint.child_columns)
                    logger.info(f"🔑 Found PK columns: {pk_constraint.child_columns}")
            
            elif constraint.foreign_key_constraint:
                # Foreign key constraint
                fk_constraint = constraint.foreign_key_constraint
                if fk_constraint.child_columns and fk_constraint.parent_columns:
                    for i, child_col in enumerate(fk_constraint.child_columns):
                        if i < len(fk_constraint.parent_columns):
                            parent_col = fk_constraint.parent_columns[i]
                            
                            # Parse referenced table from parent_table
                            referenced_table_name = None
                            if hasattr(fk_constraint, 'parent_table') and fk_constraint.parent_table:
                                # parent_table format: "catalog.schema.table" or "table"
                                parts = fk_constraint.parent_table.split('.')
                                if len(parts) >= 3:
                                    # Full qualified name - use as is
                                    referenced_table_name = fk_constraint.parent_table
                                elif len(parts) == 1:
                                    # Just table name - assume same catalog/schema
                                    referenced_table_name = f"{catalog_name}.{schema_name}.{parts[0]}"
                            
                            if referenced_table_name:
                                # Try different possible attribute names for constraint name
                                constraint_name = None
                                if hasattr(constraint, 'constraint_name'):
                                    constraint_name = constraint.constraint_name
                                elif hasattr(constraint, 'name'):
                                    constraint_name = constraint.name
                                else:
                                    constraint_name = f"fk_{table_info.name}_{child_col}"
                                
                                fk_info[child_col] = {
                                    'referenced_table': referenced_table_name,
                                    'referenced_column': parent_col,
                                    'constraint_name': constraint_name
                                }
                                logger.info(f"🔗 Found FK: {child_col} -> {referenced_table_name}.{parent_col}")
        
        # Create fields with PK/FK information
        for column in table_info.columns or []:
            # Check if we have SQL column details for this column
            sql_column_info = column_details.get(column.name) if column_details else None
            
            if sql_column_info and 'type_text' in sql_column_info:
                # Use the SQL type text which should have full type information
                raw_type_name = sql_column_info['type_text']
                logger.info(f"🔍 Using SQL type for '{column.name}': '{raw_type_name}'")
            else:
                # Fall back to SDK type name
                raw_type_name = column.type_name.value if hasattr(column.type_name, 'value') else str(column.type_name)
                logger.info(f"🔍 Using SDK type for '{column.name}': '{raw_type_name}'")
            
            # Extract type and parameters from the type name
            data_type, type_parameters = self._extract_type_and_parameters(raw_type_name)
            
            # For DECIMAL, also check precision/scale fields if parameters not extracted from type name
            if data_type == DatabricksDataType.DECIMAL and not type_parameters:
                if hasattr(column, 'type_precision') and hasattr(column, 'type_scale') and column.type_precision and column.type_scale:
                    type_parameters = f"{column.type_precision},{column.type_scale}"
                elif hasattr(column, 'type_precision') and column.type_precision:
                    type_parameters = str(column.type_precision)
            
            # For VARCHAR/CHAR, check if we have type_length field
            elif data_type in [DatabricksDataType.VARCHAR, DatabricksDataType.CHAR] and not type_parameters:
                if hasattr(column, 'type_length') and column.type_length:
                    type_parameters = str(column.type_length)
            
            # Check if this column is a PK or FK
            is_primary_key = column.name in pk_columns
            is_foreign_key = column.name in fk_info
            
            # Don't set foreign_key_reference yet - will be set after all tables are created
            # to avoid Pydantic validation errors with temporary format
            
            # Get tags for this column
            field_tags = column_tags.get(column.name, {})
            
            # Extract logical_name from field tags if it exists
            field_logical_name = field_tags.get('logical_name', None)
            
            field = TableField(
                id=str(uuid.uuid4()),
                name=column.name,
                data_type=data_type,
                type_parameters=type_parameters,
                nullable=column.nullable if column.nullable is not None else True,
                comment=column.comment,
                logical_name=field_logical_name,  # Set logical_name from tags
                is_primary_key=is_primary_key,
                is_foreign_key=is_foreign_key,
                foreign_key_reference=None,  # Will be set later
                tags=field_tags  # Include imported tags
            )
            
            # Store FK info for later processing
            if is_foreign_key:
                fk_data = fk_info[column.name]
                field._temp_fk_info = {
                    'referenced_table_name': fk_data['referenced_table'],
                    'referenced_column_name': fk_data['referenced_column'],
                    'constraint_name': fk_data['constraint_name']
                }
            fields.append(field)
        
        # Extract logical_name from tags if it exists
        logical_name = table_tags.get('logical_name', None)
        
        # Smart positioning - avoid overlaps with existing tables
        # Start from a base position and spiral outward to find empty space
        base_x, base_y = 100, 100
        spacing_x, spacing_y = 300, 250
        
        # For the first table, use base position
        if position_index == 0:
            x, y = base_x, base_y
        else:
            # Use a spiral pattern to avoid overlaps
            # Calculate spiral position based on index
            layer = int((position_index - 1) // 8) + 1  # Which spiral layer (8 positions per layer)
            pos_in_layer = (position_index - 1) % 8     # Position within the layer
            
            # Spiral positions: right, down, left, up pattern
            if pos_in_layer < 2:   # Right side
                x = base_x + layer * spacing_x
                y = base_y + (pos_in_layer * spacing_y)
            elif pos_in_layer < 4: # Bottom side  
                x = base_x + (3 - pos_in_layer) * spacing_x
                y = base_y + layer * spacing_y
            elif pos_in_layer < 6: # Left side
                x = base_x - (pos_in_layer - 4) * spacing_x
                y = base_y + (1 - (pos_in_layer - 4)) * spacing_y
            else:                  # Top side
                x = base_x + (pos_in_layer - 6) * spacing_x
                y = base_y - layer * spacing_y
            
            # Ensure minimum positions
            x = max(50, x)
            y = max(50, y)
        
        # Auto-size table based on field count and field name lengths
        field_count = len(fields)
        calculated_height = max(180, 120 + field_count * 25)  # Base height + field height
        
        # Calculate width based on longest field name + data type
        max_field_width = 0
        for field in fields:
            # Estimate character width: field name + data type + some padding
            field_text_length = len(field.name) + len(str(field.data_type)) + 10
            max_field_width = max(max_field_width, field_text_length)
        
        # Convert character width to pixels (approximate 8px per character)
        calculated_width = max(280, min(500, 150 + max_field_width * 8))
        
        # Check if liquid clustering is enabled for this table
        cluster_by_auto = False
        try:
            cluster_by_auto = self.check_liquid_clustering_enabled(
                table_info.catalog_name, table_info.schema_name, table_info.name
            )
            if cluster_by_auto:
                logger.info(f"🔗 Detected CLUSTER BY AUTO enabled for {table_info.name}")
        except Exception as e:
            logger.warning(f"⚠️ Could not check liquid clustering for {table_info.name}: {e}")

        return DataTable(
            name=table_info.name,
            schema_name=table_info.schema_name,
            catalog_name=table_info.catalog_name,
            comment=table_info.comment,
            logical_name=logical_name,  # Set logical_name from tags
            table_type=table_info.table_type.value if table_info.table_type else "MANAGED",
            storage_location=table_info.storage_location,
            file_format=table_info.data_source_format.value if table_info.data_source_format else "DELTA",
            cluster_by_auto=cluster_by_auto,  # Set clustering status from detection
            fields=fields,
            position_x=x,
            position_y=y,
            width=calculated_width,   # Auto-sized based on field names
            height=calculated_height,  # Auto-sized based on field count
            tags=table_tags  # Include imported table tags
        )

    def _extract_referenced_tables_from_constraints(self, constraints: List[TableConstraint], 
                                                  catalog_name: str, schema_name: str) -> List[str]:
        """Extract referenced table names from FK constraints, returning full qualified names"""
        referenced_tables = []
        
        for constraint in constraints:
            if constraint.foreign_key_constraint:
                fk_constraint = constraint.foreign_key_constraint
                if hasattr(fk_constraint, 'parent_table') and fk_constraint.parent_table:
                    # parent_table format: "catalog.schema.table" or just "table"
                    parts = fk_constraint.parent_table.split('.')
                    
                    if len(parts) >= 3:
                        # Full format: catalog.schema.table - use as is
                        full_table_name = fk_constraint.parent_table
                        referenced_tables.append(full_table_name)
                    elif len(parts) == 1:
                        # Just table name - assume same catalog/schema as current table
                        full_table_name = f"{catalog_name}.{schema_name}.{parts[0]}"
                        referenced_tables.append(full_table_name)
                    
                    logger.info(f"🔍 FK references table: {fk_constraint.parent_table} -> resolved to: {referenced_tables[-1] if referenced_tables else 'none'}")
                    logger.info(f"🔍 DEBUG: parent_table='{fk_constraint.parent_table}', parts={parts}, len={len(parts)}, using_catalog={catalog_name}, using_schema={schema_name}")
        
        return list(set(referenced_tables))  # Remove duplicates

    def _convert_temporary_fk_references(self, tables: List[DataTable], table_id_map: Dict[str, str]):
        """Convert temporary FK reference info to proper ForeignKeyReference objects"""
        # Create reverse lookup: table_name -> table_id
        name_to_id = {name: table_id for name, table_id in table_id_map.items()}
        
        for table in tables:
            for field in table.fields:
                if field.is_foreign_key and hasattr(field, '_temp_fk_info'):
                    temp_ref = field._temp_fk_info
                    
                    # Find referenced table ID
                    referenced_table_name = temp_ref.get('referenced_table_name')
                    referenced_table_id = name_to_id.get(referenced_table_name)
                    
                    if referenced_table_id:
                        # Find referenced field ID
                        referenced_field_id = None
                        for ref_table in tables:
                            if ref_table.id == referenced_table_id:
                                for ref_field in ref_table.fields:
                                    if ref_field.name == temp_ref.get('referenced_column_name'):
                                        referenced_field_id = ref_field.id
                                        break
                                break
                        
                        if referenced_field_id:
                            # Convert to proper ForeignKeyReference object
                            field.foreign_key_reference = ForeignKeyReference(
                                referenced_table_id=referenced_table_id,
                                referenced_field_id=referenced_field_id,
                                constraint_name=temp_ref.get('constraint_name'),
                                on_delete="NO ACTION",
                                on_update="NO ACTION"
                            )
                            logger.info(f"🔄 Converted FK reference: {table.name}.{field.name} -> {referenced_table_name}")
                            # Clean up temporary info
                            delattr(field, '_temp_fk_info')
                        else:
                            logger.warning(f"⚠️ Could not find referenced field ID for {table.name}.{field.name}")
                            field.foreign_key_reference = None
                            field.is_foreign_key = False
                            delattr(field, '_temp_fk_info')
                    else:
                        logger.warning(f"⚠️ Could not find referenced table ID for {table.name}.{field.name}")
                        field.foreign_key_reference = None
                        field.is_foreign_key = False
                        delattr(field, '_temp_fk_info')
    
    def _extract_relationships_from_constraints(self, constraints: List[TableConstraint], 
                                              table_id_map: Dict[str, str],
                                              source_table_name: str,
                                              all_tables: List[DataTable]) -> List[DataModelRelationship]:
        """Extract relationships from table constraints"""
        relationships = []
        
        for constraint in constraints:
            if constraint.foreign_key_constraint:
                fk_constraint = constraint.foreign_key_constraint
                
                if fk_constraint.child_columns and fk_constraint.parent_columns:
                    # Get source table ID (the table with the FK)
                    source_table_id = table_id_map.get(source_table_name)
                    if not source_table_id:
                        logger.warning(f"⚠️ Source table {source_table_name} not found in table_id_map")
                        logger.debug(f"🔍 Available keys in table_id_map: {list(table_id_map.keys())}")
                        continue
                    
                    # Parse referenced table name
                    referenced_table_name = None
                    if hasattr(fk_constraint, 'parent_table') and fk_constraint.parent_table:
                        parts = fk_constraint.parent_table.split('.')
                        if len(parts) >= 3:
                            # Full qualified name - use as is
                            referenced_table_name = fk_constraint.parent_table
                        elif len(parts) == 1:
                            # Just table name - assume same catalog/schema as current table
                            # Need to determine catalog/schema from source table
                            # Extract simple table name from full qualified name
                            source_simple_name = source_table_name.split('.')[-1] if '.' in source_table_name else source_table_name
                            source_table = None
                            for table in all_tables:
                                if table.name == source_simple_name:
                                    source_table = table
                                    break
                            if source_table:
                                src_catalog = source_table.catalog_name
                                src_schema = source_table.schema_name
                                referenced_table_name = f"{src_catalog}.{src_schema}.{parts[0]}"
                            else:
                                # Fallback - this shouldn't happen in normal flow
                                referenced_table_name = parts[0]
                    
                    if not referenced_table_name:
                        logger.warning(f"⚠️ Could not parse referenced table from {fk_constraint.parent_table}")
                        continue
                    
                    # Get target table ID (the table being referenced)
                    target_table_id = table_id_map.get(referenced_table_name)
                    if not target_table_id:
                        logger.warning(f"⚠️ Referenced table {referenced_table_name} not found in table_id_map")
                        continue
                    
                    # Create relationships for each FK column pair
                    for i, child_col in enumerate(fk_constraint.child_columns):
                        if i < len(fk_constraint.parent_columns):
                            parent_col = fk_constraint.parent_columns[i]
                            
                            # Find field IDs by name from the actual table objects
                            source_field_id = None
                            target_field_id = None
                            
                            # Look up source field ID (FK field)
                            for table in all_tables:
                                if table.id == source_table_id:
                                    for field in table.fields:
                                        if field.name == child_col:
                                            source_field_id = field.id
                                            break
                                    break
                            
                            # Look up target field ID (PK field)
                            for table in all_tables:
                                if table.id == target_table_id:
                                    for field in table.fields:
                                        if field.name == parent_col:
                                            target_field_id = field.id
                                            break
                                    break
                            
                            if source_field_id and target_field_id:
                                # Try different possible attribute names for constraint name
                                constraint_name = None
                                if hasattr(constraint, 'constraint_name'):
                                    constraint_name = constraint.constraint_name
                                elif hasattr(constraint, 'name'):
                                    constraint_name = constraint.name
                                else:
                                    # Use simple table name for constraint naming
                                    source_simple_name = source_table_name.split('.')[-1] if '.' in source_table_name else source_table_name
                                    constraint_name = f"fk_{source_simple_name}_{child_col}"
                                
                                relationship = DataModelRelationship(
                                    id=str(uuid.uuid4()),
                                    source_table_id=target_table_id,  # PK table (referenced table)
                                    target_table_id=source_table_id,  # FK table (table with FK)
                                    source_field_id=target_field_id,  # PK field
                                    target_field_id=source_field_id,  # FK field
                                    relationship_type="one_to_many",  # PK to FK is one-to-many
                                    constraint_name=constraint_name,
                                    fk_table_id=source_table_id,      # Table that has the FK
                                    fk_field_id=source_field_id       # FK field ID
                                )
                                
                                relationships.append(relationship)
                                logger.info(f"🔗 Created relationship: {source_table_name}.{child_col} -> {referenced_table_name}.{parent_col}")
                            else:
                                logger.warning(f"⚠️ Could not find field IDs for relationship {source_table_name}.{child_col} -> {referenced_table_name}.{parent_col}")
                                logger.warning(f"   source_field_id: {source_field_id}, target_field_id: {target_field_id}")
        
        return relationships
    
    def _extract_type_and_parameters(self, raw_type_name: str) -> Tuple[DatabricksDataType, Optional[str]]:
        """Extract data type and parameters from a raw Databricks type name"""
        import re
        
        logger.info(f"🔍 Extracting type and parameters from: '{raw_type_name}'")
        
        # Handle None or empty type names
        if not raw_type_name or raw_type_name.lower() in ['none', 'null']:
            logger.warning(f"⚠️ Received None/empty type name: '{raw_type_name}', defaulting to STRING")
            return DatabricksDataType.STRING, None
        
        # Clean up the type name
        clean_type_name = raw_type_name.replace('ColumnTypeName.', '').strip()
        
        # Handle parameterized types
        # DECIMAL(precision, scale) or DECIMAL(precision)
        decimal_match = re.match(r'DECIMAL\((\d+)(?:,\s*(\d+))?\)', clean_type_name, re.IGNORECASE)
        if decimal_match:
            precision = decimal_match.group(1)
            scale = decimal_match.group(2)
            parameters = f"{precision},{scale}" if scale else precision
            logger.info(f"📊 DECIMAL type: precision={precision}, scale={scale}")
            return DatabricksDataType.DECIMAL, parameters
        
        # VARCHAR(length) or CHAR(length)
        varchar_match = re.match(r'(VARCHAR|CHAR)\((\d+)\)', clean_type_name, re.IGNORECASE)
        if varchar_match:
            type_name = varchar_match.group(1).upper()
            length = varchar_match.group(2)
            data_type = DatabricksDataType.VARCHAR if type_name == 'VARCHAR' else DatabricksDataType.CHAR
            logger.info(f"📝 {type_name} type: length={length}")
            return data_type, length
        
        # GEOGRAPHY(srid) or GEOMETRY(srid)
        geo_match = re.match(r'(GEOGRAPHY|GEOMETRY)\(([^)]+)\)', clean_type_name, re.IGNORECASE)
        if geo_match:
            type_name = geo_match.group(1).upper()
            srid = geo_match.group(2)
            data_type = DatabricksDataType.GEOGRAPHY if type_name == 'GEOGRAPHY' else DatabricksDataType.GEOMETRY
            logger.info(f"🌍 {type_name} type: srid={srid}")
            return data_type, srid
        
        # ARRAY<elementType>
        array_match = re.match(r'ARRAY<([^>]+)>', clean_type_name, re.IGNORECASE)
        if array_match:
            element_type = array_match.group(1).strip().upper()
            logger.info(f"📋 ARRAY type: element_type={element_type}")
            return DatabricksDataType.ARRAY, element_type
        
        # MAP<keyType, valueType>
        map_match = re.match(r'MAP<([^,]+),\s*([^>]+)>', clean_type_name, re.IGNORECASE)
        if map_match:
            key_type = map_match.group(1).strip().upper()
            value_type = map_match.group(2).strip().upper()
            parameters = f"{key_type},{value_type}"
            logger.info(f"🗺️ MAP type: key_type={key_type}, value_type={value_type}")
            return DatabricksDataType.MAP, parameters
        
        # STRUCT<field1:type1,field2:type2,...>
        struct_match = re.match(r'STRUCT<([^>]+)>', clean_type_name, re.IGNORECASE)
        if struct_match:
            fields_def = struct_match.group(1).strip()
            logger.info(f"🏗️ STRUCT type: fields={fields_def}")
            return DatabricksDataType.STRUCT, fields_def
        
        # INTERVAL YEAR TO MONTH or INTERVAL DAY TO SECOND
        interval_match = re.match(r'INTERVAL\s+(YEAR\s+TO\s+MONTH|DAY\s+TO\s+SECOND)', clean_type_name, re.IGNORECASE)
        if interval_match:
            qualifier = interval_match.group(1).upper()
            logger.info(f"⏰ INTERVAL type: qualifier={qualifier}")
            return DatabricksDataType.INTERVAL, qualifier
        
        # For simple types without parameters, just map the type
        data_type = self._map_databricks_type_to_enum(clean_type_name)
        logger.info(f"✅ Simple type: {clean_type_name} -> {data_type.value}")
        return data_type, None

    def _map_databricks_type_to_enum(self, type_name: str) -> DatabricksDataType:
        """Map Databricks type name to our enum"""
        # Clean up the type name - handle enum string representations
        clean_type_name = type_name.replace('ColumnTypeName.', '').upper()
        logger.info(f"Mapping type: '{type_name}' -> '{clean_type_name}'")
        
        # Handle common type mappings
        type_mappings = {
            'LONG': DatabricksDataType.BIGINT,
            'BIGINT': DatabricksDataType.BIGINT,
            'INTEGER': DatabricksDataType.INT,
            'INT': DatabricksDataType.INT,
            'SHORT': DatabricksDataType.SMALLINT,
            'SMALLINT': DatabricksDataType.SMALLINT,
            'BYTE': DatabricksDataType.TINYINT,
            'TINYINT': DatabricksDataType.TINYINT,
            'DECIMAL': DatabricksDataType.DECIMAL,
            'NUMERIC': DatabricksDataType.DECIMAL,
            'FLOAT': DatabricksDataType.FLOAT,
            'DOUBLE': DatabricksDataType.DOUBLE,
            'BOOLEAN': DatabricksDataType.BOOLEAN,
            'BOOL': DatabricksDataType.BOOLEAN,
            'DATE': DatabricksDataType.DATE,
            'TIMESTAMP': DatabricksDataType.TIMESTAMP,
            'TIMESTAMP_NTZ': DatabricksDataType.TIMESTAMP_NTZ,
            'INTERVAL': DatabricksDataType.INTERVAL,
            'STRING': DatabricksDataType.STRING,
            'VARCHAR': DatabricksDataType.VARCHAR,
            'CHAR': DatabricksDataType.CHAR,
            'TEXT': DatabricksDataType.STRING,
            'BINARY': DatabricksDataType.BINARY,
            'GEOGRAPHY': DatabricksDataType.GEOGRAPHY,
            'GEOMETRY': DatabricksDataType.GEOMETRY,
            'VARIANT': DatabricksDataType.VARIANT,
            'OBJECT': DatabricksDataType.OBJECT,
            'ARRAY': DatabricksDataType.ARRAY,
            'MAP': DatabricksDataType.MAP,
            'STRUCT': DatabricksDataType.STRUCT,
        }
        
        if clean_type_name in type_mappings:
            mapped_type = type_mappings[clean_type_name]
            logger.info(f"Successfully mapped '{clean_type_name}' to {mapped_type.value}")
            return mapped_type
        
        # Try direct mapping
        try:
            direct_mapped = DatabricksDataType(clean_type_name)
            logger.info(f"Direct mapping '{clean_type_name}' to {direct_mapped.value}")
            return direct_mapped
        except ValueError:
            # Default to STRING if type not found
            logger.warning(f"Unknown type '{type_name}' (cleaned: '{clean_type_name}'), defaulting to STRING")
            return DatabricksDataType.STRING
    
    def _get_column_type_text(self, field: TableField) -> str:
        """Get the full type text for a column including parameters"""
        type_text = field.data_type.value
        
        if field.type_parameters:
            # Handle DECIMAL type specifically
            if field.data_type == DatabricksDataType.DECIMAL:
                if isinstance(field.type_parameters, dict):
                    precision = field.type_parameters.get('precision', 10)
                    scale = field.type_parameters.get('scale', 0)
                    type_text += f"({precision}, {scale})"
                else:
                    # Fallback for old format (string)
                    type_text += f"({field.type_parameters})"
            
            # Handle VARCHAR and CHAR types
            elif field.data_type in [DatabricksDataType.VARCHAR, DatabricksDataType.CHAR]:
                if isinstance(field.type_parameters, dict):
                    length = field.type_parameters.get('length', 50)
                    type_text += f"({length})"
                else:
                    # String format (just the length)
                    type_text += f"({field.type_parameters})"
            
            # Handle GEOGRAPHY and GEOMETRY types
            elif field.data_type in [DatabricksDataType.GEOGRAPHY, DatabricksDataType.GEOMETRY]:
                if isinstance(field.type_parameters, dict):
                    srid = field.type_parameters.get('srid', '4326')
                    type_text += f"({srid})"
                else:
                    # String format (just the SRID)
                    type_text += f"({field.type_parameters})"
            
            # Handle ARRAY types
            elif field.data_type == DatabricksDataType.ARRAY:
                if isinstance(field.type_parameters, dict):
                    element_type = field.type_parameters.get('element_type', 'STRING')
                    type_text += f"<{element_type}>"
                else:
                    # String format (just the element type)
                    type_text += f"<{field.type_parameters}>"
            
            # Handle MAP types
            elif field.data_type == DatabricksDataType.MAP:
                if isinstance(field.type_parameters, dict):
                    key_type = field.type_parameters.get('key_type', 'STRING')
                    value_type = field.type_parameters.get('value_type', 'STRING')
                    type_text += f"<{key_type},{value_type}>"
                else:
                    # String format (key_type,value_type)
                    if ',' in field.type_parameters:
                        type_text += f"<{field.type_parameters}>"
                    else:
                        type_text += f"<{field.type_parameters},STRING>"
            
            # Handle STRUCT types
            elif field.data_type == DatabricksDataType.STRUCT:
                if isinstance(field.type_parameters, dict):
                    # Convert dict to field definitions
                    fields = []
                    for field_name, field_type in field.type_parameters.items():
                        fields.append(f"{field_name}:{field_type}")
                    type_text += f"<{','.join(fields)}>"
                else:
                    # String format (field1:type1,field2:type2)
                    type_text += f"<{field.type_parameters}>"
            
            # Handle INTERVAL types
            elif field.data_type == DatabricksDataType.INTERVAL:
                if isinstance(field.type_parameters, dict):
                    qualifier = field.type_parameters.get('qualifier', 'DAY TO SECOND')
                    type_text += f" {qualifier}"
                else:
                    # String format (just the qualifier)
                    type_text += f" {field.type_parameters}"
            
            else:
                # For other types, just add the parameters as-is
                if isinstance(field.type_parameters, dict):
                    # Convert dict to string format if needed
                    params = ', '.join([str(v) for v in field.type_parameters.values()])
                    type_text += f"({params})"
                else:
                    type_text += f"({field.type_parameters})"
        
        return type_text
    
    def _parse_sql_table_references(self, sql_query: str) -> List[str]:
        """Parse SQL query to extract table references from FROM and JOIN clauses"""
        import re
        
        # Remove comments and normalize whitespace
        sql_clean = re.sub(r'--.*?\n', ' ', sql_query)  # Remove line comments
        sql_clean = re.sub(r'/\*.*?\*/', ' ', sql_clean, flags=re.DOTALL)  # Remove block comments
        sql_clean = re.sub(r'\s+', ' ', sql_clean).strip()  # Normalize whitespace
        
        table_references = []
        
        # Pattern to match table references in FROM and JOIN clauses
        # Matches: FROM table_name, JOIN table_name, FROM catalog.schema.table, etc.
        patterns = [
            r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*){0,2})',
            r'\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*){0,2})',
            r'\bINNER\s+JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*){0,2})',
            r'\bLEFT\s+JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*){0,2})',
            r'\bRIGHT\s+JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*){0,2})',
            r'\bFULL\s+JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*){0,2})',
            r'\bCROSS\s+JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*){0,2})',
        ]
        
        for pattern in patterns:
            matches = re.finditer(pattern, sql_clean, re.IGNORECASE)
            for match in matches:
                table_ref = match.group(1).strip()
                if table_ref and table_ref not in table_references:
                    table_references.append(table_ref)
        
        logger.info(f"🔍 Extracted table references from SQL: {table_references}")
        return table_references
    
    def _detect_view_type(self, view_definition: str) -> str:
        """Detect if a view is traditional or metric based on its definition"""
        # Metric views have specific YAML structure with version, source, dimensions, measures
        metric_keywords = ['version:', 'source:', 'dimensions:', 'measures:', 'joins:']
        traditional_keywords = ['SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY']
        
        # Check for metric view YAML structure
        metric_score = sum(1 for keyword in metric_keywords if keyword.lower() in view_definition.lower())
        
        # Check for traditional SQL structure
        traditional_score = sum(1 for keyword in traditional_keywords if keyword.upper() in view_definition.upper())
        
        if metric_score >= 2:  # Has at least 2 metric view keywords
            return "metric"
        elif traditional_score >= 2:  # Has at least 2 SQL keywords
            return "traditional"
        else:
            # Default to traditional if unclear
            return "traditional"
    
    def import_existing_views(self, catalog_name: str, schema_name: str, view_names: List[str]) -> List[Union[TraditionalView, MetricView]]:
        """Import existing views from Databricks, detecting type and dependencies"""
        imported_views = []
        
        for position_index, view_name in enumerate(view_names):
            try:
                # Get view definition using DESCRIBE TABLE EXTENDED or SHOW CREATE VIEW
                view_definition = self._get_view_definition(catalog_name, schema_name, view_name)
                
                # Debug: Show the actual view definition
                logger.info(f"🔍 DEBUG: Retrieved view definition for {view_name}:")
                logger.info(f"🔍 DEBUG: View definition length: {len(view_definition) if view_definition else 0}")
                if view_definition:
                    logger.info(f"🔍 DEBUG: View definition (first 500 chars): {view_definition[:500]}...")
                    logger.info(f"🔍 DEBUG: View definition (last 200 chars): ...{view_definition[-200:]}")
                
                if not view_definition:
                    logger.warning(f"⚠️ Could not get definition for view {view_name}")
                    continue
                
                # Detect view type
                view_type = self._detect_view_type(view_definition)
                logger.info(f"🔍 Detected view type for {view_name}: {view_type}")
                
                # Debug: Show view definition preview for metric view detection
                logger.info(f"🔍 DEBUG: View definition preview for {view_name}: {view_definition[:300]}...")
                if "AS $$" in view_definition:
                    logger.info(f"🔍 DEBUG: Found 'AS $$' in view definition - should be metric view")
                else:
                    logger.info(f"🔍 DEBUG: No 'AS $$' found in view definition - will be traditional view")
                
                if view_type == "metric":
                    # Parse as metric view
                    metric_view = self._parse_metric_view(view_name, view_definition, catalog_name, schema_name, position_index)
                    if metric_view:
                        imported_views.append(metric_view)
                else:
                    # Parse as traditional view
                    traditional_view = self._parse_traditional_view(view_name, view_definition, catalog_name, schema_name, position_index)
                    if traditional_view:
                        imported_views.append(traditional_view)
                
            except Exception as e:
                logger.error(f"Error importing view {view_name}: {e}")
                continue
        
        return imported_views
    
    def _get_view_definition(self, catalog_name: str, schema_name: str, view_name: str) -> Optional[str]:
        """Get the SQL definition of a view"""
        try:
            warehouse_id = self._get_warehouse_id()
            if not warehouse_id:
                logger.warning("⚠️ No warehouse available for SQL queries")
                return None
            
            full_name = f"{catalog_name}.{schema_name}.{view_name}"
            
            # Try DESCRIBE TABLE EXTENDED first (works for metric views)
            sql = f"DESCRIBE TABLE EXTENDED {full_name}"
            
            result = self.client.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=sql,
                wait_timeout="30s"
            )
            
            if result.status.state == StatementState.SUCCEEDED and result.result and result.result.data_array:
                # Look for the view definition in the DESCRIBE output
                # For metric views, look for the "View Text" or similar field
                view_definition = None
                
                for row in result.result.data_array:
                    if len(row) >= 2:
                        col_name = str(row[0]).strip()
                        col_value = str(row[1]).strip() if row[1] else ""
                        
                        # Look for view definition in various possible fields
                        if col_name.lower() in ['view text', 'view_text', 'definition', 'view_definition']:
                            view_definition = col_value
                            break
                
                if view_definition:
                    return view_definition
                
                # If not found, try SHOW CREATE VIEW as fallback
                try:
                    sql = f"SHOW CREATE VIEW {full_name}"
                    result = self.client.statement_execution.execute_statement(
                        warehouse_id=warehouse_id,
                        statement=sql,
                        wait_timeout="30s"
                    )
                    
                    if result.status.state == StatementState.SUCCEEDED and result.result and result.result.data_array:
                        for row in result.result.data_array:
                            if len(row) >= 1:
                                create_statement = row[0]
                                return create_statement
                except Exception as e:
                    logger.warning(f"⚠️ SHOW CREATE VIEW also failed for {full_name}: {e}")
            
            logger.warning(f"⚠️ Could not get view definition for {full_name}")
            return None
            
        except Exception as e:
            logger.error(f"Error getting view definition for {full_name}: {e}")
            return None
    
    def _parse_traditional_view(self, view_name: str, view_definition: str, catalog_name: str, schema_name: str, position_index: int = 0) -> Optional[TraditionalView]:
        """Parse a traditional SQL view and extract dependencies, tags, and description"""
        try:
            # Extract table references from the SQL
            referenced_tables = self._parse_sql_table_references(view_definition)
            
            # Get tags and description for the view
            view_tags = {}
            view_description = None
            
            try:
                # Get view info to extract comment/description
                if self.client:
                    table_info = self.client.tables.get(f"{catalog_name}.{schema_name}.{view_name}")
                    if table_info.comment:
                        view_description = table_info.comment
                    
                    # Get tags from table properties
                    if hasattr(table_info, 'properties') and table_info.properties:
                        view_tags = dict(table_info.properties)
                    else:
                        # Fallback to entity API for tags
                        view_tags = self._get_table_tags_from_information_schema(catalog_name, schema_name, view_name)
                        
            except Exception as e:
                logger.warning(f"⚠️ Could not get metadata for view {view_name}: {e}")
                # Fallback to entity API for tags only
                try:
                    view_tags = self._get_table_tags_from_information_schema(catalog_name, schema_name, view_name)
                except Exception as e2:
                    logger.warning(f"⚠️ Could not get tags for view {view_name}: {e2}")
            
            # Extract logical_name from tags
            logical_name = view_tags.get('logical_name', None)
            
            # Use imported description or fallback
            if not view_description:
                view_description = f"Traditional view imported from {catalog_name}.{schema_name}"
            
            # Smart positioning - use spiral pattern like tables but with different base position
            base_x, base_y = 600, 100  # Traditional views start at different position
            spacing_x, spacing_y = 300, 250
            
            if position_index == 0:
                x, y = base_x, base_y
            else:
                # Use same spiral pattern as tables
                layer = int((position_index - 1) // 8) + 1
                pos_in_layer = (position_index - 1) % 8
                
                if pos_in_layer < 2:   # Right side
                    x = base_x + layer * spacing_x
                    y = base_y + (pos_in_layer * spacing_y)
                elif pos_in_layer < 4: # Bottom side  
                    x = base_x + (3 - pos_in_layer) * spacing_x
                    y = base_y + layer * spacing_y
                elif pos_in_layer < 6: # Left side
                    x = base_x - (pos_in_layer - 4) * spacing_x
                    y = base_y + (1 - (pos_in_layer - 4)) * spacing_y
                else:                  # Top side
                    x = base_x + (pos_in_layer - 6) * spacing_x
                    y = base_y - layer * spacing_y
                
                x = max(50, x)
                y = max(50, y)
            
            # Auto-size based on SQL line count
            line_count = view_definition.count('\n') + 1
            calculated_height = max(180, 120 + line_count * 15)
            
            return TraditionalView(
                name=view_name,
                description=view_description,
                sql_query=view_definition,
                catalog_name=catalog_name,
                schema_name=schema_name,
                referenced_table_names=referenced_tables,
                tags=view_tags,
                logical_name=logical_name,
                position_x=x,
                position_y=y,
                height=calculated_height
            )
            
        except Exception as e:
            logger.error(f"Error parsing traditional view {view_name}: {e}")
            return None
    
    def _parse_metric_view(self, view_name: str, view_definition: str, catalog_name: str, schema_name: str, position_index: int = 0) -> Optional[MetricView]:
        """Parse a metric view definition from YAML structure"""
        logger.info(f"🔧 _parse_metric_view called for {view_name}")
        try:
            import yaml
            import re
            
            # Extract YAML content from the view definition
            # The view definition might already be just the YAML content, or wrapped in $$...$$
            yaml_content = view_definition.strip()
            
            # If it's wrapped in $$, extract the content
            yaml_match = re.search(r'\$\$(.*?)\$\$', view_definition, re.DOTALL)
            if yaml_match:
                yaml_content = yaml_match.group(1).strip()
            
            # If the content doesn't look like YAML, it might be a CREATE VIEW statement
            if not yaml_content.startswith(('version:', 'source:', 'dimensions:', 'measures:')):
                # Try to extract YAML from a CREATE VIEW statement
                as_match = re.search(r'AS\s+\$\$(.*?)\$\$', view_definition, re.DOTALL | re.IGNORECASE)
                if as_match:
                    yaml_content = as_match.group(1).strip()
                else:
                    logger.warning(f"⚠️ View definition doesn't appear to contain YAML: {view_definition[:100]}...")
                    return None
            
            # Parse YAML
            try:
                logger.info(f"🔍 YAML content for {view_name}:\n{yaml_content}")
                yaml_data = yaml.safe_load(yaml_content)
                logger.info(f"🔍 Parsed YAML keys: {list(yaml_data.keys()) if yaml_data else 'None'}")
            except yaml.YAMLError as e:
                logger.error(f"❌ Failed to parse YAML for metric view {view_name}: {e}")
                return None
            
            # Extract basic information
            version_value = yaml_data.get('version', '1.0')
            # Handle version as string or float
            if isinstance(version_value, (int, float)):
                version = str(version_value)
            else:
                version = version_value
            source = yaml_data.get('source', '')
            
            # Parse dimensions
            dimensions = []
            for dim_data in yaml_data.get('dimensions', []):
                dimension = MetricViewDimension(
                    name=dim_data.get('name', ''),
                    expr=dim_data.get('expr', dim_data.get('name', ''))
                )
                dimensions.append(dimension)
            
            # Parse measures
            measures = []
            for measure_data in yaml_data.get('measures', []):
                measure = MetricViewMeasure(
                    name=measure_data.get('name', ''),
                    expr=measure_data.get('expr', ''),
                    aggregation_type=measure_data.get('aggregation_type', 'SUM')
                )
                measures.append(measure)
            
            # Parse joins
            joins = []
            logger.info(f"🔧 Parsing {len(yaml_data.get('joins', []))} joins for {view_name}")
            for join_data in yaml_data.get('joins', []):
                # Debug: Show raw join data from YAML
                logger.info(f"🔍 DEBUG: Raw join data from YAML: {join_data}")
                
                # Databricks YAML uses 'joined_table' field for the table reference
                joined_table = join_data.get('joined_table', join_data.get('table', ''))
                
                # Extract join name - use 'source' field if 'name' is missing
                join_name = join_data.get('name', join_data.get('alias', ''))
                if not join_name and 'source' in join_data:
                    # Extract table name from source (e.g., "carrossoni.corp_vendas.dim_produto" -> "dim_produto")
                    join_name = join_data['source'].split('.')[-1]
                
                logger.info(f"🔍 DEBUG: Extracted join_name: '{join_name}' from join_data keys: {list(join_data.keys())}")
                
                # Parse join condition into structured components
                left_columns = []
                right_columns = []
                join_operators = []
                # Handle YAML quirk where 'on:' gets parsed as boolean True
                sql_on = join_data.get('sql_on', join_data.get('on', join_data.get(True, '')))
                
                # Debug: Show raw join data from YAML
                logger.info(f"🔍 DEBUG: Raw join data from YAML: {join_data}")
                logger.info(f"🔍 DEBUG: Join data type: {type(join_data)}")
                logger.info(f"🔍 DEBUG: Join data keys: {list(join_data.keys()) if isinstance(join_data, dict) else 'Not a dict'}")
                
                using_clause = join_data.get('using', None)
                
                logger.info(f"🔍 DEBUG: Extracted sql_on: '{sql_on}' (type: {type(sql_on)})")
                logger.info(f"🔍 DEBUG: Extracted using: '{using_clause}' (type: {type(using_clause)})")
                logger.info(f"🔍 DEBUG: Raw 'on' field: '{join_data.get('on')}' (type: {type(join_data.get('on'))})")
                logger.info(f"🔍 DEBUG: Raw 'sql_on' field: '{join_data.get('sql_on')}' (type: {type(join_data.get('sql_on'))})")
                
                # Try to parse simple join conditions like "source.id = customer.id"
                if sql_on and not using_clause:
                    logger.info(f"🔧 About to parse join condition: '{sql_on}' (type: {type(sql_on)})")
                    parsed_conditions = self._parse_join_condition(sql_on)
                    logger.info(f"🔧 Parsing join condition: '{sql_on}' -> {parsed_conditions}")
                    if parsed_conditions:
                        left_columns = [cond['left'] for cond in parsed_conditions]
                        right_columns = [cond['right'] for cond in parsed_conditions]
                        join_operators = [cond['operator'] for cond in parsed_conditions]
                        logger.info(f"🔧 Parsed arrays: left={left_columns}, right={right_columns}, ops={join_operators}")
                    else:
                        logger.warning(f"⚠️ Failed to parse join condition: '{sql_on}'")
                elif using_clause:
                    logger.info(f"🔧 Found USING clause, skipping ON condition parsing: {using_clause}")
                else:
                    logger.warning(f"⚠️ No sql_on or using clause found in join data")
                
                # Parse nested joins recursively using helper function
                def parse_joins_recursive(joins_data, parent_name="", depth=1):
                    """Recursively parse joins at any depth"""
                    parsed_joins = []
                    if not joins_data or not isinstance(joins_data, list):
                        return parsed_joins
                    
                    logger.info(f"🔧 Parsing {len(joins_data)} joins at depth {depth} for {parent_name}")
                    for join_item in joins_data:
                        join_name = join_item.get('name', join_item.get('alias', ''))
                        if not join_name and 'source' in join_item:
                            join_name = join_item['source'].split('.')[-1]
                        
                        joined_table = join_item.get('joined_table', join_item.get('table', ''))
                        if not joined_table and 'source' in join_item:
                            joined_table = join_item['source']
                        
                        # Parse join condition
                        left_columns = []
                        right_columns = []
                        join_operators = []
                        # Handle YAML quirk where 'on:' gets parsed as boolean True
                        sql_on = join_item.get('sql_on', join_item.get('on', join_item.get(True, '')))
                        
                        if sql_on and not join_item.get('using'):
                            parsed_conditions = self._parse_join_condition(sql_on)
                            if parsed_conditions:
                                left_columns = [cond['left'] for cond in parsed_conditions]
                                right_columns = [cond['right'] for cond in parsed_conditions]
                                join_operators = [cond['operator'] for cond in parsed_conditions]
                        
                        # Create join object
                        join_obj = MetricViewJoin(
                            name=join_name,
                            sql_on=sql_on,
                            join_type=join_item.get('type', 'LEFT').upper(),
                            joined_table_name=joined_table,
                            using=join_item.get('using', None),
                            left_columns=left_columns,
                            right_columns=right_columns,
                            join_operators=join_operators
                        )
                        
                        # Recursively parse deeper joins
                        if 'joins' in join_item:
                            join_obj.joins = parse_joins_recursive(join_item['joins'], join_name, depth + 1)
                        
                        parsed_joins.append(join_obj)
                    
                    return parsed_joins
                
                nested_joins = []
                if 'joins' in join_data and isinstance(join_data['joins'], list):
                    nested_joins = parse_joins_recursive(join_data['joins'], join_name, 2)
                
                # Create the main join object
                join = MetricViewJoin(
                    name=join_name or 'unknown_join',  # Ensure we always have a name
                    sql_on=sql_on,
                    join_type=join_data.get('join_type', join_data.get('type', 'LEFT')),
                    joined_table_name=joined_table or join_data.get('source', ''),  # Use 'source' as fallback
                    description=join_data.get('description', None),
                    using=join_data.get('using', None),
                    left_columns=left_columns,
                    right_columns=right_columns,
                    join_operators=join_operators,
                    joins=nested_joins if nested_joins else None
                )
                logger.info(f"🔧 Created join object: name={join.name}, left_columns={join.left_columns}, right_columns={join.right_columns}")
                joins.append(join)
                logger.info(f"🔗 Parsed join: {join.name} -> {join.joined_table_name} ({join.join_type})")
            
            # Smart positioning - use spiral pattern with different base position for metric views
            base_x, base_y = 400, 100  # Metric views start at different position
            spacing_x, spacing_y = 300, 250
            
            if position_index == 0:
                x, y = base_x, base_y
            else:
                # Use same spiral pattern as tables
                layer = int((position_index - 1) // 8) + 1
                pos_in_layer = (position_index - 1) % 8
                
                if pos_in_layer < 2:   # Right side
                    x = base_x + layer * spacing_x
                    y = base_y + (pos_in_layer * spacing_y)
                elif pos_in_layer < 4: # Bottom side  
                    x = base_x + (3 - pos_in_layer) * spacing_x
                    y = base_y + layer * spacing_y
                elif pos_in_layer < 6: # Left side
                    x = base_x - (pos_in_layer - 4) * spacing_x
                    y = base_y + (1 - (pos_in_layer - 4)) * spacing_y
                else:                  # Top side
                    x = base_x + (pos_in_layer - 6) * spacing_x
                    y = base_y - layer * spacing_y
                
                x = max(50, x)
                y = max(50, y)
            
            # Auto-size based on dimensions + measures + joins count
            content_count = len(dimensions) + len(measures) + len(joins)
            calculated_height = max(220, 150 + content_count * 20)
            
            # Create MetricView object
            # Note: source_table_id will be the full table reference (e.g., "carrossoni.corp_vendas.fact_vendas")
            # The apply logic will handle resolving this to the correct catalog/schema
            metric_view = MetricView(
                name=view_name,
                version=version,
                source_table_id=source,  # Full table reference for proper catalog/schema resolution
                dimensions=dimensions,
                measures=measures,
                joins=joins,
                catalog_name=catalog_name,  # Import catalog (may be different from source table)
                schema_name=schema_name,    # Import schema (may be different from source table)
                position_x=x,
                position_y=y,
                height=calculated_height
            )
            
            logger.info(f"✅ Successfully parsed metric view {view_name} with {len(dimensions)} dimensions, {len(measures)} measures, and {len(joins)} joins")
            return metric_view
            
        except Exception as e:
            logger.error(f"❌ Error parsing metric view {view_name}: {e}")
            return None

    def get_available_warehouses(self) -> List[Dict[str, Any]]:
        """Get list of available SQL warehouses"""
        try:
            if not self.client:
                return []
            
            warehouses = self.client.warehouses.list()
            warehouse_list = []
            
            for warehouse in warehouses:
                warehouse_list.append({
                    'id': warehouse.id,
                    'name': warehouse.name,
                    'state': str(warehouse.state),  # Convert enum to string for JSON serialization
                    'size': getattr(warehouse, 'cluster_size', 'Unknown'),
                    'auto_stop_mins': getattr(warehouse, 'auto_stop_mins', None)
                })
            
            return warehouse_list
            
        except Exception as e:
            logger.error(f"❌ Error listing warehouses: {e}")
            return []
    
    def _get_warehouse_id(self, preferred_warehouse_id: str = None) -> Optional[str]:
        """Get SQL warehouse ID for statement execution"""
        try:
            if not self.client:
                return None
            
            # If a specific warehouse is requested, use it
            if preferred_warehouse_id:
                logger.info(f"🏭 Using specified SQL warehouse: {preferred_warehouse_id}")
                return preferred_warehouse_id
            
            # Otherwise, find the first available warehouse (fallback)
            warehouses = self.client.warehouses.list()
            for warehouse in warehouses:
                if warehouse.state == "RUNNING":
                    logger.info(f"🏭 Using SQL warehouse: {warehouse.name} ({warehouse.id})")
                    return warehouse.id
            
            # If no running warehouse, try to get any warehouse
            for warehouse in warehouses:
                logger.info(f"🏭 Using SQL warehouse: {warehouse.name} ({warehouse.id}) - state: {warehouse.state}")
                return warehouse.id
            
            logger.warning("⚠️ No SQL warehouses found")
            return None
            
        except Exception as e:
            logger.error(f"❌ Error getting warehouse ID: {e}")
            return None

    # ===== METRIC VIEW METHODS =====
    
    def generate_metric_view_yaml(self, metric_view: MetricView, source_table: DataTable) -> str:
        """Generate YAML definition for a metric view following Databricks specification"""
        try:
            import yaml
            
            # Build the YAML structure according to Databricks spec
            yaml_data = {
                'version': metric_view.version,
                'source': f"{source_table.catalog_name}.{source_table.schema_name}.{source_table.name}"
            }
            
            # Add filter if specified
            if metric_view.filter:
                yaml_data['filter'] = metric_view.filter
            
            # Add joins if any
            if metric_view.joins:
                yaml_data['joins'] = []
                for join in metric_view.joins:
                    # Get the SQL ON condition
                    sql_on_condition = None
                    
                    # First try to use the sql_on field directly
                    if hasattr(join, 'sql_on') and join.sql_on and join.sql_on.strip():
                        sql_on_condition = join.sql_on.strip()
                        logger.info(f"🔧 Using direct sql_on: {sql_on_condition}")
                    
                    # If no direct sql_on, try to generate from column mappings
                    elif (hasattr(join, 'left_columns') and hasattr(join, 'right_columns') and 
                          join.left_columns and join.right_columns):
                        conditions = []
                        for i in range(min(len(join.left_columns), len(join.right_columns))):
                            left_col = join.left_columns[i]
                            right_col = join.right_columns[i]
                            operator = join.join_operators[i] if (hasattr(join, 'join_operators') and 
                                                               i < len(join.join_operators)) else '='
                            
                            if left_col and right_col:
                                # Use proper aliases according to Databricks metric view spec
                                left_alias = 'source'  # Always use 'source' for the main table
                                right_alias = join.name  # Use join name (not table name) for joined table
                                conditions.append(f"{left_alias}.{left_col} {operator} {right_alias}.{right_col}")
                        
                        if conditions:
                            sql_on_condition = ' AND '.join(conditions)
                            
                            # Add additional conditions if specified
                            if (hasattr(join, 'additional_conditions') and join.additional_conditions and 
                                join.additional_conditions.strip()):
                                sql_on_condition += f" AND {join.additional_conditions.strip()}"
                            
                            logger.info(f"🔧 Generated sql_on from columns: {sql_on_condition}")
                    
                    # Fallback: use advanced join generator if available
                    elif hasattr(join, 'generate_join_sql') and hasattr(join, 'joined_table_name') and join.joined_table_name:
                        try:
                            join_sql = join.generate_join_sql()
                            if ' ON ' in join_sql:
                                sql_on_condition = join_sql.split(' ON ', 1)[1]
                                logger.info(f"🔧 Extracted from generate_join_sql: {sql_on_condition}")
                        except Exception as e:
                            logger.warning(f"⚠️ Failed to generate join SQL: {e}")
                    
                    # Create the join data for YAML (Databricks format)
                    join_data = {
                        'name': join.name
                    }
                    
                    # Add the source table (full qualified name) - required by Databricks
                    if hasattr(join, 'joined_table_name') and join.joined_table_name:
                        # Use the joined_table_name if available - but check for duplicate catalog/schema
                        joined_table = join.joined_table_name
                        
                        # Fix duplicate catalog/schema issue (e.g., carrossoni.corp_vendas.carrossoni.corp_clientes.dim_cliente)
                        parts = joined_table.split('.')
                        if len(parts) > 3:
                            # Remove duplicate catalog/schema - keep only the last 3 parts
                            joined_table = '.'.join(parts[-3:])
                            logger.info(f"🔧 Fixed duplicate catalog/schema: {join.joined_table_name} -> {joined_table}")
                        
                        join_data['source'] = joined_table
                    else:
                        # Fallback: construct from catalog/schema and join name
                        effective_catalog = metric_view.catalog_name or source_table.catalog_name
                        effective_schema = metric_view.schema_name or source_table.schema_name
                        join_data['source'] = f"{effective_catalog}.{effective_schema}.{join.name}"
                    
                    # Add join condition - either 'on' or 'using' clause
                    # Check for USING clause first
                    logger.info(f"🔍 DEBUG: Join '{join.name}' attributes: {[attr for attr in dir(join) if not attr.startswith('_')]}")
                    logger.info(f"🔍 DEBUG: Join '{join.name}' hasattr using: {hasattr(join, 'using')}")
                    if hasattr(join, 'using'):
                        logger.info(f"🔍 DEBUG: Join '{join.name}' using value: {getattr(join, 'using', 'NOT_FOUND')}")
                    
                    if hasattr(join, 'using') and join.using and len(join.using) > 0:
                        # Use USING clause for equi-joins
                        join_data['using'] = join.using
                        logger.info(f"🔧 Added USING clause: {join.using}")
                    elif sql_on_condition:
                        logger.info(f"🔍 DEBUG: Original join condition: {sql_on_condition}")
                        
                        # Replace table aliases to use Databricks format: 'source' for main table, table name for joined table
                        databricks_condition = sql_on_condition.replace('base.', 'source.')
                        logger.info(f"🔍 DEBUG: After base. replacement: {databricks_condition}")
                        
                        # Also replace any explicit source table name references with 'source'
                        # Extract source table name from the main source table
                        source_table_name = source_table.name if hasattr(source_table, 'name') else 'fact_table'
                        logger.info(f"🔍 DEBUG: Source table name: {source_table_name}")
                        databricks_condition = databricks_condition.replace(f'{source_table_name}.', 'source.')
                        logger.info(f"🔍 DEBUG: After source table replacement: {databricks_condition}")
                        
                        # Also try common fact table patterns
                        databricks_condition = databricks_condition.replace('fact_vendas.', 'source.')
                        
                        # Fix any catalog.source.column patterns to just source.column
                        import re
                        databricks_condition = re.sub(r'\b\w+\.source\.', 'source.', databricks_condition)
                        logger.info(f"🔍 DEBUG: After catalog.source fix: {databricks_condition}")
                        
                        # Remove catalog/schema prefixes from joined table references (e.g., carrossoni.dim_produto.produto_id -> dim_produto.produto_id)
                        # Pattern: catalog.schema.table.column -> table.column OR catalog.table.column -> table.column
                        databricks_condition = re.sub(r'\b\w+\.\w+\.(\w+\.\w+)', r'\1', databricks_condition)  # 4-part -> 2-part
                        databricks_condition = re.sub(r'\b\w+\.(\w+\.\w+)', r'\1', databricks_condition)  # 3-part -> 2-part
                        logger.info(f"🔍 DEBUG: After removing catalog/schema from joined tables: {databricks_condition}")
                        
                        # Also remove any remaining fact_ patterns
                        databricks_condition = databricks_condition.replace('fact_', 'source.')
                        logger.info(f"🔍 DEBUG: After fact table replacements: {databricks_condition}")
                        
                        # Don't quote the join condition in YAML - Databricks handles quoting internally
                        join_data['on'] = databricks_condition
                        logger.info(f"🔧 Generated join condition: {databricks_condition}")
                    else:
                        # Only add placeholder ON condition if there's no USING clause
                        logger.warning(f"⚠️ No SQL ON condition or USING clause found for join '{join.name}' - skipping join condition")
                        # Don't add any join condition - let Databricks handle it or show an error
                    
                    # Add join type if not default (Databricks uses lowercase)
                    if hasattr(join, 'join_type') and join.join_type and join.join_type != 'LEFT':
                        join_data['type'] = join.join_type.lower()
                    
                    # Note: 'description' field is not supported by Databricks metric view joins
                    # Only these fields are allowed: name, source, on, using, joins
                    
                    yaml_data['joins'].append(join_data)
            
            # Add dimensions (only name and expr are supported by Databricks)
            if metric_view.dimensions:
                yaml_data['dimensions'] = []
                for dim in metric_view.dimensions:
                    dim_data = {'name': dim.name, 'expr': dim.expr}
                    # Note: description is not supported by Databricks metric views
                    yaml_data['dimensions'].append(dim_data)
            
            # Add measures
            if metric_view.measures:
                yaml_data['measures'] = []
                for measure in metric_view.measures:
                    # Generate the appropriate expression based on whether it's a window measure
                    if measure.is_window_measure and hasattr(measure, 'generate_window_expression'):
                        # Use the window expression generator
                        expr = measure.generate_window_expression(measure.expr)
                    else:
                        expr = measure.expr
                    
                    # Fix measure names and MEASURE() function calls for Databricks compatibility
                    measure_name = measure.name
                    
                    # Remove spaces from measure names for Databricks compatibility
                    if ' ' in measure_name:
                        clean_measure_name = measure_name.replace(' ', '')
                        logger.info(f"🔧 Cleaned measure name: '{measure_name}' -> '{clean_measure_name}'")
                        measure_name = clean_measure_name
                    
                    # Fix MEASURE() function calls to use clean measure names (no quotes needed)
                    if expr and 'MEASURE(' in expr:
                        import re
                        # Find all MEASURE(name) patterns and clean the names
                        def clean_measure_reference(match):
                            referenced_name = match.group(1).strip('"')  # Remove quotes if present
                            clean_referenced_name = referenced_name.replace(' ', '')  # Remove spaces
                            return f'MEASURE({clean_referenced_name})'
                        
                        expr = re.sub(r'MEASURE\(([^)]+)\)', clean_measure_reference, expr)
                        logger.info(f"🔧 Fixed measure expression: {expr}")
                    
                    measure_data = {'name': measure_name, 'expr': expr}
                    # Note: description is not supported by Databricks metric views
                    
                    # Window measure metadata is not supported in Databricks YAML
                    
                    yaml_data['measures'].append(measure_data)
            
            # Generate clean YAML
            yaml_str = yaml.dump(yaml_data, default_flow_style=False, sort_keys=False, allow_unicode=True)
            
            logger.info(f"✅ Generated YAML for metric view: {metric_view.name}")
            logger.info(f"🔍 DEBUG: Generated YAML content:\n{yaml_str}")
            return yaml_str
            
        except Exception as e:
            logger.error(f"❌ Error generating YAML for metric view {metric_view.name}: {e}")
            raise e
    
    def generate_metric_view_yaml_from_reference(self, metric_view: MetricView, source_table_ref: str) -> str:
        """Generate YAML definition for a metric view using a string table reference"""
        try:
            # Extract table name from full reference (e.g., "catalog.schema.table" -> "table")
            source_table_name = source_table_ref.split('.')[-1]
            
            # Build YAML structure
            yaml_data = {
                'version': metric_view.version,
                'source': source_table_ref  # Use the full table reference
            }
            
            # Add filter if present
            if metric_view.filter:
                yaml_data['filter'] = metric_view.filter
            
            # Add joins if present
            if metric_view.joins:
                yaml_data['joins'] = []
                for join in metric_view.joins:
                    # Use correct field names from MetricViewJoin model
                    joined_table = join.joined_table_name or f"{source_table_ref.rsplit('.', 1)[0]}.{join.name}"
                    
                    # Fix duplicate catalog/schema issue (e.g., carrossoni.corp_vendas.carrossoni.corp_clientes.dim_cliente)
                    parts = joined_table.split('.')
                    if len(parts) > 3:
                        # Remove duplicate catalog/schema - keep only the last 3 parts
                        joined_table = '.'.join(parts[-3:])
                        logger.info(f"🔧 Fixed duplicate catalog/schema in reference method: {join.joined_table_name} -> {joined_table}")
                    
                    # Also fix the join condition to use 'source' instead of source table name
                    join_condition = join.sql_on or f"source.id = {join.name}.id"
                    logger.info(f"🔍 DEBUG (reference method): Original join condition: {join_condition}")
                    
                    join_condition = join_condition.replace('base.', 'source.')
                    logger.info(f"🔍 DEBUG (reference method): After base. replacement: {join_condition}")
                    
                    # Replace any explicit source table name references with 'source'
                    if hasattr(metric_view, 'source_table') and metric_view.source_table:
                        source_name = metric_view.source_table.split('.')[-1]  # Get table name from full reference
                        logger.info(f"🔍 DEBUG (reference method): Source table name: {source_name}")
                        join_condition = join_condition.replace(f'{source_name}.', 'source.')
                        logger.info(f"🔍 DEBUG (reference method): After source table replacement: {join_condition}")
                    
                    # Also try common fact table patterns
                    join_condition = join_condition.replace('fact_vendas.', 'source.')
                    
                    # Fix any catalog.source.column patterns to just source.column
                    import re
                    join_condition = re.sub(r'\b\w+\.source\.', 'source.', join_condition)
                    logger.info(f"🔍 DEBUG (reference method): After catalog.source fix: {join_condition}")
                    
                    # Remove catalog/schema prefixes from joined table references (e.g., carrossoni.dim_produto.produto_id -> dim_produto.produto_id)
                    # Pattern: catalog.schema.table.column -> table.column OR catalog.table.column -> table.column
                    join_condition = re.sub(r'\b\w+\.\w+\.(\w+\.\w+)', r'\1', join_condition)  # 4-part -> 2-part
                    join_condition = re.sub(r'\b\w+\.(\w+\.\w+)', r'\1', join_condition)  # 3-part -> 2-part
                    logger.info(f"🔍 DEBUG (reference method): After removing catalog/schema from joined tables: {join_condition}")
                    
                    # Also remove any remaining fact_ patterns
                    join_condition = join_condition.replace('fact_', 'source.')
                    logger.info(f"🔍 DEBUG (reference method): After fact table replacements: {join_condition}")
                    
                    # Don't quote the join condition in YAML - Databricks handles quoting internally
                    join_data = {
                        'name': join.name,
                        'source': joined_table
                    }
                    
                    # Add join condition - either 'on' or 'using' clause
                    # Check for USING clause first
                    logger.info(f"🔍 DEBUG (reference method): Join '{join.name}' attributes: {[attr for attr in dir(join) if not attr.startswith('_')]}")
                    logger.info(f"🔍 DEBUG (reference method): Join '{join.name}' hasattr using: {hasattr(join, 'using')}")
                    if hasattr(join, 'using'):
                        logger.info(f"🔍 DEBUG (reference method): Join '{join.name}' using value: {getattr(join, 'using', 'NOT_FOUND')}")
                    
                    if hasattr(join, 'using') and join.using and len(join.using) > 0:
                        # Use USING clause for equi-joins
                        join_data['using'] = join.using
                        logger.info(f"🔧 Added USING clause (reference method): {join.using}")
                    elif join_condition:
                        join_data['on'] = join_condition
                        logger.info(f"🔧 Generated join condition (reference method): {join_condition}")
                    else:
                        logger.warning(f"⚠️ No SQL ON condition or USING clause found for join '{join.name}' (reference method) - skipping join condition")
                    
                    # Add join type if not default (Databricks uses lowercase)
                    if hasattr(join, 'join_type') and join.join_type and join.join_type != 'LEFT':
                        join_data['type'] = join.join_type.lower()
                    
                    # Add nested joins if present (for snowflake schema) - RECURSIVE FUNCTION
                    def process_nested_joins_recursive(joins_list, depth=0):
                        """Recursively process nested joins at any depth"""
                        nested_joins_data = []
                        logger.info(f"🔧 Processing {len(joins_list)} nested joins at depth {depth} (reference method)")
                        
                        for nested_join in joins_list:
                            nested_joined_table = nested_join.joined_table_name or f"{source_table_ref.rsplit('.', 1)[0]}.{nested_join.name}"
                            
                            # Fix duplicate catalog/schema for nested joins too
                            nested_parts = nested_joined_table.split('.')
                            if len(nested_parts) > 3:
                                nested_joined_table = '.'.join(nested_parts[-3:])
                                logger.info(f"🔧 Fixed duplicate catalog/schema in nested join (reference method): {nested_join.joined_table_name} -> {nested_joined_table}")
                            
                            nested_join_data = {
                                'name': nested_join.name,
                                'source': nested_joined_table
                            }
                            
                            # Handle nested join condition - either 'on' or 'using' clause
                            logger.info(f"🔍 DEBUG (reference method): Nested join '{nested_join.name}' at depth {depth} attributes: {[attr for attr in dir(nested_join) if not attr.startswith('_')]}")
                            logger.info(f"🔍 DEBUG (reference method): Nested join '{nested_join.name}' hasattr using: {hasattr(nested_join, 'using')}")
                            if hasattr(nested_join, 'using'):
                                logger.info(f"🔍 DEBUG (reference method): Nested join '{nested_join.name}' using value: {getattr(nested_join, 'using', 'NOT_FOUND')}")
                            
                            if hasattr(nested_join, 'using') and nested_join.using and len(nested_join.using) > 0:
                                # Use USING clause for equi-joins
                                nested_join_data['using'] = nested_join.using
                                logger.info(f"🔧 Added USING clause to nested join at depth {depth} (reference method): {nested_join.using}")
                            elif nested_join.sql_on:
                                # Process nested join condition similar to parent join
                                nested_join_condition = nested_join.sql_on
                                nested_join_condition = nested_join_condition.replace('base.', 'source.')
                                
                                # Replace source table references
                                if hasattr(metric_view, 'source_table') and metric_view.source_table:
                                    source_name = metric_view.source_table.split('.')[-1]
                                    nested_join_condition = nested_join_condition.replace(f'{source_name}.', 'source.')
                                
                                # Apply same transformations as parent join
                                nested_join_condition = nested_join_condition.replace('fact_vendas.', 'source.')
                                import re
                                nested_join_condition = re.sub(r'\b\w+\.source\.', 'source.', nested_join_condition)
                                nested_join_condition = re.sub(r'\b\w+\.\w+\.(\w+\.\w+)', r'\1', nested_join_condition)
                                nested_join_condition = re.sub(r'\b\w+\.(\w+\.\w+)', r'\1', nested_join_condition)
                                nested_join_condition = nested_join_condition.replace('fact_', 'source.')
                                
                                nested_join_data['on'] = nested_join_condition
                                logger.info(f"🔧 Generated nested join condition at depth {depth} (reference method): {nested_join_condition}")
                            else:
                                logger.warning(f"⚠️ No SQL ON condition or USING clause found for nested join '{nested_join.name}' at depth {depth} (reference method)")
                            
                            # Add nested join type if not default
                            if hasattr(nested_join, 'join_type') and nested_join.join_type and nested_join.join_type != 'LEFT':
                                nested_join_data['type'] = nested_join.join_type.lower()
                            
                            # RECURSIVELY process deeper nested joins
                            if hasattr(nested_join, 'joins') and nested_join.joins and len(nested_join.joins) > 0:
                                nested_join_data['joins'] = process_nested_joins_recursive(nested_join.joins, depth + 1)
                            
                            nested_joins_data.append(nested_join_data)
                        
                        return nested_joins_data
                    
                    if hasattr(join, 'joins') and join.joins and len(join.joins) > 0:
                        join_data['joins'] = process_nested_joins_recursive(join.joins)
                    
                    # Note: 'description' field is not supported by Databricks metric view joins
                    # Only these fields are allowed: name, source, on, using, joins
                    
                    yaml_data['joins'].append(join_data)
            
            # Add dimensions
            if metric_view.dimensions:
                yaml_data['dimensions'] = []
                for dim in metric_view.dimensions:
                    dim_data = {
                        'name': dim.name,
                        'expr': dim.expr
                    }
                    yaml_data['dimensions'].append(dim_data)
            
            # Add measures
            if metric_view.measures:
                yaml_data['measures'] = []
                for measure in metric_view.measures:
                    measure_data = {
                        'name': measure.name,
                        'expr': measure.expr
                    }
                    # Note: aggregation_type is not supported in Databricks YAML format
                    # The aggregation is specified in the expr field (e.g., "COUNT(*)")
                    yaml_data['measures'].append(measure_data)
            
            # Convert to YAML string
            import yaml
            yaml_string = yaml.dump(yaml_data, default_flow_style=False, sort_keys=False)
            
            logger.info(f"✅ Generated YAML for metric view from reference: {metric_view.name}")
            logger.info(f"🔍 DEBUG (reference method): Generated YAML content:\n{yaml_string}")
            return yaml_string
            
        except Exception as e:
            logger.error(f"❌ Error generating YAML for metric view {metric_view.name} from reference: {e}")
            raise e
    
    def create_metric_view(self, metric_view: MetricView, catalog_name: str, schema_name: str, source_table_ref: Union[DataTable, str]) -> bool:
        """Create a metric view in Databricks
        
        Args:
            metric_view: The metric view to create
            catalog_name: Target catalog name
            schema_name: Target schema name
            source_table_ref: Either a DataTable object or a string table reference (e.g., "catalog.schema.table")
        """
        try:
            if not self.client:
                logger.warning("⚠️ No Databricks client - running in demo mode")
                return True
            
            # Generate YAML definition - handle both DataTable object and string reference
            if isinstance(source_table_ref, str):
                # Create a minimal DataTable-like object for YAML generation
                source_table_name = source_table_ref.split('.')[-1]  # Extract table name from full reference
                yaml_definition = self.generate_metric_view_yaml_from_reference(metric_view, source_table_ref)
            else:
                # Use existing method for DataTable objects
                yaml_definition = self.generate_metric_view_yaml(metric_view, source_table_ref)
            
            # Create the metric view using SQL
            full_name = f"{catalog_name}.{schema_name}.{metric_view.name}"
            
            # Use CREATE VIEW with METRICS and YAML definition (Databricks metric view syntax)
            create_sql = f"""CREATE OR REPLACE VIEW {full_name}
WITH METRICS
LANGUAGE YAML
AS $$
{yaml_definition}
$$"""
            
            logger.info(f"🎯 Creating metric view: {full_name}")
            print(f"🎯 Creating metric view: {full_name}")
            
            # DEBUG: Log the exact SQL being executed
            logger.info(f"🔍 DEBUG: Executing SQL:\n{create_sql}")
            print(f"🔍 DEBUG: Executing SQL:\n{create_sql}")
            
            # Execute the SQL
            warehouse_id = self._get_warehouse_id()
            if not warehouse_id:
                logger.error("❌ No SQL warehouse available for metric view creation")
                return False
            
            response = self.client.statement_execution.execute_statement(
                statement=create_sql,
                warehouse_id=warehouse_id,
                wait_timeout="30s"
            )
            
            if response.status.state == StatementState.SUCCEEDED:
                logger.info(f"✅ Metric view created successfully: {full_name}")
                print(f"✅ Metric view created successfully: {full_name}")
                
                # Apply tags if any
                if metric_view.tags:
                    self._apply_metric_view_tags(metric_view, catalog_name, schema_name)
                
                return True
            else:
                error_msg = f"Failed to create metric view: {response.status.state}"
                if hasattr(response.status, 'error') and response.status.error:
                    error_msg += f" - {response.status.error.message}"
                
                logger.error(f"❌ {error_msg}")
                print(f"❌ {error_msg}")
                self._last_error = error_msg
                return False
                
        except Exception as e:
            error_msg = f"Error creating metric view {metric_view.name}: {str(e)}"
            logger.error(f"❌ {error_msg}")
            print(f"❌ {error_msg}")
            self._last_error = error_msg
            return False
    
    def alter_metric_view(self, metric_view: MetricView, catalog_name: str, schema_name: str, source_table_ref: Union[DataTable, str]) -> bool:
        """Alter an existing metric view in Databricks"""
        try:
            if not self.client:
                logger.warning("⚠️ No Databricks client - running in demo mode")
                return True
            
            # For metric views, we need to use CREATE OR REPLACE VIEW
            # as ALTER VIEW with YAML is not directly supported
            full_name = f"{catalog_name}.{schema_name}.{metric_view.name}"
            
            logger.info(f"🔄 Altering metric view: {full_name}")
            print(f"🔄 Altering metric view: {full_name}")
            
            # Generate new YAML definition - handle both DataTable object and string reference
            if isinstance(source_table_ref, str):
                yaml_definition = self.generate_metric_view_yaml_from_reference(metric_view, source_table_ref)
            else:
                yaml_definition = self.generate_metric_view_yaml(metric_view, source_table_ref)
            
            # Use the same syntax as create_metric_view (WITH METRICS LANGUAGE YAML)
            alter_sql = f"""CREATE OR REPLACE VIEW {full_name}
WITH METRICS
LANGUAGE YAML
AS $$
{yaml_definition}
$$"""
            
            # DEBUG: Log the exact SQL being executed
            logger.info(f"🔍 DEBUG: Executing SQL:\n{alter_sql}")
            print(f"🔍 DEBUG: Executing SQL:\n{alter_sql}")
            
            # Execute the SQL
            warehouse_id = self._get_warehouse_id()
            if not warehouse_id:
                logger.error("❌ No SQL warehouse available for metric view alteration")
                return False
            
            response = self.client.statement_execution.execute_statement(
                statement=alter_sql,
                warehouse_id=warehouse_id,
                wait_timeout="30s"
            )
            
            if response.status.state == StatementState.SUCCEEDED:
                logger.info(f"✅ Metric view altered successfully: {full_name}")
                print(f"✅ Metric view altered successfully: {full_name}")
                
                # Update tags if any
                if metric_view.tags:
                    self._apply_metric_view_tags(metric_view, catalog_name, schema_name)
                
                return True
            else:
                error_msg = f"Failed to alter metric view: {response.status.state}"
                if hasattr(response.status, 'error') and response.status.error:
                    error_msg += f" - {response.status.error.message}"
                
                logger.error(f"❌ {error_msg}")
                print(f"❌ {error_msg}")
                self._last_error = error_msg
                return False
                
        except Exception as e:
            error_msg = f"Error altering metric view {metric_view.name}: {str(e)}"
            logger.error(f"❌ {error_msg}")
            print(f"❌ {error_msg}")
            self._last_error = error_msg
            return False
    
    def _parse_join_condition(self, sql_on: str) -> List[Dict[str, str]]:
        """Parse SQL ON clause into structured conditions"""
        import re
        
        conditions = []
        if not sql_on or not sql_on.strip():
            return conditions
            
        # Split by AND (case insensitive)
        and_parts = re.split(r'\s+AND\s+', sql_on.strip(), flags=re.IGNORECASE)
        
        for part in and_parts:
            part = part.strip()
            if not part:
                continue
                
            # Match patterns like "source.field = table.field" or "table1.field = table2.field"
            # Support operators: =, !=, <>, <, >, <=, >=
            match = re.match(r'(\w+)\.(\w+)\s*(=|!=|<>|<=|>=|<|>)\s*(\w+)\.(\w+)', part.strip())
            if match:
                left_table, left_field, operator, right_table, right_field = match.groups()
                
                # For metric views, convert 'source' table references to just field names
                left_column = left_field if left_table.lower() == 'source' else f"{left_table}.{left_field}"
                right_column = right_field if right_table.lower() == 'source' else f"{right_table}.{right_field}"
                
                # If left side is not source, swap them so source is always on left
                if left_table.lower() != 'source' and right_table.lower() == 'source':
                    left_column, right_column = right_column, left_column
                    # Reverse operator if needed
                    if operator == '<':
                        operator = '>'
                    elif operator == '>':
                        operator = '<'
                    elif operator == '<=':
                        operator = '>='
                    elif operator == '>=':
                        operator = '<='
                
                conditions.append({
                    'left': left_column,
                    'operator': operator,
                    'right': right_column
                })
                
        return conditions

    def import_metric_view(self, catalog_name: str, schema_name: str, metric_view_name: str) -> Optional[MetricView]:
        """Import an existing metric view from Databricks"""
        logger.info(f"🔧 IMPORT DEBUG: Starting import of metric view {metric_view_name}")
        try:
            if not self.client:
                logger.warning("⚠️ No Databricks client - running in demo mode")
                return None
            
            full_name = f"{catalog_name}.{schema_name}.{metric_view_name}"
            logger.info(f"📥 Importing metric view: {full_name}")
            print(f"📥 Importing metric view: {full_name}")
            
            # Get metric view information
            try:
                view_info = self.client.tables.get(full_name)
            except Exception as e:
                logger.error(f"❌ Metric view not found: {full_name} - {e}")
                return None
            
            # Debug: Check what view_info contains
            logger.info(f"🔍 DEBUG: view_info type: {type(view_info)}")
            logger.info(f"🔍 DEBUG: view_info attributes: {dir(view_info)}")
            if hasattr(view_info, 'view_definition'):
                logger.info(f"🔍 DEBUG: view_definition exists: {bool(view_info.view_definition)}")
                if view_info.view_definition:
                    logger.info(f"🔍 DEBUG: view_definition preview: {view_info.view_definition[:200]}...")
            else:
                logger.info(f"🔍 DEBUG: view_definition attribute not found")
            
            # Parse the YAML definition from the view
            yaml_definition = None
            joins = []
            dimensions = []
            measures = []
            source_table_ref = ""
            
            if hasattr(view_info, 'view_definition') and view_info.view_definition:
                try:
                    # Extract YAML from the view definition
                    view_def = view_info.view_definition
                    if 'AS $$' in view_def and '$$' in view_def:
                        yaml_start = view_def.find('AS $$') + 5
                        yaml_end = view_def.rfind('$$')
                        yaml_content = view_def[yaml_start:yaml_end].strip()
                        logger.info(f"🔧 IMPORT DEBUG: Extracted YAML content:\n{yaml_content}")
                        
                        import yaml
                        yaml_data = yaml.safe_load(yaml_content)
                        logger.info(f"🔧 IMPORT DEBUG: Parsed YAML data: {yaml_data}")
                        
                        if yaml_data:
                            # Extract source table
                            source_table_ref = yaml_data.get('source', '')
                            
                            # Extract joins
                            if 'joins' in yaml_data:
                                for join_data in yaml_data['joins']:
                                    # Parse the ON clause into structured conditions
                                    left_columns = []
                                    right_columns = []
                                    join_operators = []
                                    sql_on = join_data.get('on', '')
                                    
                                    # Try to parse simple join conditions like "source.id = customer.id"
                                    if sql_on and not join_data.get('using'):
                                        parsed_conditions = self._parse_join_condition(sql_on)
                                        logger.info(f"🔧 Parsing join condition: '{sql_on}' -> {parsed_conditions}")
                                        if parsed_conditions:
                                            left_columns = [cond['left'] for cond in parsed_conditions]
                                            right_columns = [cond['right'] for cond in parsed_conditions]
                                            join_operators = [cond['operator'] for cond in parsed_conditions]
                                            logger.info(f"🔧 Parsed arrays: left={left_columns}, right={right_columns}, ops={join_operators}")
                                    
                                    join = MetricViewJoin(
                                        name=join_data.get('name', ''),
                                        sql_on=sql_on,
                                        join_type=join_data.get('type', 'LEFT').upper(),
                                        joined_table_name=join_data.get('source', ''),
                                        using=join_data.get('using', None),
                                        left_columns=left_columns,
                                        right_columns=right_columns,
                                        join_operators=join_operators
                                    )
                                    logger.info(f"🔧 Created join object: name={join.name}, left_columns={join.left_columns}, right_columns={join.right_columns}")
                                    
                                    # Handle nested joins for snowflake schema (recursively)
                                    if 'joins' in join_data:
                                        def parse_nested_joins_recursive(join_data_list, depth=0):
                                            """Recursively parse nested joins at any depth"""
                                            logger.debug(f"🔍 Parsing nested joins at depth {depth}, count: {len(join_data_list)}")
                                            nested_joins = []
                                            for i, nested_join_data in enumerate(join_data_list):
                                                logger.debug(f"🔍 Processing join {i} at depth {depth}: {nested_join_data.get('name', 'unnamed')}")
                                                # Parse nested join conditions
                                                nested_left_columns = []
                                                nested_right_columns = []
                                                nested_join_operators = []
                                                # Handle YAML quirk where 'on:' gets parsed as boolean True
                                                nested_sql_on = nested_join_data.get('sql_on', nested_join_data.get('on', nested_join_data.get(True, '')))
                                                
                                                if nested_sql_on and not nested_join_data.get('using'):
                                                    nested_parsed_conditions = self._parse_join_condition(nested_sql_on)
                                                    if nested_parsed_conditions:
                                                        nested_left_columns = [cond['left'] for cond in nested_parsed_conditions]
                                                        nested_right_columns = [cond['right'] for cond in nested_parsed_conditions]
                                                        nested_join_operators = [cond['operator'] for cond in nested_parsed_conditions]
                                                
                                                nested_join = MetricViewJoin(
                                                    name=nested_join_data.get('name', ''),
                                                    sql_on=nested_sql_on,
                                                    join_type=nested_join_data.get('type', 'LEFT').upper(),
                                                    joined_table_name=nested_join_data.get('source', ''),
                                                    using=nested_join_data.get('using', None),
                                                    left_columns=nested_left_columns,
                                                    right_columns=nested_right_columns,
                                                    join_operators=nested_join_operators
                                                )
                                                
                                                # Recursively process deeper nested joins
                                                if 'joins' in nested_join_data:
                                                    logger.debug(f"🔍 Found nested joins in {nested_join_data.get('name', 'unnamed')} at depth {depth}, going deeper...")
                                                    logger.debug(f"🔍 Nested join data: {nested_join_data['joins']}")
                                                    try:
                                                        nested_join.joins = parse_nested_joins_recursive(nested_join_data['joins'], depth + 1)
                                                        logger.debug(f"🔍 Successfully processed {len(nested_join.joins)} nested joins for {nested_join_data.get('name', 'unnamed')}")
                                                    except Exception as e:
                                                        logger.error(f"❌ Error processing nested joins for {nested_join_data.get('name', 'unnamed')}: {e}")
                                                        nested_join.joins = []
                                                else:
                                                    logger.debug(f"🔍 No nested joins found for {nested_join_data.get('name', 'unnamed')} at depth {depth}")
                                                    nested_join.joins = []
                                                
                                                nested_joins.append(nested_join)
                                            return nested_joins
                                        
                                        join.joins = parse_nested_joins_recursive(join_data['joins'], 1)
                                    
                                    joins.append(join)
                            
                            # Extract dimensions
                            if 'dimensions' in yaml_data:
                                for dim_data in yaml_data['dimensions']:
                                    dimension = MetricViewDimension(
                                        name=dim_data.get('name', ''),
                                        expr=dim_data.get('expr', ''),
                                        description=dim_data.get('comment', '')
                                    )
                                    dimensions.append(dimension)
                            
                            # Extract measures
                            if 'measures' in yaml_data:
                                for measure_data in yaml_data['measures']:
                                    measure = MetricViewMeasure(
                                        name=measure_data.get('name', ''),
                                        expr=measure_data.get('expr', ''),
                                        description=measure_data.get('comment', ''),
                                        aggregation_type='CUSTOM'
                                    )
                                    measures.append(measure)
                                    
                        logger.info(f"🔍 Parsed YAML: {len(joins)} joins, {len(dimensions)} dimensions, {len(measures)} measures")
                        
                        # Debug: Show the parsed joins
                        for i, join in enumerate(joins):
                            logger.info(f"🔍 DEBUG: Join {i}: name='{join.name}', sql_on='{join.sql_on}', joined_table_name='{join.joined_table_name}'")
                        
                except Exception as yaml_error:
                    logger.warning(f"⚠️ Could not parse YAML definition: {yaml_error}")
            
            # Create MetricView object with parsed data
            metric_view = MetricView(
                name=metric_view_name,
                description=view_info.comment or "",
                source_table_id="",  # Will need to be resolved
                source_table=source_table_ref,
                joins=joins,
                dimensions=dimensions,
                measures=measures,
                tags={}  # Will be populated separately
            )
            
            logger.info(f"✅ Imported metric view: {metric_view_name}")
            print(f"✅ Imported metric view: {metric_view_name}")
            
            # Debug: Show the final MetricView object
            logger.info(f"🔍 DEBUG: Final MetricView joins count: {len(metric_view.joins)}")
            for i, join in enumerate(metric_view.joins):
                logger.info(f"🔍 DEBUG: Final Join {i}: {join.model_dump()}")
            
            return metric_view
            
        except Exception as e:
            error_msg = f"Error importing metric view {metric_view_name}: {str(e)}"
            logger.error(f"❌ {error_msg}")
            print(f"❌ {error_msg}")
            return None
    
    def _apply_metric_view_tags(self, metric_view: MetricView, catalog_name: str, schema_name: str):
        """Apply tags to a metric view"""
        try:
            full_name = f"{catalog_name}.{schema_name}.{metric_view.name}"
            
            for tag_key, tag_value in metric_view.tags.items():
                if tag_value:  # Only apply non-empty tags
                    tag_sql = f"ALTER VIEW {full_name} SET TAGS ('{tag_key}' = '{tag_value}')"
                    
                    warehouse_id = self._get_warehouse_id()
                    if warehouse_id:
                        response = self.client.statement_execution.execute_statement(
                            statement=tag_sql,
                            warehouse_id=warehouse_id,
                            wait_timeout="10s"
                        )
                        
                        if response.status.state == StatementState.SUCCEEDED:
                            logger.info(f"✅ Applied tag {tag_key}={tag_value} to metric view {full_name}")
                        else:
                            logger.warning(f"⚠️ Failed to apply tag {tag_key} to metric view {full_name}")
                            
        except Exception as e:
            logger.warning(f"⚠️ Error applying tags to metric view: {e}")
    
    # TODO: Uncomment when Databricks Unity Catalog API supports 'views' entity type for tagging
    # def _generate_view_tag_changes(self, traditional_view, catalog_name: str, schema_name: str, current_view_info=None) -> list:
    #     """Generate tag changes for a traditional view (same pattern as table tag changes)"""
    #     tag_changes = []
    #     full_name = f"{catalog_name}.{schema_name}.{traditional_view.name}"
    #     
    #     # Get current view tags
    #     current_tags = {}
    #     if current_view_info and hasattr(current_view_info, 'properties') and current_view_info.properties:
    #         for key, value in current_view_info.properties.items():
    #             # Skip system properties and focus on user tags
    #             if not key.startswith('__') and not key.startswith('delta.') and not key.startswith('view.'):
    #                 current_tags[key] = value
    #     
    #     # Get desired view tags
    #     desired_tags = traditional_view.tags or {}
    #     
    #     logger.info(f"🏷️ Generating tag changes for view {full_name}")
    #     logger.info(f"   Current tags: {current_tags}")
    #     logger.info(f"   Desired tags: {desired_tags}")
    #     
    #     # SET new/updated view tags
    #     for tag_key, tag_value in desired_tags.items():
    #         if tag_key and tag_key.strip():
    #             current_value = current_tags.get(tag_key)
    #             if tag_value != current_value:  # Only set if different
    #                 if current_value is None:
    #                     # Tag doesn't exist, CREATE it
    #                     logger.info(f"   ➕ CREATE VIEW TAG: {tag_key} = '{tag_value}'")
    #                     tag_changes.append({
    #                         'action': 'CREATE',
    #                         'entity_type': 'views',
    #                         'entity_id': full_name,
    #                         'tag_key': tag_key,
    #                         'tag_value': tag_value or ''
    #                     })
    #                 else:
    #                     # Tag exists with different value, UPDATE it
    #                     logger.info(f"   🔄 UPDATE VIEW TAG: {tag_key} = '{tag_value}' (was: '{current_value}')")
    #                     tag_changes.append({
    #                         'action': 'UPDATE',
    #                         'entity_type': 'views',
    #                         'entity_id': full_name,
    #                         'tag_key': tag_key,
    #                         'tag_value': tag_value or ''
    #                     })
    #             else:
    #                 logger.info(f"   ✅ VIEW TAG UNCHANGED: {tag_key} = '{tag_value}'")
    #     
    #     # UNSET view tags that are no longer desired
    #     for tag_key in current_tags:
    #         if tag_key not in desired_tags:
    #             logger.info(f"   ➖ UNSET VIEW TAG: {tag_key} (was: '{current_tags[tag_key]}')")
    #             tag_changes.append({
    #                 'action': 'UNSET',
    #                 'entity_type': 'views',
    #                 'entity_id': full_name,
    #                 'tag_key': tag_key
    #             })
    #         else:
    #             logger.info(f"   ✅ VIEW TAG STILL DESIRED: {tag_key}")
    #     
    #     if not current_tags and not desired_tags:
    #         logger.info(f"   ℹ️ No tags for view {traditional_view.name}")
    #     
    #     return tag_changes
    
    
    def delete_metric_view(self, catalog_name: str, schema_name: str, metric_view_name: str) -> bool:
        """Delete a metric view from Databricks"""
        try:
            if not self.client:
                logger.warning("⚠️ No Databricks client - running in demo mode")
                return True
            
            full_name = f"{catalog_name}.{schema_name}.{metric_view_name}"
            
            logger.info(f"🗑️ Deleting metric view: {full_name}")
            print(f"🗑️ Deleting metric view: {full_name}")
            
            # Use DROP VIEW
            drop_sql = f"DROP VIEW IF EXISTS {full_name}"
            
            warehouse_id = self._get_warehouse_id()
            if not warehouse_id:
                logger.error("❌ No SQL warehouse available for metric view deletion")
                return False
            
            response = self.client.statement_execution.execute_statement(
                statement=drop_sql,
                warehouse_id=warehouse_id,
                wait_timeout="10s"
            )
            
            if response.status.state == StatementState.SUCCEEDED:
                logger.info(f"✅ Metric view deleted successfully: {full_name}")
                print(f"✅ Metric view deleted successfully: {full_name}")
                return True
            else:
                error_msg = f"Failed to delete metric view: {response.status.state}"
                logger.error(f"❌ {error_msg}")
                print(f"❌ {error_msg}")
                return False
                
        except Exception as e:
            error_msg = f"Error deleting metric view {metric_view_name}: {str(e)}"
            logger.error(f"❌ {error_msg}")
            print(f"❌ {error_msg}")
            return False
    
    def create_traditional_view(self, traditional_view, catalog_name: str, schema_name: str) -> bool:
        """Create a traditional SQL view in Databricks with proper tags and comments
        
        Args:
            traditional_view: The TraditionalView object to create
            catalog_name: Target catalog name
            schema_name: Target schema name
        """
        try:
            if not self.client:
                logger.warning("⚠️ No Databricks client - running in demo mode")
                return True
            
            # Create the traditional view using SQL with proper Databricks syntax
            full_name = f"{catalog_name}.{schema_name}.{traditional_view.name}"
            
            # Build CREATE VIEW statement with COMMENT and TBLPROPERTIES
            create_sql = f"CREATE OR REPLACE VIEW {full_name}"
            
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
                create_sql += f"\nCOMMENT '{escaped_comment}'"
            
            # Note: Tags will be applied separately using Unity Catalog API (same as tables/columns)
            
            # Validate and qualify SQL query
            from data_modeling_routes import _validate_traditional_view_sql, _qualify_table_references
            validated_sql = _validate_traditional_view_sql(traditional_view.sql_query)
            qualified_sql = _qualify_table_references(validated_sql, catalog_name, schema_name)
            create_sql += f"\nAS\n{qualified_sql}"
            
            # Ensure SQL query ends with semicolon
            if not create_sql.strip().endswith(';'):
                create_sql += ";"
            
            logger.info(f"🎯 Creating traditional view: {full_name}")
            logger.info(f"📝 SQL: {create_sql}")
            print(f"🎯 Creating traditional view: {full_name}")
            
            # Execute the SQL
            warehouse_id = self._get_warehouse_id()
            if not warehouse_id:
                logger.error("❌ No warehouse available for SQL execution")
                self._last_error = "No warehouse available for SQL execution"
                return False
            
            try:
                # Execute the CREATE VIEW statement directly (table references are now qualified)
                logger.info(f"🔧 Executing CREATE VIEW with qualified table references")
                response = self.client.statement_execution.execute_statement(
                    warehouse_id=warehouse_id,
                    statement=create_sql,
                    wait_timeout="30s"
                )
                
                if response.status.state == StatementState.SUCCEEDED:
                    logger.info(f"✅ Traditional view {traditional_view.name} created successfully")
                    print(f"✅ Traditional view {traditional_view.name} created successfully")
                    
                    # TODO: Apply tag management using Unity Catalog API when views are supported
                    # Currently, Databricks Unity Catalog API doesn't support 'views' entity type for tagging
                    # Supported types: catalogs, schemas, tables, columns, volumes, functions, modelversions
                    # See: https://docs.databricks.com/api/workspace/entitytagassignments
                    #
                    # if traditional_view.tags:
                    #     try:
                    #         # Get current view info for tag comparison
                    #         current_view_info = None
                    #         try:
                    #             tables_api = self.client.tables
                    #             current_view_info = tables_api.get(full_name=full_name)
                    #         except Exception:
                    #             pass  # View might not exist yet or no access
                    #         
                    #         # Generate tag changes using the same pattern as tables
                    #         tag_changes = self._generate_view_tag_changes(traditional_view, catalog_name, schema_name, current_view_info)
                    #         
                    #         if tag_changes:
                    #             logger.info(f"🏷️ Executing {len(tag_changes)} tag changes for view {full_name}")
                    #             success = self._execute_tag_changes_via_api(tag_changes, traditional_view.name)
                    #             
                    #             if not success:
                    #                 logger.warning(f"⚠️ View {full_name} created but some tags failed to apply")
                    #             else:
                    #                 logger.info(f"✅ All tags applied successfully for view {full_name}")
                    #         else:
                    #             logger.info(f"ℹ️ No tag changes needed for view {full_name}")
                    #             
                    #     except Exception as tag_error:
                    #         logger.warning(f"⚠️ View created successfully but tag application failed: {tag_error}")
                    
                    if traditional_view.tags:
                        logger.info(f"ℹ️ View {full_name} has tags defined, but Unity Catalog API doesn't support view tagging yet")
                    
                    return True
                else:
                    error_msg = f"Statement execution failed: {response.status.state}"
                    if hasattr(response.status, 'error') and response.status.error:
                        error_msg += f" - {response.status.error.message}"
                    
                    logger.error(f"❌ Failed to create traditional view: {error_msg}")
                    self._last_error = error_msg
                    return False
                    
            except Exception as sql_error:
                error_msg = f"SQL execution error: {str(sql_error)}"
                logger.error(f"❌ SQL execution failed: {error_msg}")
                self._last_error = error_msg
                return False
                
        except Exception as e:
            error_msg = f"Error creating traditional view {traditional_view.name}: {str(e)}"
            logger.error(f"❌ {error_msg}")
            self._last_error = error_msg
            return False