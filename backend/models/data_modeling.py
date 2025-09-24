import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, validator, field_validator

from .autoui import AutoUIWidgetSpec, get_ui_spec


class DatabricksDataType(str, Enum):
    """Databricks supported data types for Unity Catalog tables"""
    # Numeric types
    BIGINT = "BIGINT"
    INT = "INT" 
    SMALLINT = "SMALLINT"
    TINYINT = "TINYINT"
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    DECIMAL = "DECIMAL"
    
    # String types
    STRING = "STRING"
    VARCHAR = "VARCHAR"
    CHAR = "CHAR"
    
    # Date/Time types
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    TIMESTAMP_NTZ = "TIMESTAMP_NTZ"
    INTERVAL = "INTERVAL"
    
    # Boolean
    BOOLEAN = "BOOLEAN"
    
    # Binary
    BINARY = "BINARY"
    
    # Geospatial types
    GEOGRAPHY = "GEOGRAPHY"
    GEOMETRY = "GEOMETRY"
    
    # Semi-structured data types
    VARIANT = "VARIANT"
    OBJECT = "OBJECT"
    
    # Complex types
    ARRAY = "ARRAY"
    MAP = "MAP"
    STRUCT = "STRUCT"


class FieldConstraintType(str, Enum):
    """Types of field constraints"""
    PRIMARY_KEY = "PRIMARY_KEY"
    FOREIGN_KEY = "FOREIGN_KEY"
    NOT_NULL = "NOT_NULL"
    UNIQUE = "UNIQUE"
    CHECK = "CHECK"


class TableField(BaseModel):
    """Represents a field/column in a table"""
    model_config = ConfigDict(extra='allow', validate_assignment=True)
    
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str = Field(description="Field name")
    data_type: DatabricksDataType = Field(description="Data type of the field")
    type_parameters: Optional[str] = Field(default=None, description="Type parameters (e.g., VARCHAR(50), DECIMAL(10,2))")
    nullable: bool = Field(default=True, description="Whether field allows NULL values")
    default_value: Optional[str] = Field(default=None, description="Default value for the field")
    comment: Optional[str] = Field(default=None, description="Field comment/description")
    logical_name: Optional[str] = Field(default=None, description="Business/logical name for the field")
    tags: Dict[str, Optional[str]] = Field(default_factory=dict, description="Field tags for Unity Catalog")
    
    # Constraints
    is_primary_key: bool = Field(default=False, description="Whether this field is part of primary key")
    is_foreign_key: bool = Field(default=False, description="Whether this field is a foreign key")
    foreign_key_reference: Optional['ForeignKeyReference'] = Field(default=None, description="Foreign key reference details")
    
    # UI positioning for ERD
    position_x: Optional[float] = Field(default=None, description="X position in ERD")
    position_y: Optional[float] = Field(default=None, description="Y position in ERD")
    
    
    @field_validator('type_parameters')
    @classmethod
    def validate_type_parameters(cls, v, info):
        if v is not None:
            # Remove extra quotes if present (fix for frontend sending quoted values)
            if isinstance(v, str) and v.startswith('"') and v.endswith('"'):
                v = v[1:-1]  # Remove surrounding quotes
            
            data_type = info.data.get('data_type')
            
            # VARCHAR and CHAR require length parameter
            if data_type in [DatabricksDataType.VARCHAR, DatabricksDataType.CHAR]:
                if not v or not v.strip():
                    raise ValueError(f"{data_type.value} requires a length parameter (e.g., '50')")
                try:
                    length = int(v.strip())
                    if length <= 0:
                        raise ValueError(f"{data_type.value} length must be positive")
                    if length > 65535:  # Databricks VARCHAR/CHAR max length
                        raise ValueError(f"{data_type.value} length cannot exceed 65535")
                except ValueError as e:
                    if "invalid literal" in str(e):
                        raise ValueError(f"{data_type.value} length must be a valid integer")
                    raise e
            
            # GEOGRAPHY and GEOMETRY require SRID parameter
            elif data_type in [DatabricksDataType.GEOGRAPHY, DatabricksDataType.GEOMETRY]:
                if not v or not v.strip():
                    raise ValueError(f"{data_type.value} requires SRID parameter")
                srid = v.strip()
                
                if data_type == DatabricksDataType.GEOGRAPHY:
                    # GEOGRAPHY only supports SRID 4326
                    if srid.upper() == 'ANY':
                        raise ValueError("GEOGRAPHY(ANY) cannot be persisted in tables")
                    try:
                        srid_int = int(srid)
                        if srid_int != 4326:
                            raise ValueError("GEOGRAPHY only supports SRID 4326")
                    except ValueError as e:
                        if "invalid literal" in str(e):
                            raise ValueError("GEOGRAPHY SRID must be 4326")
                        raise e
                        
                elif data_type == DatabricksDataType.GEOMETRY:
                    # GEOMETRY supports many SRIDs (about 11,000)
                    if srid.upper() == 'ANY':
                        raise ValueError("GEOMETRY(ANY) cannot be persisted in tables")
                    try:
                        srid_int = int(srid)
                        if srid_int < 0:
                            raise ValueError("GEOMETRY SRID must be non-negative (0 for unknown CRS)")
                    except ValueError as e:
                        if "invalid literal" in str(e):
                            raise ValueError("GEOMETRY SRID must be a valid integer")
                        raise e
            
            # ARRAY requires element type
            elif data_type == DatabricksDataType.ARRAY:
                if not v or not v.strip():
                    raise ValueError("ARRAY requires element type (e.g., 'STRING', 'INT')")
                # Validate that element type is a valid Databricks type
                element_type = v.strip().upper()
                # Map common type aliases
                if element_type == 'INT':
                    element_type = 'BIGINT'
                elif element_type == 'BOOL':
                    element_type = 'BOOLEAN'
                
                valid_types = [dt.value for dt in DatabricksDataType if dt != DatabricksDataType.ARRAY]
                if element_type not in valid_types:
                    raise ValueError(f"ARRAY element type '{element_type}' is not a valid Databricks data type")
            
            # MAP requires key and value types
            elif data_type == DatabricksDataType.MAP:
                if not v or not v.strip():
                    raise ValueError("MAP requires key and value types (e.g., 'STRING,INT')")
                parts = v.strip().split(',')
                if len(parts) != 2:
                    raise ValueError("MAP requires exactly two types: 'keyType,valueType'")
                key_type = parts[0].strip().upper()
                value_type = parts[1].strip().upper()
                
                # Map common type aliases
                if key_type == 'INT':
                    key_type = 'BIGINT'
                elif key_type == 'BOOL':
                    key_type = 'BOOLEAN'
                if value_type == 'INT':
                    value_type = 'BIGINT'
                elif value_type == 'BOOL':
                    value_type = 'BOOLEAN'
                
                valid_types = [dt.value for dt in DatabricksDataType if dt != DatabricksDataType.MAP]
                if key_type not in valid_types:
                    raise ValueError(f"MAP key type '{key_type}' is not a valid Databricks data type")
                if value_type not in valid_types:
                    raise ValueError(f"MAP value type '{value_type}' is not a valid Databricks data type")
            
            # STRUCT requires field definitions (simplified validation for now)
            elif data_type == DatabricksDataType.STRUCT:
                if not v or not v.strip():
                    raise ValueError("STRUCT requires field definitions (e.g., 'field1:STRING,field2:INT')")
                # Basic validation - should contain field definitions
                if ':' not in v:
                    raise ValueError("STRUCT fields must be in format 'fieldName:fieldType'")
            
            # INTERVAL requires qualifier
            elif data_type == DatabricksDataType.INTERVAL:
                if not v or not v.strip():
                    raise ValueError("INTERVAL requires qualifier (e.g., 'YEAR TO MONTH', 'DAY TO SECOND')")
                qualifier = v.strip().upper()
                valid_qualifiers = [
                    'YEAR', 'YEAR TO MONTH', 'MONTH',
                    'DAY', 'DAY TO HOUR', 'DAY TO MINUTE', 'DAY TO SECOND',
                    'HOUR', 'HOUR TO MINUTE', 'HOUR TO SECOND',
                    'MINUTE', 'MINUTE TO SECOND', 'SECOND'
                ]
                if qualifier not in valid_qualifiers:
                    raise ValueError(f"INTERVAL qualifier '{qualifier}' is not valid. Must be one of: {', '.join(valid_qualifiers)}")
            
            # DECIMAL requires precision and optionally scale
            elif data_type == DatabricksDataType.DECIMAL:
                # Handle both string format ("10,2") and dict format (from frontend)
                if isinstance(v, str):
                    # Remove extra quotes if present (fix for frontend sending "11,1" instead of 11,1)
                    if v.startswith('"') and v.endswith('"'):
                        v = v[1:-1]  # Remove surrounding quotes
                    
                    # Handle JSON string format from frontend
                    if v.startswith('{') and v.endswith('}'):
                        try:
                            import json
                            dict_params = json.loads(v)
                            precision = dict_params.get('precision', 10)
                            scale = dict_params.get('scale', 0)
                            # Convert to string format
                            v = f"{precision},{scale}" if scale > 0 else str(precision)
                        except (json.JSONDecodeError, KeyError, TypeError):
                            pass  # Fall through to normal string processing
                    
                    if not v or not v.strip():
                        raise ValueError("DECIMAL requires precision parameter (e.g., '10,2' or '10')")
                    # Allow both "10,2" and "10" formats
                    parts = v.strip().split(',')
                    if len(parts) > 2:
                        raise ValueError("DECIMAL parameters should be 'precision' or 'precision,scale'")
                    try:
                        precision = int(parts[0].strip())
                        if precision <= 0 or precision > 38:
                            raise ValueError("DECIMAL precision must be between 1 and 38")
                        if len(parts) == 2:
                            scale = int(parts[1].strip())
                            if scale < 0 or scale > precision:
                                raise ValueError("DECIMAL scale must be between 0 and precision")
                    except ValueError as e:
                        if "invalid literal" in str(e):
                            raise ValueError("DECIMAL parameters must be valid integers")
                        raise e
                elif isinstance(v, dict):
                    # Handle dict format directly
                    precision = v.get('precision', 10)
                    scale = v.get('scale', 0)
                    if not isinstance(precision, int) or not isinstance(scale, int):
                        raise ValueError("DECIMAL precision and scale must be integers")
                    if precision <= 0 or precision > 38:
                        raise ValueError("DECIMAL precision must be between 1 and 38")
                    if scale < 0 or scale > precision:
                        raise ValueError("DECIMAL scale must be between 0 and precision")
                    # Convert to string format for consistency
                    v = f"{precision},{scale}" if scale > 0 else str(precision)
                else:
                    raise ValueError("DECIMAL parameters must be a string or dict")
            
            # GEOGRAPHY and GEOMETRY can have SRID parameter
            elif data_type in [DatabricksDataType.GEOGRAPHY, DatabricksDataType.GEOMETRY]:
                if v and v.strip():
                    srid = v.strip().upper()
                    if srid != "ANY":
                        try:
                            srid_int = int(srid)
                            if srid_int < 0:
                                raise ValueError(f"{data_type.value} SRID must be positive or 'ANY'")
                        except ValueError as e:
                            if "invalid literal" in str(e):
                                raise ValueError(f"{data_type.value} SRID must be an integer or 'ANY'")
                            raise e
        
        return v


class ForeignKeyReference(BaseModel):
    """Represents a foreign key reference"""
    model_config = ConfigDict(extra='allow', validate_assignment=True)
    
    referenced_table_id: str = Field(description="ID of the referenced table")
    referenced_field_id: str = Field(description="ID of the referenced field")
    constraint_name: Optional[str] = Field(default=None, description="Name of the FK constraint")
    on_delete: Optional[str] = Field(default="NO ACTION", description="ON DELETE action")
    on_update: Optional[str] = Field(default="NO ACTION", description="ON UPDATE action")


class DataTable(BaseModel):
    """Represents a table in the data model"""
    model_config = ConfigDict(extra='allow', validate_assignment=True)
    
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str = Field(description="Table name")
    schema_name: Optional[str] = Field(default=None, description="Schema name")
    catalog_name: Optional[str] = Field(default=None, description="Catalog name")
    comment: Optional[str] = Field(default=None, description="Table comment/description")
    logical_name: Optional[str] = Field(default=None, description="Business/logical name for the table")
    tags: Dict[str, str] = Field(default_factory=dict, description="Table tags")
    
    # Fields/columns
    fields: List[TableField] = Field(default_factory=list, description="Table fields")
    
    # Table properties
    table_type: str = Field(default="MANAGED", description="Table type (MANAGED, EXTERNAL, VIEW)")
    storage_location: Optional[str] = Field(default=None, description="Storage location for external tables")
    file_format: Optional[str] = Field(default="DELTA", description="File format (DELTA, PARQUET, etc.)")
    
    # Liquid clustering properties
    cluster_by_auto: bool = Field(default=False, description="Enable automatic liquid clustering")
    
    # UI positioning for ERD
    position_x: float = Field(default=100.0, description="X position in ERD canvas")
    position_y: float = Field(default=100.0, description="Y position in ERD canvas")
    width: float = Field(default=200.0, description="Width of table in ERD")
    height: float = Field(default=150.0, description="Height of table in ERD")
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    
    @field_validator('fields')
    @classmethod
    def validate_primary_keys(cls, v):
        """Ensure only one primary key per table"""
        pk_count = sum(1 for field in v if field.is_primary_key)
        if pk_count > 1:
            raise ValueError("Table can have only one primary key field")
        return v
    
    def get_primary_key_field(self) -> Optional[TableField]:
        """Get the primary key field if exists"""
        for field in self.fields:
            if field.is_primary_key:
                return field
        return None
    
    def get_foreign_key_fields(self) -> List[TableField]:
        """Get all foreign key fields"""
        return [field for field in self.fields if field.is_foreign_key]


class DataModelRelationship(BaseModel):
    """Represents a relationship between two tables"""
    model_config = ConfigDict(extra='allow', validate_assignment=True)
    
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    source_table_id: str = Field(description="ID of the source table (PK side)")
    target_table_id: str = Field(description="ID of the target table (FK side)")
    source_field_id: Optional[str] = Field(default=None, description="ID of the source field (PK)")
    target_field_id: Optional[str] = Field(default=None, description="ID of the target field (FK)")
    relationship_type: str = Field(default="one_to_many", description="Type of relationship")
    constraint_name: Optional[str] = Field(default=None, description="Name of the FK constraint")
    
    # Explicit FK tracking for proper deletion (optional for backward compatibility)
    fk_table_id: Optional[str] = Field(default=None, description="ID of table that contains the FK field")
    fk_field_id: Optional[str] = Field(default=None, description="ID of the FK field")
    
    # UI properties for ERD lines
    line_points: List[Dict[str, float]] = Field(default_factory=list, description="Line points for drawing relationship")


# Metric View Models
class MetricViewDimension(BaseModel):
    """Represents a dimension in a metric view"""
    model_config = ConfigDict(extra='allow', validate_assignment=True)
    
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str = Field(description="Dimension name")
    expr: str = Field(description="SQL expression for the dimension")
    description: Optional[str] = Field(default=None, description="Dimension description")
    data_type: Optional[DatabricksDataType] = Field(default=None, description="Expected data type")

class MetricViewMeasure(BaseModel):
    """Represents a measure in a metric view"""
    model_config = ConfigDict(extra='allow', validate_assignment=True)
    
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str = Field(description="Measure name")
    expr: str = Field(description="Aggregate SQL expression")
    description: Optional[str] = Field(default=None, description="Measure description")
    aggregation_type: str = Field(default="SUM", description="Type of aggregation (SUM, COUNT, AVG, etc.)")
    is_window_measure: bool = Field(default=False, description="Whether this is a window measure")
    data_type: Optional[DatabricksDataType] = Field(default=None, description="Expected data type")
    
    # Window measure specific properties
    window_type: Optional[str] = Field(default=None, description="Type of window measure (moving_average, period_over_period, running_total, etc.)")
    window_size: Optional[int] = Field(default=None, description="Window size for moving averages (e.g., 7 for 7-day moving average)")
    window_unit: Optional[str] = Field(default=None, description="Time unit for window (day, week, month, quarter, year)")
    partition_by: Optional[List[str]] = Field(default_factory=list, description="Columns to partition by for window functions")
    order_by: Optional[str] = Field(default=None, description="Column to order by for window functions (usually a date/time column)")
    offset_periods: Optional[int] = Field(default=None, description="Number of periods to offset for period-over-period calculations")
    window_frame: Optional[str] = Field(default=None, description="Custom window frame specification (e.g., 'ROWS BETWEEN 6 PRECEDING AND CURRENT ROW')")
    
    def generate_window_expression(self, base_measure_expr: str) -> str:
        """Generate the SQL expression for window measures"""
        if not self.is_window_measure or not self.window_type:
            return base_measure_expr
        
        partition_clause = ""
        if self.partition_by:
            partition_clause = f"PARTITION BY {', '.join(self.partition_by)}"
        
        order_clause = ""
        if self.order_by:
            order_clause = f"ORDER BY {self.order_by}"
        
        window_clause = f"OVER ({partition_clause} {order_clause}".strip() + ")"
        
        if self.window_type == "moving_average":
            if self.window_size:
                frame = f"ROWS BETWEEN {self.window_size - 1} PRECEDING AND CURRENT ROW"
                window_clause = f"OVER ({partition_clause} {order_clause} {frame}".strip() + ")"
            return f"AVG({base_measure_expr}) {window_clause}"
        
        elif self.window_type == "running_total":
            frame = "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
            window_clause = f"OVER ({partition_clause} {order_clause} {frame}".strip() + ")"
            return f"SUM({base_measure_expr}) {window_clause}"
        
        elif self.window_type == "period_over_period":
            if self.offset_periods:
                return f"({base_measure_expr} - LAG({base_measure_expr}, {self.offset_periods}) {window_clause}) / LAG({base_measure_expr}, {self.offset_periods}) {window_clause} * 100"
            else:
                return f"({base_measure_expr} - LAG({base_measure_expr}, 1) {window_clause}) / LAG({base_measure_expr}, 1) {window_clause} * 100"
        
        elif self.window_type == "rank":
            return f"RANK() {window_clause}"
        
        elif self.window_type == "row_number":
            return f"ROW_NUMBER() {window_clause}"
        
        elif self.window_type == "percent_of_total":
            return f"{base_measure_expr} / SUM({base_measure_expr}) {window_clause} * 100"
        
        elif self.window_type == "cumulative_sum":
            frame = "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
            window_clause = f"OVER ({partition_clause} {order_clause} {frame}".strip() + ")"
            return f"SUM({base_measure_expr}) {window_clause}"
        
        elif self.window_type == "custom" and self.window_frame:
            window_clause = f"OVER ({partition_clause} {order_clause} {self.window_frame}".strip() + ")"
            return f"{base_measure_expr} {window_clause}"
        
        # Default fallback
        return f"{base_measure_expr} {window_clause}"

class MetricViewJoin(BaseModel):
    """Represents a join in a metric view"""
    model_config = ConfigDict(extra='allow', validate_assignment=True)
    
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str = Field(description="Join name/alias")
    sql_on: str = Field(description="JOIN condition SQL")
    join_type: str = Field(default="LEFT", description="Type of join (LEFT, INNER, RIGHT, FULL)")
    joined_table_id: Optional[str] = Field(default=None, description="ID of the joined table if available")
    
    # Advanced join properties
    joined_table_name: Optional[str] = Field(default=None, description="Full name of joined table (catalog.schema.table)")
    join_alias: Optional[str] = Field(default=None, description="Alias for the joined table")
    description: Optional[str] = Field(default=None, description="Description of the join purpose")
    
    # Join condition components (for UI building)
    left_columns: Optional[List[str]] = Field(default_factory=list, description="Columns from left table in join condition")
    right_columns: Optional[List[str]] = Field(default_factory=list, description="Columns from right table in join condition")
    join_operators: Optional[List[str]] = Field(default_factory=list, description="Operators for join conditions (=, <, >, etc.)")
    additional_conditions: Optional[str] = Field(default=None, description="Additional WHERE-like conditions for the join")
    
    # Databricks metric view specific fields
    using: Optional[List[str]] = Field(default=None, description="USING clause columns for equi-joins")
    joins: Optional[List['MetricViewJoin']] = Field(default=None, description="Nested joins for snowflake schema")
    
    # Performance and optimization hints
    broadcast_hint: bool = Field(default=False, description="Whether to use broadcast hint for small tables")
    bucket_hint: Optional[str] = Field(default=None, description="Bucket join hint if tables are bucketed")
    sort_merge_hint: bool = Field(default=False, description="Whether to use sort-merge join hint")
    
    def generate_join_sql(self, base_table_alias: str = "base") -> str:
        """Generate the complete JOIN SQL statement"""
        if not self.joined_table_name:
            return ""
        
        # Build join type
        join_clause = f"{self.join_type} JOIN"
        
        # Add hints if specified
        hints = []
        if self.broadcast_hint:
            hints.append("BROADCAST")
        if self.bucket_hint:
            hints.append(f"BUCKET({self.bucket_hint})")
        if self.sort_merge_hint:
            hints.append("MERGE")
        
        if hints:
            join_clause += f" /*+ {', '.join(hints)} */"
        
        # Add table name and alias
        table_part = self.joined_table_name
        if self.join_alias:
            table_part += f" AS {self.join_alias}"
        
        # Build ON condition
        on_condition = self.sql_on
        if not on_condition and self.left_columns and self.right_columns:
            # Auto-generate ON condition from column mappings
            conditions = []
            for i, (left_col, right_col) in enumerate(zip(self.left_columns, self.right_columns)):
                operator = self.join_operators[i] if i < len(self.join_operators) else "="
                left_alias = base_table_alias
                right_alias = self.join_alias or self.joined_table_name.split('.')[-1]
                conditions.append(f"{left_alias}.{left_col} {operator} {right_alias}.{right_col}")
            
            on_condition = " AND ".join(conditions)
            
            # Add additional conditions if specified
            if self.additional_conditions:
                on_condition += f" AND {self.additional_conditions}"
        
        return f"{join_clause} {table_part} ON {on_condition}"
    
    def validate_join_condition(self) -> List[str]:
        """Validate the join configuration and return any errors"""
        errors = []
        
        if not self.joined_table_name and not self.sql_on:
            errors.append("Either joined_table_name or sql_on must be specified")
        
        if self.left_columns and self.right_columns:
            if len(self.left_columns) != len(self.right_columns):
                errors.append("Number of left and right columns must match")
        
        if not self.sql_on and not (self.left_columns and self.right_columns):
            errors.append("Either sql_on or column mappings (left_columns + right_columns) must be provided")
        
        return errors

class MetricView(BaseModel):
    """Represents a Databricks Metric View"""
    model_config = ConfigDict(extra='allow', validate_assignment=True)
    
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str = Field(description="Metric view name")
    description: Optional[str] = Field(default=None, description="Metric view description")
    
    # Databricks location (individual catalog/schema support)
    catalog_name: Optional[str] = Field(default=None, description="Catalog name")
    schema_name: Optional[str] = Field(default=None, description="Schema name")
    
    # Databricks Metric View YAML structure
    version: str = Field(default="0.1", description="Metric view specification version")
    source_table_id: str = Field(description="ID of the source table")
    source_sql: Optional[str] = Field(default=None, description="Custom SQL source (alternative to table)")
    filter: Optional[str] = Field(default=None, description="Global filter (WHERE clause)")
    
    # Metric components
    dimensions: List[MetricViewDimension] = Field(default_factory=list, description="Dimensions")
    measures: List[MetricViewMeasure] = Field(default_factory=list, description="Measures")
    joins: List[MetricViewJoin] = Field(default_factory=list, description="Joins")
    
    # Metadata and tags
    tags: Dict[str, str] = Field(default_factory=dict, description="Metric view tags")
    
    # UI positioning for ERD
    position_x: float = Field(default=100.0, description="X position in ERD canvas")
    position_y: float = Field(default=100.0, description="Y position in ERD canvas")
    width: float = Field(default=280.0, description="Width of metric view in ERD")
    height: float = Field(default=220.0, description="Height of metric view in ERD")
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    
    def get_dimension_by_id(self, dimension_id: str) -> Optional[MetricViewDimension]:
        """Get dimension by ID"""
        return next((dim for dim in self.dimensions if dim.id == dimension_id), None)
    
    def get_measure_by_id(self, measure_id: str) -> Optional[MetricViewMeasure]:
        """Get measure by ID"""
        return next((measure for measure in self.measures if measure.id == measure_id), None)


class TraditionalView(BaseModel):
    """Represents a traditional SQL view (CREATE VIEW)"""
    model_config = ConfigDict(extra='allow', validate_assignment=True)
    
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str = Field(description="View name")
    description: Optional[str] = Field(default=None, description="View description")
    
    # SQL query definition
    sql_query: str = Field(description="Complete SQL query for the view")
    
    # Metadata and tags
    tags: Dict[str, str] = Field(default_factory=dict, description="View tags")
    logical_name: Optional[str] = Field(default=None, description="Business/logical name for the view")
    
    # Databricks properties
    catalog_name: Optional[str] = Field(default=None, description="Catalog name")
    schema_name: Optional[str] = Field(default=None, description="Schema name")
    
    # Dependencies (auto-detected during import)
    referenced_table_ids: List[str] = Field(default_factory=list, description="IDs of tables referenced in the SQL query")
    referenced_table_names: List[str] = Field(default_factory=list, description="Names of tables referenced in the SQL query")
    
    # UI positioning for ERD
    position_x: float = Field(default=100.0, description="X position in ERD canvas")
    position_y: float = Field(default=100.0, description="Y position in ERD canvas")
    width: float = Field(default=250.0, description="Width of view in ERD")
    height: float = Field(default=180.0, description="Height of view in ERD")
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)


class MetricSourceRelationship(BaseModel):
    """Represents the relationship between a table and a metric view"""
    model_config = ConfigDict(extra='allow', validate_assignment=True)
    
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    source_table_id: str = Field(description="ID of the source table")
    metric_view_id: str = Field(description="ID of the metric view")
    relationship_type: str = Field(default="source_to_metric", description="Type of relationship")
    
    # Field mappings from table fields to metric dimensions/measures
    field_mappings: List[Dict[str, Any]] = Field(default_factory=list, description="Field to dimension/measure mappings")
    
    # Visual properties for relationship line
    line_points: List[Dict[str, float]] = Field(default_factory=list, description="Line points for drawing relationship")
    
    created_at: datetime = Field(default_factory=datetime.now)


class DataModelProject(BaseModel):
    """Represents a complete data modeling project"""
    model_config = ConfigDict(extra='allow', validate_assignment=True)
    
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str = Field(description="Project name")
    description: Optional[str] = Field(default=None, description="Project description")
    
    # Databricks connection info (default for objects that don't specify their own)
    catalog_name: str = Field(description="Default Databricks catalog for objects without explicit catalog")
    schema_name: str = Field(description="Default Databricks schema for objects without explicit schema")
    
    # Model data
    tables: List[DataTable] = Field(default_factory=list, description="Tables in the model")
    metric_views: List[MetricView] = Field(default_factory=list, description="Metric views in the model")
    traditional_views: List[TraditionalView] = Field(default_factory=list, description="Traditional SQL views in the model")
    relationships: List[DataModelRelationship] = Field(default_factory=list, description="Relationships between tables")
    metric_relationships: List[MetricSourceRelationship] = Field(default_factory=list, description="Relationships between tables and metric views")
    
    # Metadata
    version: str = Field(default="1.0", description="Project version")
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    created_by: Optional[str] = Field(default=None, description="User who created the project")
    canvas_settings: dict = Field(default_factory=dict, description="Canvas UI settings")
    
    def get_effective_catalog(self, obj_catalog: Optional[str] = None) -> str:
        """Get the effective catalog name for an object (object's catalog or project default)"""
        return obj_catalog if obj_catalog else self.catalog_name
    
    def get_effective_schema(self, obj_schema: Optional[str] = None) -> str:
        """Get the effective schema name for an object (object's schema or project default)"""
        return obj_schema if obj_schema else self.schema_name
    
    def get_object_full_name(self, obj_name: str, obj_catalog: Optional[str] = None, obj_schema: Optional[str] = None) -> str:
        """Get the fully qualified name for an object"""
        effective_catalog = self.get_effective_catalog(obj_catalog)
        effective_schema = self.get_effective_schema(obj_schema)
        return f"{effective_catalog}.{effective_schema}.{obj_name}"
    
    def get_table_by_id(self, table_id: str) -> Optional[DataTable]:
        """Get table by ID"""
        for table in self.tables:
            if table.id == table_id:
                return table
        return None
    
    def get_table_by_name(self, table_name: str) -> Optional[DataTable]:
        """Get table by name"""
        for table in self.tables:
            if table.name == table_name:
                return table
        return None
    
    def get_metric_view_by_id(self, metric_view_id: str) -> Optional[MetricView]:
        """Get metric view by ID"""
        for metric_view in self.metric_views:
            if metric_view.id == metric_view_id:
                return metric_view
        return None
    
    def get_metric_view_by_name(self, metric_view_name: str) -> Optional[MetricView]:
        """Get metric view by name"""
        for metric_view in self.metric_views:
            if metric_view.name == metric_view_name:
                return metric_view
        return None
    
    def validate_relationships(self) -> List[str]:
        """Validate all relationships in the project"""
        errors = []
        
        for relationship in self.relationships:
            source_table = self.get_table_by_id(relationship.source_table_id)
            target_table = self.get_table_by_id(relationship.target_table_id)
            
            if not source_table:
                errors.append(f"Source table not found for relationship {relationship.id}")
                continue
                
            if not target_table:
                errors.append(f"Target table not found for relationship {relationship.id}")
                continue
                
            # Find source and target fields
            source_field = None
            target_field = None
            
            for field in source_table.fields:
                if field.id == relationship.source_field_id:
                    source_field = field
                    break
                    
            for field in target_table.fields:
                if field.id == relationship.target_field_id:
                    target_field = field
                    break
            
            if not source_field:
                errors.append(f"Source field not found for relationship {relationship.id}")
                
            if not target_field:
                errors.append(f"Target field not found for relationship {relationship.id}")
                
            if target_field and not target_field.is_primary_key:
                errors.append(f"Target field must be primary key for relationship {relationship.id}")
        
        return errors


class ExistingTableImport(BaseModel):
    """Model for importing existing tables from Databricks"""
    model_config = ConfigDict(extra='allow', validate_assignment=True)
    
    catalog_name: str = Field(description="Catalog name")
    schema_name: str = Field(description="Schema name")
    table_names: List[str] = Field(description="List of table names to import")
    include_constraints: bool = Field(default=True, description="Whether to import existing constraints")
    position_strategy: str = Field(default="auto_layout", description="How to position imported tables")


# Update the ForeignKeyReference forward reference
TableField.model_rebuild()
