import yaml
from typing import Dict, Any
from datetime import datetime
from .data_modeling import (
    DataModelProject, DataTable, TableField, DataModelRelationship, ForeignKeyReference,
    MetricView, MetricViewDimension, MetricViewMeasure, MetricViewJoin, MetricSourceRelationship,
    TraditionalView
)


class DataModelYAMLSerializer:
    """Handles YAML serialization and deserialization of data modeling projects"""
    
    @staticmethod
    def to_yaml(project: DataModelProject) -> str:
        """Convert DataModelProject to YAML string"""
        project_dict = DataModelYAMLSerializer._project_to_dict(project)
        
        # Custom representer to handle enums as strings
        def represent_enum(dumper, data):
            return dumper.represent_scalar('tag:yaml.org,2002:str', str(data.value) if hasattr(data, 'value') else str(data))
        
        # Find a DatabricksDataType enum to register the representer
        # Look through all tables and fields to find at least one data type
        data_type_found = None
        for table in project.tables:
            for field in table.fields:
                if hasattr(field.data_type, 'value'):
                    data_type_found = type(field.data_type)
                    break
            if data_type_found:
                break
        
        # Add custom representer for DatabricksDataType if we found one
        if data_type_found:
            yaml.add_representer(data_type_found, represent_enum)
        
        try:
            return yaml.safe_dump(project_dict, default_flow_style=False, sort_keys=False, allow_unicode=True)
        except Exception as e:
            # Fallback: convert all enums to strings manually
            import json
            json_str = json.dumps(project_dict, default=str)
            return yaml.safe_dump(json.loads(json_str), default_flow_style=False, sort_keys=False, allow_unicode=True)
    
    @staticmethod
    def from_yaml(yaml_content: str) -> DataModelProject:
        """Create DataModelProject from YAML string"""
        project_dict = yaml.safe_load(yaml_content)
        return DataModelYAMLSerializer._dict_to_project(project_dict)
    
    @staticmethod
    def _project_to_dict(project: DataModelProject) -> Dict[str, Any]:
        """Convert project to dictionary for YAML serialization"""
        return {
            'metadata': {
                'name': project.name,
                'description': project.description,
                'version': project.version,
                'created_at': project.created_at.isoformat() if project.created_at else None,
                'updated_at': project.updated_at.isoformat() if project.updated_at else None,
                'created_by': project.created_by
            },
            'databricks': {
                'catalog_name': project.catalog_name,
                'schema_name': project.schema_name
            },
            'tables': [
                DataModelYAMLSerializer._table_to_dict(table) for table in project.tables
            ],
            'relationships': [
                DataModelYAMLSerializer._relationship_to_dict(rel) for rel in project.relationships
            ],
            'metric_views': [
                DataModelYAMLSerializer._metric_view_to_dict(mv) for mv in project.metric_views
            ],
            'traditional_views': [
                DataModelYAMLSerializer._traditional_view_to_dict(tv) for tv in project.traditional_views
            ],
            'metric_relationships': [
                DataModelYAMLSerializer._metric_relationship_to_dict(mr) for mr in project.metric_relationships
            ],
            'canvas_settings': project.canvas_settings
        }
    
    @staticmethod
    def _table_to_dict(table: DataTable) -> Dict[str, Any]:
        """Convert table to dictionary"""
        return {
            'id': table.id,
            'name': table.name,
            'logical_name': table.logical_name,
            'comment': table.comment,
            'catalog_name': table.catalog_name,
            'schema_name': table.schema_name,
            'table_type': table.table_type,
            'storage_location': table.storage_location,
            'file_format': table.file_format,
            'tags': table.tags,
            'position': {
                'x': table.position_x,
                'y': table.position_y,
                'width': table.width,
                'height': table.height
            },
            'fields': [
                DataModelYAMLSerializer._field_to_dict(field) for field in table.fields
            ]
        }
    
    @staticmethod
    def _field_to_dict(field: TableField) -> Dict[str, Any]:
        """Convert field to dictionary"""
        field_dict = {
            'id': field.id,
            'name': field.name,
            'data_type': str(field.data_type.value) if hasattr(field.data_type, 'value') else str(field.data_type),
            'type_parameters': field.type_parameters,
            'nullable': field.nullable,
            'default_value': field.default_value,
            'comment': field.comment,
            'logical_name': field.logical_name,
            'tags': field.tags,
            'is_primary_key': field.is_primary_key,
            'is_foreign_key': field.is_foreign_key
        }
        
        if field.foreign_key_reference:
            field_dict['foreign_key_reference'] = {
                'referenced_table_id': field.foreign_key_reference.referenced_table_id,
                'referenced_field_id': field.foreign_key_reference.referenced_field_id,
                'constraint_name': field.foreign_key_reference.constraint_name,
                'on_delete': field.foreign_key_reference.on_delete,
                'on_update': field.foreign_key_reference.on_update
            }
        
        return field_dict
    
    @staticmethod
    def _relationship_to_dict(relationship: DataModelRelationship) -> Dict[str, Any]:
        """Convert relationship to dictionary"""
        return {
            'id': relationship.id,
            'source_table_id': relationship.source_table_id,
            'target_table_id': relationship.target_table_id,
            'source_field_id': relationship.source_field_id,
            'target_field_id': relationship.target_field_id,
            'relationship_type': relationship.relationship_type,
            'constraint_name': relationship.constraint_name,
            'line_points': relationship.line_points
        }
    
    @staticmethod
    def _metric_view_to_dict(metric_view: MetricView) -> Dict[str, Any]:
        """Convert metric view to dictionary"""
        return {
            'id': metric_view.id,
            'name': metric_view.name,
            'description': metric_view.description,
            'catalog_name': metric_view.catalog_name,
            'schema_name': metric_view.schema_name,
            'version': metric_view.version,
            'source_table_id': metric_view.source_table_id,
            'source_sql': metric_view.source_sql,
            'filter': metric_view.filter,
            'dimensions': [
                DataModelYAMLSerializer._dimension_to_dict(dim) for dim in metric_view.dimensions
            ],
            'measures': [
                DataModelYAMLSerializer._measure_to_dict(measure) for measure in metric_view.measures
            ],
            'joins': [
                DataModelYAMLSerializer._join_to_dict(join) for join in metric_view.joins
            ],
            'tags': metric_view.tags,
            'position': {
                'x': metric_view.position_x,
                'y': metric_view.position_y
            },
            'size': {
                'width': metric_view.width,
                'height': metric_view.height
            },
            'created_at': metric_view.created_at.isoformat() if metric_view.created_at else None,
            'updated_at': metric_view.updated_at.isoformat() if metric_view.updated_at else None
        }
    
    @staticmethod
    def _dimension_to_dict(dimension: MetricViewDimension) -> Dict[str, Any]:
        """Convert dimension to dictionary"""
        return {
            'id': dimension.id,
            'name': dimension.name,
            'expression': dimension.expr,
            'description': dimension.description,
            'data_type': dimension.data_type
        }
    
    @staticmethod
    def _measure_to_dict(measure: MetricViewMeasure) -> Dict[str, Any]:
        """Convert measure to dictionary"""
        measure_dict = {
            'id': measure.id,
            'name': measure.name,
            'expression': measure.expr,
            'description': measure.description,
            'aggregation': measure.aggregation_type,
            'is_window_measure': measure.is_window_measure,
            'data_type': measure.data_type
        }
        
        # Add window measure properties if applicable
        if measure.is_window_measure:
            measure_dict.update({
                'window_type': measure.window_type,
                'window_size': measure.window_size,
                'window_unit': measure.window_unit,
                'partition_by': measure.partition_by,
                'order_by': measure.order_by,
                'offset_periods': measure.offset_periods,
                'window_frame': measure.window_frame
            })
        
        return measure_dict
    
    @staticmethod
    def _join_to_dict(join: MetricViewJoin) -> Dict[str, Any]:
        """Convert join to dictionary"""
        return {
            'id': join.id,
            'name': join.name,
            'sql_on': join.sql_on,
            'joined_table_id': join.joined_table_id,
            'joined_table_name': join.joined_table_name,
            'join_type': join.join_type,
            'join_alias': join.join_alias,
            'description': join.description,
            'left_columns': join.left_columns,
            'right_columns': join.right_columns,
            'join_operators': join.join_operators,
            'additional_conditions': join.additional_conditions,
            'broadcast_hint': join.broadcast_hint,
            'bucket_hint': join.bucket_hint,
            'sort_merge_hint': join.sort_merge_hint
        }
    
    @staticmethod
    def _traditional_view_to_dict(traditional_view: TraditionalView) -> Dict[str, Any]:
        """Convert traditional view to dictionary"""
        return {
            'id': traditional_view.id,
            'name': traditional_view.name,
            'description': traditional_view.description,
            'sql_query': traditional_view.sql_query,
            'tags': traditional_view.tags,
            'logical_name': traditional_view.logical_name,
            'catalog_name': traditional_view.catalog_name,
            'schema_name': traditional_view.schema_name,
            'referenced_table_ids': traditional_view.referenced_table_ids,
            'referenced_table_names': traditional_view.referenced_table_names,
            'position': {
                'x': traditional_view.position_x,
                'y': traditional_view.position_y
            },
            'size': {
                'width': traditional_view.width,
                'height': traditional_view.height
            },
            'created_at': traditional_view.created_at.isoformat() if traditional_view.created_at else None,
            'updated_at': traditional_view.updated_at.isoformat() if traditional_view.updated_at else None
        }

    @staticmethod
    def _metric_relationship_to_dict(metric_rel: MetricSourceRelationship) -> Dict[str, Any]:
        """Convert metric relationship to dictionary"""
        return {
            'id': metric_rel.id,
            'source_table_id': metric_rel.source_table_id,
            'metric_view_id': metric_rel.metric_view_id,
            'field_mappings': [
                {
                    'source_field': mapping.source_field,
                    'target_dimension': mapping.target_dimension,
                    'target_measure': mapping.target_measure,
                    'mapping_type': mapping.mapping_type
                }
                for mapping in metric_rel.field_mappings
            ]
        }
    
    @staticmethod
    def _dict_to_project(project_dict: Dict[str, Any]) -> DataModelProject:
        """Convert dictionary to DataModelProject"""
        metadata = project_dict.get('metadata', {})
        databricks = project_dict.get('databricks', {})
        
        # Parse dates
        created_at = None
        updated_at = None
        
        if metadata.get('created_at'):
            created_at = datetime.fromisoformat(metadata['created_at'])
        if metadata.get('updated_at'):
            updated_at = datetime.fromisoformat(metadata['updated_at'])
        
        # Parse tables
        tables = []
        for table_dict in project_dict.get('tables', []):
            tables.append(DataModelYAMLSerializer._dict_to_table(table_dict))
        
        # Parse relationships
        relationships = []
        for rel_dict in project_dict.get('relationships', []):
            relationships.append(DataModelYAMLSerializer._dict_to_relationship(rel_dict))
        
        # Parse metric views
        metric_views = []
        for mv_dict in project_dict.get('metric_views', []):
            metric_views.append(DataModelYAMLSerializer._dict_to_metric_view(mv_dict))
        
        # Parse traditional views
        traditional_views = []
        for tv_dict in project_dict.get('traditional_views', []):
            traditional_views.append(DataModelYAMLSerializer._dict_to_traditional_view(tv_dict))
        
        # Parse metric relationships
        metric_relationships = []
        for mr_dict in project_dict.get('metric_relationships', []):
            metric_relationships.append(DataModelYAMLSerializer._dict_to_metric_relationship(mr_dict))
        
        return DataModelProject(
            name=metadata.get('name', 'Untitled Project'),
            description=metadata.get('description'),
            version=metadata.get('version', '1.0'),
            created_at=created_at or datetime.now(),
            updated_at=updated_at or datetime.now(),
            created_by=metadata.get('created_by'),
            catalog_name=databricks.get('catalog_name', ''),
            schema_name=databricks.get('schema_name', ''),
            tables=tables,
            relationships=relationships,
            metric_views=metric_views,
            traditional_views=traditional_views,
            metric_relationships=metric_relationships,
            canvas_settings=project_dict.get('canvas_settings', {})
        )
    
    @staticmethod
    def _dict_to_table(table_dict: Dict[str, Any]) -> DataTable:
        """Convert dictionary to DataTable"""
        position = table_dict.get('position', {})
        
        # Parse fields
        fields = []
        for field_dict in table_dict.get('fields', []):
            fields.append(DataModelYAMLSerializer._dict_to_field(field_dict))
        
        return DataTable(
            id=table_dict.get('id'),
            name=table_dict.get('name', ''),
            logical_name=table_dict.get('logical_name'),
            comment=table_dict.get('comment'),
            catalog_name=table_dict.get('catalog_name'),
            schema_name=table_dict.get('schema_name'),
            table_type=table_dict.get('table_type', 'MANAGED'),
            storage_location=table_dict.get('storage_location'),
            file_format=table_dict.get('file_format', 'DELTA'),
            tags=table_dict.get('tags', {}),
            position_x=position.get('x', 100.0),
            position_y=position.get('y', 100.0),
            width=position.get('width', 200.0),
            height=position.get('height', 150.0),
            fields=fields
        )
    
    @staticmethod
    def _dict_to_field(field_dict: Dict[str, Any]) -> TableField:
        """Convert dictionary to TableField"""
        # Parse foreign key reference if present
        fk_ref = None
        if field_dict.get('foreign_key_reference'):
            fk_data = field_dict['foreign_key_reference']
            fk_ref = ForeignKeyReference(
                referenced_table_id=fk_data.get('referenced_table_id', ''),
                referenced_field_id=fk_data.get('referenced_field_id', ''),
                constraint_name=fk_data.get('constraint_name'),
                on_delete=fk_data.get('on_delete', 'NO ACTION'),
                on_update=fk_data.get('on_update', 'NO ACTION')
            )
        
        return TableField(
            id=field_dict.get('id'),
            name=field_dict.get('name', ''),
            data_type=field_dict.get('data_type', 'STRING'),
            type_parameters=field_dict.get('type_parameters'),
            nullable=field_dict.get('nullable', True),
            default_value=field_dict.get('default_value'),
            comment=field_dict.get('comment'),
            logical_name=field_dict.get('logical_name'),
            tags=field_dict.get('tags', {}),
            is_primary_key=field_dict.get('is_primary_key', False),
            is_foreign_key=field_dict.get('is_foreign_key', False),
            foreign_key_reference=fk_ref
        )
    
    @staticmethod
    def _dict_to_relationship(rel_dict: Dict[str, Any]) -> DataModelRelationship:
        """Convert dictionary to DataModelRelationship"""
        return DataModelRelationship(
            id=rel_dict.get('id'),
            source_table_id=rel_dict.get('source_table_id', ''),
            target_table_id=rel_dict.get('target_table_id', ''),
            source_field_id=rel_dict.get('source_field_id', ''),
            target_field_id=rel_dict.get('target_field_id', ''),
            relationship_type=rel_dict.get('relationship_type', 'one_to_many'),
            constraint_name=rel_dict.get('constraint_name'),
            line_points=rel_dict.get('line_points', [])
        )
    
    @staticmethod
    def _dict_to_metric_view(mv_dict: Dict[str, Any]) -> MetricView:
        """Convert dictionary to MetricView"""
        position = mv_dict.get('position', {})
        size = mv_dict.get('size', {})
        
        # Parse dates
        created_at = None
        updated_at = None
        if mv_dict.get('created_at'):
            created_at = datetime.fromisoformat(mv_dict['created_at'])
        if mv_dict.get('updated_at'):
            updated_at = datetime.fromisoformat(mv_dict['updated_at'])
        
        # Parse dimensions
        dimensions = []
        for dim_dict in mv_dict.get('dimensions', []):
            dimensions.append(DataModelYAMLSerializer._dict_to_dimension(dim_dict))
        
        # Parse measures
        measures = []
        for measure_dict in mv_dict.get('measures', []):
            measures.append(DataModelYAMLSerializer._dict_to_measure(measure_dict))
        
        # Parse joins
        joins = []
        for join_dict in mv_dict.get('joins', []):
            joins.append(DataModelYAMLSerializer._dict_to_join(join_dict))
        
        return MetricView(
            id=mv_dict.get('id'),
            name=mv_dict.get('name', ''),
            catalog_name=mv_dict.get('catalog_name'),
            schema_name=mv_dict.get('schema_name'),
            description=mv_dict.get('description'),
            version=mv_dict.get('version', '0.1'),
            source_table_id=mv_dict.get('source_table_id', ''),
            source_sql=mv_dict.get('source_sql'),
            filter=mv_dict.get('filter'),
            dimensions=dimensions,
            measures=measures,
            joins=joins,
            tags=mv_dict.get('tags', {}),
            position_x=position.get('x', 100.0),
            position_y=position.get('y', 100.0),
            width=size.get('width', 280.0),
            height=size.get('height', 220.0),
            created_at=created_at or datetime.now(),
            updated_at=updated_at or datetime.now()
        )
    
    @staticmethod
    def _dict_to_dimension(dim_dict: Dict[str, Any]) -> MetricViewDimension:
        """Convert dictionary to MetricViewDimension"""
        return MetricViewDimension(
            id=dim_dict.get('id'),
            name=dim_dict.get('name', ''),
            expr=dim_dict.get('expression', ''),
            description=dim_dict.get('description'),
            data_type=dim_dict.get('data_type')
        )
    
    @staticmethod
    def _dict_to_measure(measure_dict: Dict[str, Any]) -> MetricViewMeasure:
        """Convert dictionary to MetricViewMeasure"""
        return MetricViewMeasure(
            id=measure_dict.get('id'),
            name=measure_dict.get('name', ''),
            expr=measure_dict.get('expression', ''),
            description=measure_dict.get('description'),
            aggregation_type=measure_dict.get('aggregation', 'SUM'),
            is_window_measure=measure_dict.get('is_window_measure', False),
            data_type=measure_dict.get('data_type'),
            window_type=measure_dict.get('window_type'),
            window_size=measure_dict.get('window_size'),
            window_unit=measure_dict.get('window_unit'),
            partition_by=measure_dict.get('partition_by', []),
            order_by=measure_dict.get('order_by'),
            offset_periods=measure_dict.get('offset_periods'),
            window_frame=measure_dict.get('window_frame')
        )
    
    @staticmethod
    def _dict_to_join(join_dict: Dict[str, Any]) -> MetricViewJoin:
        """Convert dictionary to MetricViewJoin"""
        return MetricViewJoin(
            id=join_dict.get('id'),
            name=join_dict.get('name', ''),
            sql_on=join_dict.get('sql_on', ''),
            joined_table_id=join_dict.get('joined_table_id'),
            joined_table_name=join_dict.get('joined_table_name'),
            join_type=join_dict.get('join_type', 'LEFT'),
            join_alias=join_dict.get('join_alias'),
            description=join_dict.get('description'),
            left_columns=join_dict.get('left_columns', []),
            right_columns=join_dict.get('right_columns', []),
            join_operators=join_dict.get('join_operators', []),
            additional_conditions=join_dict.get('additional_conditions'),
            broadcast_hint=join_dict.get('broadcast_hint', False),
            bucket_hint=join_dict.get('bucket_hint'),
            sort_merge_hint=join_dict.get('sort_merge_hint', False)
        )
    
    @staticmethod
    def _dict_to_metric_relationship(mr_dict: Dict[str, Any]) -> MetricSourceRelationship:
        """Convert dictionary to MetricSourceRelationship"""
        field_mappings = []
        for mapping_dict in mr_dict.get('field_mappings', []):
            field_mappings.append({
                'source_field': mapping_dict.get('source_field', ''),
                'target_dimension': mapping_dict.get('target_dimension'),
                'target_measure': mapping_dict.get('target_measure'),
                'mapping_type': mapping_dict.get('mapping_type', 'dimension')
            })
        
        return MetricSourceRelationship(
            id=mr_dict.get('id'),
            source_table_id=mr_dict.get('source_table_id', ''),
            metric_view_id=mr_dict.get('metric_view_id', ''),
            field_mappings=field_mappings
        )
    
    @staticmethod
    def _dict_to_traditional_view(tv_dict: Dict[str, Any]) -> TraditionalView:
        """Convert dictionary to TraditionalView"""
        position = tv_dict.get('position', {})
        size = tv_dict.get('size', {})
        
        # Parse dates
        created_at = None
        updated_at = None
        if tv_dict.get('created_at'):
            created_at = datetime.fromisoformat(tv_dict['created_at'])
        if tv_dict.get('updated_at'):
            updated_at = datetime.fromisoformat(tv_dict['updated_at'])
        
        return TraditionalView(
            id=tv_dict.get('id'),
            name=tv_dict.get('name', ''),
            description=tv_dict.get('description'),
            sql_query=tv_dict.get('sql_query', ''),
            tags=tv_dict.get('tags', {}),
            logical_name=tv_dict.get('logical_name'),
            catalog_name=tv_dict.get('catalog_name'),
            schema_name=tv_dict.get('schema_name'),
            referenced_table_ids=tv_dict.get('referenced_table_ids', []),
            referenced_table_names=tv_dict.get('referenced_table_names', []),
            position_x=position.get('x', 100.0),
            position_y=position.get('y', 100.0),
            width=size.get('width', 250.0),
            height=size.get('height', 180.0),
            created_at=created_at or datetime.now(),
            updated_at=updated_at or datetime.now()
        )
