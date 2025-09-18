import { create } from 'zustand';
import axios from 'axios';

import { BACKEND_HOST } from "src/config"

import generateUUID from 'src/utils/idGenerator';

// Helper function to check if a field is a drag-and-drop FK (same name as referenced PK)
const isDragAndDropFK = (field, tables, relationships) => {
  if (!field.is_foreign_key || !field.foreign_key_reference) return false;
  
  // Find the referenced PK field
  const referencedTable = tables.find(t => t.id === field.foreign_key_reference.referenced_table_id);
  const referencedField = referencedTable?.fields?.find(f => f.id === field.foreign_key_reference.referenced_field_id);
  
  return field.name === referencedField?.name;
};

// Helper function to sync PK field changes to all matching FK fields
const syncPKFieldToFKs = (updatedPKField, originalPKField, pkTableId, tables, relationships) => {
  return tables.map(table => {
    if (table.id === pkTableId) {
      // This is the PK table, update the PK field
      return {
        ...table,
        fields: table.fields.map(field => 
          field.id === updatedPKField.id ? updatedPKField : field
        )
      };
    } else {
      // Check if this table has FK fields that reference the updated PK field
      return {
        ...table,
        fields: table.fields.map(field => {
          if (field.is_foreign_key && 
              field.foreign_key_reference?.referenced_table_id === pkTableId &&
              field.foreign_key_reference?.referenced_field_id === updatedPKField.id) {
            
            // Determine if this is a drag-and-drop FK (same name) or manual FK (different name)
            const isDragAndDropFK = field.name === originalPKField.name;
            
            console.log(`🔄 Syncing ${isDragAndDropFK ? 'drag-and-drop' : 'manual'} FK field:`, field.name, '→ PK:', updatedPKField.name);
            console.log('   📊 PK tags:', updatedPKField.tags);
            console.log('   📊 FK tags (before):', field.tags);
            
            // Merge tags - add PK tags to FK tags without overwriting existing FK tags
            const mergedTags = {
              ...(updatedPKField.tags || {}),
              ...(field.tags || {})  // FK tags take precedence
            };
            console.log('   📊 Merged tags:', mergedTags);
            
            return {
              ...field,
              // For drag-and-drop FKs, sync the name too. For manual FKs, keep original name
              name: isDragAndDropFK ? updatedPKField.name : field.name,
              // Always sync these properties for ALL FKs
              data_type: updatedPKField.data_type,
              type_parameters: updatedPKField.type_parameters,
              nullable: updatedPKField.nullable,
              comment: updatedPKField.comment,
              // Only sync logical_name if FK doesn't have one
              logical_name: field.logical_name || updatedPKField.logical_name,
              // Use merged tags
              tags: mergedTags,
              // Keep FK-specific properties
              is_foreign_key: true,
              foreign_key_reference: field.foreign_key_reference
            };
          }
          return field;
        })
      };
    }
  });
};

// Helper function to clean data and remove circular references
const cleanDataForSerialization = (obj) => {
  const seen = new WeakSet();
  
  const clean = (value) => {
    // Handle null and primitive values
    if (value === null || typeof value !== 'object') {
      return value;
    }
    
    // Check for circular references
    if (seen.has(value)) {
      return null; // Replace circular references with null
    }
    
    // Skip DOM elements and React internals
    try {
      if (value.constructor && value.constructor.name) {
        const constructorName = value.constructor.name;
        if (constructorName.startsWith('HTML') || 
            constructorName === 'FiberNode' || 
            constructorName === 'Element' ||
            constructorName === 'Node') {
          return null;
        }
      }
    } catch (e) {
      // If we can't check the constructor, skip this object to be safe
      return null;
    }
    
    // Add to seen set
    seen.add(value);
    
    try {
      if (Array.isArray(value)) {
        const result = value.map(clean).filter(item => item !== null);
        seen.delete(value);
        return result;
      }
      
      const result = {};
      for (const key in value) {
        // Skip React internals and functions
        if (key.startsWith('__react') || 
            key.startsWith('_react') || 
            key === 'stateNode' ||
            typeof value[key] === 'function') {
          continue;
        }
        
        const cleanedValue = clean(value[key]);
        if (cleanedValue !== null) {
          result[key] = cleanedValue;
        }
      }
      seen.delete(value);
      return result;
    } catch (error) {
      seen.delete(value);
      return null;
    }
  };
  
  return clean(obj);
};

// Helper function to transform tables data for backend compatibility
const transformTablesForBackend = (tables) => {
  // First clean the tables to remove any circular references
  const cleanTables = cleanDataForSerialization(tables);
  
  return cleanTables.map(table => ({
    ...table,
    id: String(table.id), // Ensure ID is string
    fields: table.fields.map(field => {
      let transformedField = {
        ...field,
        id: String(field.id), // Ensure field ID is string
        type_parameters: (() => {
          // Handle different type_parameters formats
          if (!field.type_parameters) return null;
          
          // If it's already a string, use it as-is
          if (typeof field.type_parameters === 'string') {
            return field.type_parameters;
          }
          
          // If it's an object with precision/scale (DECIMAL), convert to string format
          if (typeof field.type_parameters === 'object' && Object.keys(field.type_parameters).length > 0) {
            const params = field.type_parameters;
            if (params.precision !== undefined) {
              // DECIMAL format: "precision,scale" or "precision"
              return params.scale !== undefined ? `${params.precision},${params.scale}` : String(params.precision);
            }
            // For other object formats, JSON stringify (though this shouldn't happen with current UI)
            return JSON.stringify(params);
          }
          
          return null;
        })()
      };

       // Convert legacy string foreign_key_reference to new object format
       if (field.is_foreign_key && field.foreign_key_reference && typeof field.foreign_key_reference === 'string') {
         if (field.foreign_key_reference.includes('.')) {
           const [referencedTableName, referencedFieldName] = field.foreign_key_reference.split('.');
           const referencedTable = cleanTables.find(t => t.name === referencedTableName);
           const referencedField = referencedTable?.fields?.find(f => f.name === referencedFieldName);
          
          if (referencedTable && referencedField) {
            transformedField.foreign_key_reference = {
              referenced_table_id: referencedTable.id,
              referenced_field_id: referencedField.id,
              constraint_name: `fk_${table.name}_${referencedTableName}`,
              on_delete: "NO ACTION",
              on_update: "NO ACTION"
            };
          }
        }
      }

      return transformedField;
    }),
    created_at: (() => {
      if (!table.created_at) return new Date().toISOString();
      if (typeof table.created_at === 'string') {
        // Handle GMT format from imported tables
        if (table.created_at.includes('GMT')) {
          return new Date(table.created_at).toISOString();
        }
        // Handle ISO format
        if (table.created_at.match(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/)) {
          return table.created_at;
        }
      }
      return new Date().toISOString();
    })(),
    updated_at: (() => {
      if (!table.updated_at) return new Date().toISOString();
      if (typeof table.updated_at === 'string') {
        // Handle GMT format from imported tables
        if (table.updated_at.includes('GMT')) {
          return new Date(table.updated_at).toISOString();
        }
        // Handle ISO format
        if (table.updated_at.match(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/)) {
          return table.updated_at;
        }
      }
      return new Date().toISOString();
    })()
  }));
};

const useDataModelingStore = create((set, get) => ({
  // Current project state
  currentProject: null,
  projectName: '',
  catalogName: '',
  schemaName: '',
  
  // Canvas state
  tables: [],
  metricViews: [],
  traditionalViews: [],
  relationships: [],
  metricRelationships: [],
  selectedTables: [],
  selectedMetricViews: [],
  selectedTraditionalViews: [],
  selectedRelationships: [],
  
  // UI state
  showTableDialog: false,
  showFieldDialog: false,
  showRelationshipDialog: false,
  showMetricViewDialog: false,
  showTraditionalViewDialog: false,
  showFieldMappingDialog: false,
  showImportDialog: false,
  showProjectDialog: false,
  showProjectStartupDialog: true,  // Show project selection on startup
  projectStartupMode: 'select', // 'select', 'create', 'load'
  selectedTableForEditing: null,
  selectedFieldForEditing: null,
  selectedMetricViewForEditing: null,
  selectedTraditionalViewForEditing: null,
  editingTable: null,
  editingMetricView: null,
  editingTraditionalView: null,
  fieldMappingSourceTable: null,
  fieldMappingTargetMetricView: null,
  
  // Available data from Databricks
  availableCatalogs: [],
  availableSchemas: [],
  availableTables: [],
  availableDatatypes: [],
  savedProjects: [],
  
  // App state
  appState: {
    needs_project_selection: true,
    has_existing_projects: false,
    project_count: 0
  },
  
  // Error handling
  displayAlertDialog: false,
  currentAlertDialogErrorType: null,
  errors: {
    loadCatalogs: null,
    loadSchemas: null,
    loadTables: null,
    importTables: null,
    saveProject: null,
    loadProject: null,
    generateDDL: null,
    applyChanges: null,
  },

  // Helper functions
  isDragAndDropFK: (field, tableId) => {
    const state = get();
    return isDragAndDropFK(field, state.tables, state.relationships);
  },

  getPKFieldForFK: (fkField) => {
    const state = get();
    if (!fkField.is_foreign_key || !fkField.foreign_key_reference) return null;
    
    const referencedTable = state.tables.find(t => t.id === fkField.foreign_key_reference.referenced_table_id);
    const referencedField = referencedTable?.fields?.find(f => f.id === fkField.foreign_key_reference.referenced_field_id);
    
    return referencedField;
  },

  // Project actions
  setCurrentProject: (project) => set({ currentProject: project }),
  setProjectMetadata: (name, catalog, schema) => {
    const currentProject = get().currentProject;
    set({ 
      projectName: name, 
      catalogName: catalog, 
      schemaName: schema,
      // Also update the current project object
      currentProject: currentProject ? {
        ...currentProject,
        catalog_name: catalog,
        schema_name: schema
      } : null
    });
  },
  
  createNewProject: (name, description, catalog, schema) => {
    const newProject = {
      id: generateUUID(),
      name,
      description,
      catalog_name: catalog,
      schema_name: schema,
      tables: [],
      relationships: [],
      version: '1.0',
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
      canvas_settings: {}
    };
    
    set({ 
      currentProject: newProject,
      projectName: name,
      catalogName: catalog,
      schemaName: schema,
      tables: [],
      relationships: []
    });
    
    return newProject;
  },

  // Table actions
  addTable: (tableData) => {
    const newTable = {
      id: tableData.id || generateUUID(),
      name: tableData.name,
      logical_name: tableData.logical_name,
      comment: tableData.comment,
      table_type: tableData.table_type || 'MANAGED',
      file_format: tableData.file_format || 'DELTA',
      tags: tableData.tags || {},
      position_x: tableData.position_x || 100,
      position_y: tableData.position_y || 100,
      width: tableData.width || 200,
      height: tableData.height || 150,
      fields: tableData.fields || [],
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString()
    };
    
    set(state => ({
      tables: [...state.tables, newTable],
      currentProject: state.currentProject ? {
        ...state.currentProject,
        tables: [...state.currentProject.tables, newTable],
        updated_at: new Date().toISOString()
      } : null
    }));
    
    return newTable;
  },

  updateTable: (tableId, updates) => {
    set(state => {
      let updatedTables = state.tables.map(table =>
        table.id === tableId 
          ? { ...table, ...updates, updated_at: new Date().toISOString() }
          : table
      );
      
      // Check if we updated any PK fields and need to sync to FK fields
      if (updates.fields) {
        const updatedTable = updatedTables.find(t => t.id === tableId);
        const originalTable = state.tables.find(t => t.id === tableId);
        
        // Check each updated field to see if it's a PK that needs to sync to FKs
        updates.fields.forEach(updatedField => {
          const originalField = originalTable?.fields?.find(f => f.id === updatedField.id);
          
          // If this is a PK field and it has been modified
          if (updatedField.is_primary_key && originalField) {
            // Check for tag changes
            const originalTags = JSON.stringify(originalField.tags || {});
            const updatedTags = JSON.stringify(updatedField.tags || {});
            const tagsChanged = originalTags !== updatedTags;
            
            const hasChanged = (
              originalField.data_type !== updatedField.data_type ||
              originalField.type_parameters !== updatedField.type_parameters ||
              originalField.nullable !== updatedField.nullable ||
              originalField.comment !== updatedField.comment ||
              originalField.logical_name !== updatedField.logical_name ||
              originalField.name !== updatedField.name ||
              tagsChanged
            );
            
            console.log('🔍 PK Change Detection:');
            console.log('   📊 Original tags:', originalField.tags);
            console.log('   📊 Updated tags:', updatedField.tags);
            console.log('   📊 Tags changed:', tagsChanged);
            
            if (hasChanged) {
              console.log('🔄 PK field changed, syncing to FK fields:', updatedField.name);
              console.log('🔄 Original PK field name:', originalField.name);
              console.log('🔄 Before sync - tables count:', updatedTables.length);
              console.log('🔄 Looking for FK fields that reference table:', tableId, 'field:', updatedField.id);
              
              // Debug: Check what FK fields exist that might need syncing
              updatedTables.forEach(table => {
                const fkFields = table.fields.filter(f => 
                  f.is_foreign_key && 
                  f.foreign_key_reference?.referenced_table_id === tableId &&
                  f.foreign_key_reference?.referenced_field_id === updatedField.id
                  // ✅ REMOVED name check - now syncs ALL FKs regardless of name
                );
                if (fkFields.length > 0) {
                  const dragDropFKs = fkFields.filter(f => f.name === originalField.name);
                  const manualFKs = fkFields.filter(f => f.name !== originalField.name);
                  console.log('🔄 Found FK fields to sync in table', table.name, ':');
                  console.log('   📌 Drag-and-drop FKs (same name):', dragDropFKs.map(f => f.name));
                  console.log('   🔗 Manual FKs (different names):', manualFKs.map(f => f.name));
                }
              });
              
              updatedTables = syncPKFieldToFKs(updatedField, originalField, tableId, updatedTables, state.relationships);
              console.log('🔄 After sync - tables count:', updatedTables.length);
            }
          }
        });
      }
      
      return {
        tables: updatedTables,
        currentProject: state.currentProject ? {
          ...state.currentProject,
          tables: updatedTables,
          updated_at: new Date().toISOString()
        } : null
      };
    });
  },

  deleteTable: (tableId) => {
    set(state => {
      console.log('🗑️ Deleting table with ID:', tableId);
      
      // Find all relationships involving this table
      const relationshipsToDelete = state.relationships.filter(rel => 
        rel.source_table_id === tableId || rel.target_table_id === tableId
      );
      
      console.log('🗑️ Found relationships to handle:', relationshipsToDelete.length);
      
      // Start with current tables
      let updatedTables = [...state.tables];
      
      // Process each relationship to handle FK fields properly
      relationshipsToDelete.forEach(relationship => {
        console.log('🗑️ Processing relationship:', relationship.id);
        
        // Skip if this table is being deleted (we'll remove it anyway)
        if (relationship.fk_table_id === tableId) {
          console.log('🗑️ FK table is being deleted, skipping FK field handling');
          return;
        }
        
        // Determine the correct FK table and field
        let fkTableId, fkFieldId;
        
        if (relationship.fk_table_id && relationship.fk_field_id) {
          // Use explicit FK tracking fields (preferred)
          fkTableId = relationship.fk_table_id;
          fkFieldId = relationship.fk_field_id;
          console.log('🗑️ Using explicit FK tracking fields');
        } else {
          // Fallback: The FK field is in the TARGET table (not source)
          fkTableId = relationship.target_table_id;
          fkFieldId = relationship.target_field_id;
          console.log('🗑️ Using target table/field as FK location');
        }
        
        // Skip if FK table is the one being deleted
        if (fkTableId === tableId) {
          console.log('🗑️ FK table is being deleted, skipping FK field handling');
          return;
        }
        
        console.log('🗑️ FK table ID (has FK):', fkTableId);
        console.log('🗑️ FK field ID (FK field):', fkFieldId);
        
        // Find the FK table and handle FK field removal
        const fkTable = updatedTables.find(t => t.id === fkTableId);
        if (fkTable) {
          console.log('🗑️ Found FK table:', fkTable.name);
          const fkFieldToModify = fkTable.fields.find(f => f.id === fkFieldId);
          console.log('🗑️ FK field to modify:', fkFieldToModify?.name);
          
          if (fkFieldToModify) {
            // Find the referenced PK field to compare names
            const pkTable = updatedTables.find(table => table.id === relationship.source_table_id);
            const pkField = pkTable?.fields?.find(field => field.id === relationship.source_field_id);
            
            console.log('🗑️ Referenced PK field name:', pkField?.name);
            console.log('🗑️ FK field name:', fkFieldToModify.name);
            
            // Simple logic: If FK name matches PK name, it was drag-and-drop created
            const isDragAndDropFK = fkFieldToModify.name === pkField?.name;
            
            if (isDragAndDropFK) {
              // Drag-and-drop FK: Remove the entire field
              console.log('🗑️ Drag-and-drop FK (same name as PK): Removing entire field');
              const updatedFields = fkTable.fields.filter(field => field.id !== fkFieldId);
              updatedTables = updatedTables.map(table => 
                table.id === fkTableId 
                  ? { ...table, fields: updatedFields }
                  : table
              );
              console.log('🗑️ Removed entire FK field from FK table:', fkTable.name);
            } else {
              // Manual FK: Only remove FK reference, keep the field
              console.log('🗑️ Manual FK (different name from PK): Removing only FK reference, keeping field');
              const updatedFields = fkTable.fields.map(field => 
                field.id === fkFieldId 
                  ? { 
                      ...field, 
                      is_foreign_key: false, 
                      foreign_key_reference: null 
                    }
                  : field
              );
              updatedTables = updatedTables.map(table => 
                table.id === fkTableId 
                  ? { ...table, fields: updatedFields }
                  : table
              );
              console.log('🗑️ Removed FK reference from field, kept field:', fkFieldToModify.name);
            }
          }
        }
      });
      
      // Now remove the table itself and filter relationships
      const filteredTables = updatedTables.filter(table => table.id !== tableId);
      const filteredRelationships = state.relationships.filter(rel => 
        rel.source_table_id !== tableId && rel.target_table_id !== tableId
      );
      
      console.log('🗑️ Table deletion complete. Remaining tables:', filteredTables.length);
      console.log('🗑️ Remaining relationships:', filteredRelationships.length);
      
      return {
        tables: filteredTables,
        relationships: filteredRelationships,
        currentProject: state.currentProject ? {
          ...state.currentProject,
          tables: filteredTables,
          relationships: filteredRelationships,
          updated_at: new Date().toISOString()
        } : null
      };
    });
  },

  // Field actions
  addFieldToTable: (tableId, fieldData) => {
    const newField = {
      id: generateUUID(),
      name: fieldData.name,
      data_type: fieldData.data_type,
      type_parameters: fieldData.type_parameters,
      nullable: fieldData.nullable !== false,
      default_value: fieldData.default_value,
      comment: fieldData.comment,
      logical_name: fieldData.logical_name,
      tags: fieldData.tags || {},
      is_primary_key: fieldData.is_primary_key || false,
      is_foreign_key: fieldData.is_foreign_key || false,
      foreign_key_reference: fieldData.foreign_key_reference || null
    };
    
    get().updateTable(tableId, {
      fields: [...(get().tables.find(t => t.id === tableId)?.fields || []), newField]
    });
    
    return newField;
  },

  updateField: (tableId, fieldId, updates) => {
    const table = get().tables.find(t => t.id === tableId);
    if (table) {
      const updatedFields = table.fields.map(field =>
        field.id === fieldId ? { ...field, ...updates } : field
      );
      get().updateTable(tableId, { fields: updatedFields });
    }
  },

  deleteField: (tableId, fieldId) => {
    const table = get().tables.find(t => t.id === tableId);
    if (table) {
      const filteredFields = table.fields.filter(field => field.id !== fieldId);
      get().updateTable(tableId, { fields: filteredFields });
      
      // Also remove any relationships that reference this field
      set(state => ({
        relationships: state.relationships.filter(rel =>
          rel.source_field_id !== fieldId && rel.target_field_id !== fieldId
        )
      }));
    }
  },

  // Relationship actions
  addRelationship: (relationshipData) => {
    const newRelationship = {
      id: generateUUID(),
      source_table_id: relationshipData.source_table_id,
      target_table_id: relationshipData.target_table_id,
      source_field_id: relationshipData.source_field_id,
      target_field_id: relationshipData.target_field_id,
      relationship_type: relationshipData.relationship_type || 'many_to_one',
      constraint_name: relationshipData.constraint_name,
      line_points: relationshipData.line_points || [],
      // ✅ ADD THESE FOR DRAG-AND-DROP IDENTIFICATION
      fk_table_id: relationshipData.fk_table_id,
      fk_field_id: relationshipData.fk_field_id,
      foreign_key_reference: relationshipData.foreign_key_reference
    };
    
    set(state => ({
      relationships: [...state.relationships, newRelationship],
      currentProject: state.currentProject ? {
        ...state.currentProject,
        relationships: [...state.currentProject.relationships, newRelationship],
        updated_at: new Date().toISOString()
      } : null
    }));
    
    return newRelationship;
  },

  updateRelationship: (relationshipId, updates) => {
    set(state => {
      const updatedRelationships = state.relationships.map(rel =>
        rel.id === relationshipId ? { ...rel, ...updates } : rel
      );
      
      return {
        relationships: updatedRelationships,
        currentProject: state.currentProject ? {
          ...state.currentProject,
          relationships: updatedRelationships,
          updated_at: new Date().toISOString()
        } : null
      };
    });
  },

  // Helper function to remove only the relationship line without touching FK field properties
  removeRelationshipLine: (relationshipId) => {
    set(state => {
      console.log('🔗 Removing relationship line only (no FK field changes):', relationshipId);
      const filteredRelationships = state.relationships.filter(rel => rel.id !== relationshipId);
      
      return {
        relationships: filteredRelationships,
        currentProject: state.currentProject ? {
          ...state.currentProject,
          relationships: filteredRelationships,
          updated_at: new Date().toISOString()
        } : null
      };
    });
  },

  deleteRelationship: (relationshipId) => {
    set(state => {
      const relationship = state.relationships.find(rel => rel.id === relationshipId);
      const filteredRelationships = state.relationships.filter(rel => rel.id !== relationshipId);
      
      // If relationship exists, also delete the FK field from the FK table
      if (relationship) {
        console.log('🗑️ Deleting relationship:', relationship);
        
        // Determine the correct FK table and field
        // The FK field should be in the TARGET table (where the FK was created)
        let fkTableId, fkFieldId;
        
        console.log('🗑️ Relationship fk_table_id:', relationship.fk_table_id);
        console.log('🗑️ Relationship fk_field_id:', relationship.fk_field_id);
        
        if (relationship.fk_table_id && relationship.fk_field_id) {
          // Use explicit FK tracking fields (preferred)
          fkTableId = relationship.fk_table_id;
          fkFieldId = relationship.fk_field_id;
          console.log('🗑️ Using explicit FK tracking fields (drag-and-drop)');
        } else {
          // Fallback: The FK field is in the TARGET table (not source)
          // For relationships: source = PK table, target = FK table
          fkTableId = relationship.target_table_id;
          fkFieldId = relationship.target_field_id;
          console.log('🗑️ Using target table/field as FK location (manual)');
        }
        
        console.log('🗑️ FK table ID (has FK):', fkTableId);
        console.log('🗑️ FK field ID (FK field):', fkFieldId);
        
        // Find the FK table and handle FK field removal
        const fkTable = state.tables.find(t => t.id === fkTableId);
        if (fkTable) {
          console.log('🗑️ Found FK table:', fkTable.name);
          const fkFieldToModify = fkTable.fields.find(f => f.id === fkFieldId);
          console.log('🗑️ FK field to modify:', fkFieldToModify?.name, '(is_foreign_key:', fkFieldToModify?.is_foreign_key, ')');
          
          let updatedTables;
          
          // Find the referenced PK field to compare names
          const pkTable = state.tables.find(table => table.id === relationship.source_table_id);
          const pkField = pkTable?.fields?.find(field => field.id === relationship.source_field_id);
          
          console.log('🗑️ Referenced PK field name:', pkField?.name);
          console.log('🗑️ FK field name:', fkFieldToModify.name);
          
          // Simple logic: If FK name matches PK name, it was drag-and-drop created
          const isDragAndDropFK = fkFieldToModify.name === pkField?.name;
          
          if (isDragAndDropFK) {
            // Drag-and-drop FK: Remove the entire field
            console.log('🗑️ Drag-and-drop FK (same name as PK): Removing entire field');
            const updatedFields = fkTable.fields.filter(field => field.id !== fkFieldId);
            updatedTables = state.tables.map(table => 
              table.id === fkTableId 
                ? { ...table, fields: updatedFields }
                : table
            );
            console.log('🗑️ Removed entire FK field from FK table:', fkTable.name);
          } else {
            // Manual FK: Only remove FK reference, keep the field
            console.log('🗑️ Manual FK (different name from PK): Removing only FK reference, keeping field');
            const updatedFields = fkTable.fields.map(field => 
              field.id === fkFieldId 
                ? { 
                    ...field, 
                    is_foreign_key: false, 
                    foreign_key_reference: null 
                  }
                : field
            );
            updatedTables = state.tables.map(table => 
              table.id === fkTableId 
                ? { ...table, fields: updatedFields }
                : table
            );
            console.log('🗑️ Removed FK reference from field, kept field:', fkFieldToModify?.name);
          }
          
          return {
            relationships: filteredRelationships,
            tables: updatedTables,
            currentProject: state.currentProject ? {
              ...state.currentProject,
              relationships: filteredRelationships,
              tables: updatedTables,
              updated_at: new Date().toISOString()
            } : null
          };
        }
      }
      
      return {
        relationships: filteredRelationships,
        currentProject: state.currentProject ? {
          ...state.currentProject,
          relationships: filteredRelationships,
          updated_at: new Date().toISOString()
        } : null
      };
    });
  },

  // Metric View actions
  addMetricView: (metricViewData) => {
    const newMetricView = {
      id: metricViewData.id || generateUUID(),
      name: metricViewData.name,
      description: metricViewData.description || '',
      version: metricViewData.version || '0.1',
      source_table_id: metricViewData.source_table_id,
      dimensions: metricViewData.dimensions || [],
      measures: metricViewData.measures || [],
      joins: metricViewData.joins || [],
      tags: metricViewData.tags || {},
      position_x: metricViewData.position_x || 100,
      position_y: metricViewData.position_y || 100,
      width: metricViewData.width || 280,
      height: metricViewData.height || 220,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString()
    };
    
    set(state => ({
      metricViews: [...state.metricViews, newMetricView],
      currentProject: state.currentProject ? {
        ...state.currentProject,
        metric_views: [...(state.currentProject.metric_views || []), newMetricView],
        updated_at: new Date().toISOString()
      } : null
    }));
    
    return newMetricView;
  },

  updateMetricView: (metricViewId, updates) => {
    set(state => ({
      metricViews: state.metricViews.map(metricView =>
        metricView.id === metricViewId 
          ? { ...metricView, ...updates, updated_at: new Date().toISOString() }
          : metricView
      ),
      currentProject: state.currentProject ? {
        ...state.currentProject,
        metric_views: (state.currentProject.metric_views || []).map(metricView =>
          metricView.id === metricViewId 
            ? { ...metricView, ...updates, updated_at: new Date().toISOString() }
            : metricView
        ),
        updated_at: new Date().toISOString()
      } : null
    }));
  },

  deleteMetricView: (metricViewId) => {
    set(state => ({
      metricViews: state.metricViews.filter(metricView => metricView.id !== metricViewId),
      selectedMetricViews: state.selectedMetricViews.filter(id => id !== metricViewId),
      currentProject: state.currentProject ? {
        ...state.currentProject,
        metric_views: (state.currentProject.metric_views || []).filter(metricView => metricView.id !== metricViewId),
        updated_at: new Date().toISOString()
      } : null
    }));
  },

  // Selection actions
  setSelectedTables: (tables) => set({ selectedTables: tables }),
  setSelectedRelationships: (relationships) => set({ selectedRelationships: relationships }),

  // Dialog actions
  setShowTableDialog: (show, table = null) => set({
    showTableDialog: show,
    selectedTableForEditing: table,
    editingTable: table
  }),
  setEditingTable: (table) => set({
    editingTable: table
  }),
  setShowFieldDialog: (show, field = null) => set({ 
    showFieldDialog: show, 
    selectedFieldForEditing: field 
  }),
  setShowRelationshipDialog: (show) => set({ showRelationshipDialog: show }),
  setShowImportDialog: (show) => set({ showImportDialog: show }),
  setShowProjectDialog: (show) => set({ showProjectDialog: show }),
  setShowProjectStartupDialog: (show, mode = 'select') => set({ 
    showProjectStartupDialog: show, 
    projectStartupMode: mode 
  }),

  // API calls
  loadCatalogs: async () => {
    try {
      set({
        errors: { ...get().errors, loadCatalogs: null }
      });
      
      const response = await axios.get(`${BACKEND_HOST}/api/data_modeling/catalogs`);
      set({ availableCatalogs: response.data });
      return response.data;
    } catch (error) {
      console.error('Error loading catalogs:', error);
      set({
        errors: { ...get().errors, loadCatalogs: error.response?.data || error.message },
        currentAlertDialogErrorType: "loadCatalogs",
        displayAlertDialog: true
      });
      throw error;
    }
  },

  loadSchemas: async (catalogName) => {
    try {
      set({
        errors: { ...get().errors, loadSchemas: null }
      });
      
      const response = await axios.get(`${BACKEND_HOST}/api/data_modeling/schemas/${catalogName}`);
      let schemas = response.data;
      
      // Add schemas from current project objects that might not exist in database
      const state = get();
      const projectSchemas = new Set();
      
      // Collect schemas from all project objects
      [...state.tables, ...state.metricViews, ...state.traditionalViews].forEach(obj => {
        if (obj.catalog_name === catalogName && obj.schema_name) {
          projectSchemas.add(obj.schema_name);
        }
      });
      
      // Add project schemas that aren't in the database response
      projectSchemas.forEach(schemaName => {
        if (!schemas.some(schema => schema.name === schemaName)) {
          schemas.push({
            name: schemaName,
            comment: 'Schema from imported project (not in database)',
            is_imported: true
          });
        }
      });
      
      set({ availableSchemas: schemas });
      return schemas;
    } catch (error) {
      console.error('Error loading schemas:', error);
      set({
        errors: { ...get().errors, loadSchemas: error.response?.data || error.message },
        currentAlertDialogErrorType: "loadSchemas",
        displayAlertDialog: true
      });
      throw error;
    }
  },

  loadTables: async (catalogName, schemaName) => {
    try {
      set({
        errors: { ...get().errors, loadTables: null }
      });
      
      const response = await axios.get(`${BACKEND_HOST}/api/data_modeling/tables/${catalogName}/${schemaName}`);
      set({ availableTables: response.data });
      return response.data;
    } catch (error) {
      console.error('Error loading tables:', error);
      set({
        errors: { ...get().errors, loadTables: error.response?.data || error.message },
        currentAlertDialogErrorType: "loadTables",
        displayAlertDialog: true
      });
      throw error;
    }
  },

  loadDatatypes: async () => {
    try {
      const response = await axios.get(`${BACKEND_HOST}/api/data_modeling/datatypes`);
      set({ availableDatatypes: response.data });
      return response.data;
    } catch (error) {
      console.error('Error loading datatypes:', error);
      // Don't show error dialog for datatypes as it's not critical
      return [];
    }
  },

  importExistingTables: async (catalogName, schemaName, tableNames, sessionId = null) => {
    try {
      set({
        errors: { ...get().errors, importTables: null }
      });
      
      // Get existing tables to avoid duplicates and enable relationship creation
      const storeState = get();
      const existingTables = storeState.tables.map(table => ({
        id: table.id,
        name: table.name,
        catalog_name: table.catalog_name,
        schema_name: table.schema_name,
        fields: table.fields || []
      }));

      const requestData = {
        catalog_name: catalogName,
        schema_name: schemaName,
        table_names: tableNames,
        include_constraints: true,
        position_strategy: 'auto_layout',
        existing_tables: existingTables
      };
      
      // Add session ID if provided for progress streaming
      if (sessionId) {
        requestData.session_id = sessionId;
      }
      
      const response = await axios.post(`${BACKEND_HOST}/api/data_modeling/import_existing`, requestData);
      
      const importedProject = response.data;
      const currentState = get();

      // Check for duplicate table names and IDs
      const existingTableIds = new Set(currentState.tables.map(t => t.id));
      const existingTableNames = new Set(currentState.tables.map(t => t.name.toLowerCase()));
      
      // Filter out tables with duplicate IDs or names and apply parameter conversion
      const duplicateNames = [];
      const newTables = importedProject.tables.filter(table => {
        if (existingTableIds.has(table.id)) {
          return false; // Skip tables with duplicate IDs
        }
        if (existingTableNames.has(table.name.toLowerCase())) {
          duplicateNames.push(table.name);
          return false; // Skip tables with duplicate names
        }
        return true;
      }).map(table => ({
        ...table,
        // Apply the same field conversion logic as loadProject
        fields: (table.fields || []).map(field => {
          let transformedField = { ...field };
          
          // Convert string type_parameters to object format for DECIMAL fields
          if (field.data_type === 'DECIMAL' && typeof field.type_parameters === 'string') {
            const params = field.type_parameters;
            if (params && params.trim()) {
              const parts = params.split(',');
              if (parts.length >= 1) {
                const precision = parseInt(parts[0].trim());
                const scale = parts.length > 1 ? parseInt(parts[1].trim()) : 0;
                transformedField.type_parameters = {
                  precision: isNaN(precision) ? 10 : precision,
                  scale: isNaN(scale) ? 0 : scale
                };
              }
            }
          }
          
          return transformedField;
        })
      }));
      
      // Show warning if there were duplicate names
      if (duplicateNames.length > 0) {
        const message = `The following tables were not imported because tables with the same names already exist in the canvas: ${duplicateNames.join(', ')}`;
        console.warn(message);
        
        // Store the warning to show in UI
        set({
          errors: { 
            ...get().errors, 
            importTables: message 
          }
        });
        
        // If no tables were imported due to duplicates, throw an error
        if (newTables.length === 0) {
          throw new Error(message);
        }
      }

      // Merge imported relationships with existing relationships, avoiding duplicates
      const existingRelationshipIds = new Set(
        currentState.relationships.map(r => `${r.source_table_id}-${r.target_table_id}-${r.source_field_id}-${r.target_field_id}`)
      );
      const newRelationships = importedProject.relationships.filter(relationship => {
        const relationshipId = `${relationship.source_table_id}-${relationship.target_table_id}-${relationship.source_field_id}-${relationship.target_field_id}`;
        return !existingRelationshipIds.has(relationshipId);
      });

      set({
        currentProject: {
          ...currentState.currentProject,
          tables: [...currentState.tables, ...newTables],
          relationships: [...currentState.relationships, ...newRelationships]
        },
        tables: [...currentState.tables, ...newTables],
        relationships: [...currentState.relationships, ...newRelationships]
      });
      
      return importedProject;
    } catch (error) {
      console.error('Error importing tables:', error);
      set({
        errors: { ...get().errors, importTables: error.response?.data || error.message },
        currentAlertDialogErrorType: "importTables",
        displayAlertDialog: true
      });
      throw error;
    }
  },

  importExistingTablesCrossCatalog: async (tableGroups, sessionId) => {
    try {
      set({
        errors: { ...get().errors, importTables: null }
      });
      
      const requestData = {
        table_groups: tableGroups, // Format: [{'catalog': 'cat1', 'schema': 'sch1', 'tables': ['t1', 't2']}, ...]
        session_id: sessionId
      };
      
      console.log('🌐 Calling cross-catalog import endpoint with:', requestData);
      
      const response = await fetch(`${BACKEND_HOST}/api/data_modeling/import_existing_cross_catalog`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(requestData)
      });

      const result = await response.json();
      
      if (!response.ok) {
        throw new Error(result.error || `HTTP ${response.status}`);
      }
      
      console.log('✅ Cross-catalog import endpoint response:', result);
      
      // Process the imported tables and relationships directly from the response
      if (result.tables && result.tables.length > 0) {
        const currentState = get();
        
        // Check for duplicate table names and IDs
        const existingTableIds = new Set(currentState.tables.map(t => t.id));
        const existingTableNames = new Set(currentState.tables.map(t => t.name.toLowerCase()));
        
        // Filter out tables with duplicate IDs or names and apply field transformations
        const duplicateNames = [];
        const newTables = result.tables.filter(table => {
          if (existingTableIds.has(table.id)) {
            return false; // Skip tables with duplicate IDs
          }
          if (existingTableNames.has(table.name.toLowerCase())) {
            duplicateNames.push(table.name);
            return false; // Skip tables with duplicate names
          }
          return true;
        }).map(table => ({
          ...table,
          // Apply the same field conversion logic as loadProject
          fields: (table.fields || []).map(field => {
            let transformedField = { ...field };
            
            // Convert string type_parameters to object format for DECIMAL fields
            if (field.data_type === 'DECIMAL' && typeof field.type_parameters === 'string') {
              const params = field.type_parameters;
              if (params && params.trim()) {
                const parts = params.split(',');
                if (parts.length >= 1) {
                  const precision = parseInt(parts[0].trim());
                  const scale = parts.length > 1 ? parseInt(parts[1].trim()) : 0;
                  transformedField.type_parameters = {
                    precision: isNaN(precision) ? 10 : precision,
                    scale: isNaN(scale) ? 0 : scale
                  };
                }
              }
            }
            
            return transformedField;
          })
        }));
        
        // Handle relationships
        const existingRelationshipIds = new Set(
          currentState.relationships.map(r => `${r.source_table_id}-${r.target_table_id}-${r.source_field_id}-${r.target_field_id}`)
        );
        const newRelationships = (result.relationships || []).filter(relationship => {
          const relationshipId = `${relationship.source_table_id}-${relationship.target_table_id}-${relationship.source_field_id}-${relationship.target_field_id}`;
          return !existingRelationshipIds.has(relationshipId);
        });
        
        // Update the store with imported tables and relationships
        set({
          currentProject: {
            ...currentState.currentProject,
            tables: [...currentState.tables, ...newTables],
            relationships: [...currentState.relationships, ...newRelationships]
          },
          tables: [...currentState.tables, ...newTables],
          relationships: [...currentState.relationships, ...newRelationships]
        });
        
        console.log(`✅ Added ${newTables.length} tables and ${newRelationships.length} relationships to store`);
        
        // Show warning if there were duplicate names
        if (duplicateNames.length > 0) {
          const message = `The following tables were not imported because tables with the same names already exist in the canvas: ${duplicateNames.join(', ')}`;
          console.warn(message);
          
          set({
            errors: { 
              ...get().errors, 
              importTables: message 
            }
          });
        }
      }
      
      return result;
      
    } catch (error) {
      console.error('❌ Cross-catalog import failed:', error);
      set({
        errors: { ...get().errors, importTables: error.message }
      });
      throw error;
    }
  },

  saveProject: async (name, format = 'yaml', overwrite = false) => {
    try {
      set({
        errors: { ...get().errors, saveProject: null }
      });

      const currentProject = get().currentProject;
      if (!currentProject) {
        throw new Error('No project to save');
      }

      // Transform tables data to ensure proper types
      const transformedTables = transformTablesForBackend(get().tables);

      // Transform metric relationships to ensure proper date formats
      const transformedMetricRelationships = get().metricRelationships.map(rel => ({
        ...rel,
        created_at: rel.created_at && typeof rel.created_at === 'string' 
          ? (rel.created_at.includes('GMT') 
              ? new Date(rel.created_at).toISOString() 
              : rel.created_at)
          : new Date().toISOString(),
        updated_at: rel.updated_at && typeof rel.updated_at === 'string'
          ? (rel.updated_at.includes('GMT')
              ? new Date(rel.updated_at).toISOString()
              : rel.updated_at)
          : new Date().toISOString()
      }));

      // Clean the project data to remove circular references and React internals
      const cleanProjectData = cleanDataForSerialization({
        ...currentProject,
        // Ensure required fields are present
        catalog_name: currentProject.catalog_name || get().catalogName || 'default',
        schema_name: currentProject.schema_name || get().schemaName || 'default',
        tables: transformedTables,
        relationships: get().relationships,
        metric_views: get().metricViews,
        traditional_views: get().traditionalViews, // Include traditional views in save
        metric_relationships: transformedMetricRelationships,
        created_at: (currentProject.created_at && typeof currentProject.created_at === 'string' &&
                     currentProject.created_at.match(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/))
                     ? currentProject.created_at
                     : new Date().toISOString(), // Ensure valid ISO datetime format
        updated_at: new Date().toISOString()
      });

      const projectData = {
        name: name || currentProject.name,
        format,
        overwrite,
        project: cleanProjectData
      };

      console.log('Saving project data:', projectData);
      console.log('Transformed tables:', transformedTables);

      // Final cleaning pass to ensure no circular references
      const finalCleanedData = cleanDataForSerialization(projectData);
      
      console.log('Final cleaned data:', finalCleanedData);

      const response = await axios.post(`${BACKEND_HOST}/api/data_modeling/project/save`, finalCleanedData);

      // Update the current project state with the new name and saved data
      const savedProject = response.data.project || projectData.project;
      set({
        currentProject: {
          ...savedProject,
          name: name || currentProject.name
        },
        projectName: name || currentProject.name
      });

      console.log('Project saved successfully. Updated project name to:', name || currentProject.name);

      return response.data;
    } catch (error) {
      console.error('Error saving project:', error);
      const errorMessage = error.response?.data?.error ||
        error.response?.data?.validation_errors ||
        error.message ||
        'Unknown error occurred while saving project';

      console.error('Save project error details:', error.response?.data);

      set({
        errors: { ...get().errors, saveProject: errorMessage },
        currentAlertDialogErrorType: "saveProject",
        displayAlertDialog: true
      });
      throw error;
    }
  },

  loadProject: async (projectName) => {
    try {
      set({
        errors: { ...get().errors, loadProject: null }
      });
      
      const response = await axios.get(`${BACKEND_HOST}/api/data_modeling/project/load/${projectName}`);
      const savedData = response.data;
      let project = savedData.project;
      
      console.log('🔧 LoadProject: Raw response data:', savedData);
      console.log('🔧 LoadProject: Project name in response:', project?.name);

      // Ensure datetime fields are valid ISO format
      project = {
        ...project,
        created_at: (project.created_at && typeof project.created_at === 'string' &&
                    project.created_at.match(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/))
                    ? project.created_at
                    : new Date().toISOString(),
        updated_at: (project.updated_at && typeof project.updated_at === 'string' &&
                    project.updated_at.match(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/))
                    ? project.updated_at
                    : new Date().toISOString(),
        tables: (project.tables || []).map(table => ({
          ...table,
          created_at: (table.created_at && typeof table.created_at === 'string' &&
                      table.created_at.match(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/))
                      ? table.created_at
                      : new Date().toISOString(),
          updated_at: (table.updated_at && typeof table.updated_at === 'string' &&
                      table.updated_at.match(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/))
                      ? table.updated_at
                      : new Date().toISOString(),
          // Keep foreign_key_reference in object format for proper FK inheritance detection
          fields: (table.fields || []).map(field => {
            let transformedField = { ...field };
            
            // Convert string type_parameters to object format for DECIMAL fields
            if (field.data_type === 'DECIMAL' && typeof field.type_parameters === 'string') {
              const params = field.type_parameters;
              if (params && params.trim()) {
                const parts = params.split(',');
                if (parts.length >= 1) {
                  const precision = parseInt(parts[0].trim());
                  const scale = parts.length > 1 ? parseInt(parts[1].trim()) : 0;
                  transformedField.type_parameters = {
                    precision: isNaN(precision) ? 10 : precision,
                    scale: isNaN(scale) ? 0 : scale
                  };
                }
              }
            }
            
            // Convert legacy string foreign_key_reference to object format if needed
            if (field.is_foreign_key && field.foreign_key_reference && typeof field.foreign_key_reference === 'string') {
              if (field.foreign_key_reference.includes('.')) {
                const [referencedTableName, referencedFieldName] = field.foreign_key_reference.split('.');
                const referencedTable = (project.tables || []).find(t => t.name === referencedTableName);
                const referencedField = referencedTable?.fields?.find(f => f.name === referencedFieldName);
                
                if (referencedTable && referencedField) {
                  transformedField.foreign_key_reference = {
                    referenced_table_id: referencedTable.id,
                    referenced_field_id: referencedField.id,
                    constraint_name: `fk_${table.name}_${referencedTableName}`,
                    on_delete: "NO ACTION",
                    on_update: "NO ACTION"
                  };
                  console.log('🔄 Converted string FK reference to object:', 
                    field.foreign_key_reference, '→', transformedField.foreign_key_reference);
                } else {
                  console.warn('⚠️ Could not resolve string FK reference:', field.foreign_key_reference);
                  transformedField.foreign_key_reference = null;
                }
              }
            }
            // If it's already an object, keep it as-is
            
            return transformedField;
          })
        })),
        metric_views: Array.isArray(project.metric_views) ? project.metric_views.map(metricView => ({
          ...metricView,
          created_at: (metricView.created_at && typeof metricView.created_at === 'string' &&
                      metricView.created_at.match(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/))
                      ? metricView.created_at
                      : new Date().toISOString(),
          updated_at: (metricView.updated_at && typeof metricView.updated_at === 'string' &&
                      metricView.updated_at.match(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/))
                      ? metricView.updated_at
                      : new Date().toISOString(),
          // Ensure dimensions and measures are arrays
          dimensions: Array.isArray(metricView.dimensions) ? metricView.dimensions : [],
          measures: Array.isArray(metricView.measures) ? metricView.measures : [],
          joins: Array.isArray(metricView.joins) ? metricView.joins : [],
          tags: metricView.tags && typeof metricView.tags === 'object' ? metricView.tags : {}
        })) : [],
        traditional_views: Array.isArray(project.traditional_views) ? project.traditional_views.map(traditionalView => ({
          ...traditionalView,
          created_at: (traditionalView.created_at && typeof traditionalView.created_at === 'string' &&
                      traditionalView.created_at.match(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/))
                      ? traditionalView.created_at
                      : new Date().toISOString(),
          updated_at: (traditionalView.updated_at && typeof traditionalView.updated_at === 'string' &&
                      traditionalView.updated_at.match(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/))
                      ? traditionalView.updated_at
                      : new Date().toISOString(),
          tags: traditionalView.tags && typeof traditionalView.tags === 'object' ? traditionalView.tags : {}
        })) : []
      };

      // Use the correct project name from the response (not from inside project object)
      const correctProjectName = savedData.name || project.name;
      
      console.log('🔧 LoadProject: Setting project state');
      console.log('🔧 Correct project name (savedData.name):', savedData.name);
      console.log('🔧 Project name from project object:', project.name);
      console.log('🔧 Using project name:', correctProjectName);
      
      // Update the project object with the correct name
      const updatedProject = {
        ...project,
        name: correctProjectName
      };
      
      set({
        currentProject: updatedProject,
        projectName: correctProjectName,
        catalogName: project.catalog_name,
        schemaName: project.schema_name,
        tables: project.tables,
        metricViews: Array.isArray(project.metric_views) ? project.metric_views : [],
        traditionalViews: Array.isArray(project.traditional_views) ? project.traditional_views : [],
        relationships: Array.isArray(project.relationships) ? project.relationships : [],
        metricRelationships: Array.isArray(project.metric_relationships) ? project.metric_relationships : []
      });
      
      console.log('🔧 LoadProject: State updated successfully');
      
      return savedData;
    } catch (error) {
      console.error('Error loading project:', error);
      set({
        errors: { ...get().errors, loadProject: error.response?.data || error.message },
        currentAlertDialogErrorType: "loadProject",
        displayAlertDialog: true
      });
      throw error;
    }
  },

  getSavedProjects: async () => {
    try {
      const response = await axios.get(`${BACKEND_HOST}/api/data_modeling/projects`);
      console.log('🔧 getSavedProjects response:', response.data);
      set({ savedProjects: response.data });
      return response.data;
    } catch (error) {
      console.error('Error getting saved projects:', error);
      set({ savedProjects: [] });
      return [];
    }
  },

  deleteProject: async (projectIdentifier) => {
    try {
      set({
        errors: { ...get().errors, deleteProject: null }
      });

      console.log('🔧 Deleting project with identifier:', projectIdentifier);
      const response = await axios.delete(`${BACKEND_HOST}/api/data_modeling/project/${projectIdentifier}`);
      
      // Refresh the saved projects list
      await get().getSavedProjects();
      
      // If the deleted project is the current project, clear it
      const currentProject = get().currentProject;
      if (currentProject && (currentProject.id === projectIdentifier || currentProject.name === projectIdentifier)) {
        set({
          currentProject: null,
          projectName: '',
          tables: [],
          relationships: []
        });
      }
      
      return response.data;
    } catch (error) {
      console.error('Error deleting project:', error);
      const errorMessage = error.response?.data?.error || error.message || 'Failed to delete project';
      
      set({
        errors: { ...get().errors, deleteProject: errorMessage },
        currentAlertDialogErrorType: "deleteProject",
        displayAlertDialog: true
      });
      throw error;
    }
  },

  downloadProject: async (projectIdentifier) => {
    try {
      set({
        errors: { ...get().errors, downloadProject: null }
      });

      console.log('🔧 Downloading project with identifier:', projectIdentifier);
      const response = await axios.get(`${BACKEND_HOST}/api/data_modeling/project/${projectIdentifier}/download`, {
        responseType: 'blob'
      });
      
      // Create download link
      const blob = new Blob([response.data], { type: 'application/x-yaml' });
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      
      // Get filename from Content-Disposition header or use default
      const contentDisposition = response.headers['content-disposition'];
      let filename = `${projectIdentifier}.yaml`;
      if (contentDisposition) {
        const filenameMatch = contentDisposition.match(/filename="(.+)"/);
        if (filenameMatch) {
          filename = filenameMatch[1];
        }
      }
      
      link.setAttribute('download', filename);
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      window.URL.revokeObjectURL(url);
      
      return response.data;
    } catch (error) {
      console.error('Error downloading project:', error);
      const errorMessage = error.response?.data?.error || error.message || 'Failed to download project';
      
      set({
        errors: { ...get().errors, downloadProject: errorMessage },
        currentAlertDialogErrorType: "downloadProject",
        displayAlertDialog: true
      });
      throw error;
    }
  },

  importProject: async (content, projectName, overwrite = false, contentType = 'yaml') => {
    try {
      set({
        errors: { ...get().errors, importProject: null }
      });

      console.log('🔧 Importing project:', projectName, 'type:', contentType);
      
      // Ensure content is a clean string without any DOM references
      let cleanContent;
      if (typeof content === 'string') {
        cleanContent = content;
      } else if (content && typeof content.toString === 'function') {
        cleanContent = content.toString();
      } else {
        cleanContent = String(content);
      }
      
      // Additional safety check - remove any potential React fiber references
      if (typeof cleanContent === 'string' && cleanContent.includes('__reactFiber')) {
        console.warn('🔧 Detected React fiber references in content, attempting to clean...');
        // This shouldn't happen with file content, but just in case
        cleanContent = cleanContent.replace(/__reactFiber\$[^'"\s]*/g, '');
      }
      
      // Create a completely clean payload object to avoid any contamination
      const payload = {};
      
      // Ensure all values are clean primitives
      payload.project_name = String(projectName);
      payload.overwrite = Boolean(overwrite);
      
      // Add content based on type
      if (contentType === 'json') {
        payload.json_content = String(cleanContent);
      } else {
        payload.yaml_content = String(cleanContent);
      }
      
      // Payload is ready for sending
      
      const response = await axios.post(`${BACKEND_HOST}/api/data_modeling/project/import`, payload);
      
      // Refresh the saved projects list
      await get().getSavedProjects();
      
      return response.data;
    } catch (error) {
      console.error('Error importing project:', error);
      console.error('Error response status:', error.response?.status);
      console.error('Error response data:', error.response?.data);
      console.error('Error response headers:', error.response?.headers);
      
      let errorMessage;
      if (!error.response) {
        // Network error - no response received
        errorMessage = 'Network error: Unable to connect to the backend server. Please ensure the server is running on http://localhost:8080';
      } else {
        // Server responded with an error
        errorMessage = error.response?.data?.error || error.message || 'Failed to import project';
      }
      
      set({
        errors: { ...get().errors, importProject: errorMessage },
        currentAlertDialogErrorType: "importProject",
        displayAlertDialog: true
      });
      throw error;
    }
  },

  getAppState: async () => {
    try {
      const response = await axios.get(`${BACKEND_HOST}/api/data_modeling/app_state`);
      set({ appState: response.data });
      return response.data;
    } catch (error) {
      console.error('Error getting app state:', error);
      return get().appState;
    }
  },

  generateDDL: async (tableIds = []) => {
    try {
      set({
        errors: { ...get().errors, generateDDL: null }
      });
      
      const currentProject = get().currentProject;
      if (!currentProject) {
        throw new Error('No project available');
      }
      
      console.log('Generating DDL for project ID:', currentProject.id);
      console.log('Project data keys:', Object.keys(currentProject));
      console.log('Tables count:', get().tables.length);

      // Transform tables data to ensure proper types (same as saveProject)
      const transformedTables = transformTablesForBackend(get().tables);

      console.log('Project data being sent:', {
        project: {
          ...currentProject,
          catalog_name: currentProject.catalog_name || get().catalogName || 'default',
          schema_name: currentProject.schema_name || get().schemaName || 'default',
          tables: transformedTables,
          relationships: get().relationships,
          created_at: (currentProject.created_at && typeof currentProject.created_at === 'string' &&
                      currentProject.created_at.match(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/))
                      ? currentProject.created_at
                      : new Date().toISOString(),
          updated_at: new Date().toISOString()
        },
        table_ids: tableIds
      });

      // Helper function to ensure proper datetime format (same as applyChanges)
      const ensureISODateTime = (dateValue) => {
        if (!dateValue) return new Date().toISOString();
        if (typeof dateValue === 'string') {
          // If it's already in ISO format, return as is
          if (dateValue.match(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/)) {
            return dateValue;
          }
          // Try to parse and convert to ISO
          try {
            return new Date(dateValue).toISOString();
          } catch (e) {
            return new Date().toISOString();
          }
        }
        if (dateValue instanceof Date) {
          return dateValue.toISOString();
        }
        return new Date().toISOString();
      };

      // Clean metric relationships datetime fields
      const cleanedMetricRelationships = (get().metricRelationships || []).map(rel => ({
        ...rel,
        created_at: ensureISODateTime(rel.created_at),
        updated_at: ensureISODateTime(rel.updated_at)
      }));

      // Clean metric views datetime fields
      const cleanedMetricViews = (get().metricViews || []).map(mv => ({
        ...mv,
        created_at: ensureISODateTime(mv.created_at),
        updated_at: ensureISODateTime(mv.updated_at)
      }));

      const response = await axios.post(`${BACKEND_HOST}/api/data_modeling/project/${currentProject.id}/generate_ddl`, {
        project: {
          ...currentProject,
          catalog_name: currentProject.catalog_name || get().catalogName || 'default',
          schema_name: currentProject.schema_name || get().schemaName || 'default',
          tables: transformedTables,
          relationships: get().relationships,
          metric_views: cleanedMetricViews,
          metric_relationships: cleanedMetricRelationships,
          created_at: ensureISODateTime(currentProject.created_at),
          updated_at: ensureISODateTime(currentProject.updated_at)
        },
        table_ids: tableIds
      });
      
      return response.data;
    } catch (error) {
      console.error('Error generating DDL:', error);
      
      // Enhanced error message for validation errors
      let errorMessage = error.message;
      if (error.response?.status === 422 && error.response?.data) {
        const data = error.response.data;
        if (data.validation_errors && data.validation_errors.length > 0) {
          errorMessage = `Validation Error: ${data.error}\n\nDetails:\n${data.validation_errors.map(err => `- ${err.loc?.join('.')}: ${err.msg}`).join('\n')}`;
        } else {
          errorMessage = data.error || errorMessage;
        }
      }
      
      set({
        errors: { ...get().errors, generateDDL: errorMessage },
        currentAlertDialogErrorType: "generateDDL",
        displayAlertDialog: true
      });
      throw error;
    }
  },

  applyChanges: async (tableIds = [], createOnly = true, warehouseId = null, sessionId = null) => {
    try {
      set({
        errors: { ...get().errors, applyChanges: null }
      });
      
      const currentProject = get().currentProject;
      if (!currentProject) {
        throw new Error('No project available');
      }
      
      console.log('Applying changes for project ID:', currentProject.id);
      console.log('Project data keys:', Object.keys(currentProject));
      console.log('Tables count:', get().tables.length);

      // Transform tables data to ensure proper types (same as saveProject)
      const transformedTables = transformTablesForBackend(get().tables);

      // Helper function to ensure proper datetime format
      const ensureISODateTime = (dateValue) => {
        if (!dateValue) return new Date().toISOString();
        if (typeof dateValue === 'string') {
          // If it's already in ISO format, return as is
          if (dateValue.match(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/)) {
            return dateValue;
          }
          // Try to parse and convert to ISO
          try {
            return new Date(dateValue).toISOString();
          } catch (e) {
            return new Date().toISOString();
          }
        }
        if (dateValue instanceof Date) {
          return dateValue.toISOString();
        }
        return new Date().toISOString();
      };

      // Clean metric relationships datetime fields
      const cleanedMetricRelationships = (get().metricRelationships || []).map(rel => ({
        ...rel,
        created_at: ensureISODateTime(rel.created_at),
        updated_at: ensureISODateTime(rel.updated_at)
      }));

      // Clean metric views datetime fields
      const cleanedMetricViews = (get().metricViews || []).map(mv => ({
        ...mv,
        created_at: ensureISODateTime(mv.created_at),
        updated_at: ensureISODateTime(mv.updated_at)
      }));

      const response = await axios.post(`${BACKEND_HOST}/api/data_modeling/project/${currentProject.id}/apply_changes`, {
        project: {
          ...currentProject,
          catalog_name: currentProject.catalog_name || get().catalogName || 'default',
          schema_name: currentProject.schema_name || get().schemaName || 'default',
          tables: transformedTables,
          relationships: get().relationships,
          metric_views: cleanedMetricViews,
          metric_relationships: cleanedMetricRelationships,
          created_at: ensureISODateTime(currentProject.created_at),
          updated_at: ensureISODateTime(currentProject.updated_at)
        },
        table_ids: tableIds,
        create_only: createOnly,
        warehouse_id: warehouseId,
        session_id: sessionId
      });
      
      return response.data;
    } catch (error) {
      console.error('Error applying changes:', error);
      
      // Enhanced error message for validation errors
      let errorMessage = error.message;
      if (error.response?.status === 422 && error.response?.data) {
        const data = error.response.data;
        if (data.validation_errors && data.validation_errors.length > 0) {
          errorMessage = `Validation Error: ${data.error}\n\nDetails:\n${data.validation_errors.map(err => `- ${err.loc?.join('.')}: ${err.msg}`).join('\n')}`;
        } else {
          errorMessage = data.error || errorMessage;
        }
      }
      
      set({
        errors: { ...get().errors, applyChanges: errorMessage },
        currentAlertDialogErrorType: "applyChanges",
        displayAlertDialog: true
      });
      throw error;
    }
  },

  // ===== METRIC VIEW ACTIONS =====

  // Metric View Dialog Management
  setShowMetricViewDialog: (show, metricView = null, sourceTableId = null) => {
    console.log('🎯 Setting metric view dialog:', { show, metricView: metricView?.name, sourceTableId });
    set({ 
      showMetricViewDialog: show,
      editingMetricView: metricView,
      selectedMetricViewForEditing: metricView,
      fieldMappingSourceTable: sourceTableId ? get().tables.find(t => t.id === sourceTableId) : null
    });
  },

  setShowFieldMappingDialog: (show, sourceTable = null, targetMetricView = null) => {
    console.log('🎯 Setting field mapping dialog:', { show, sourceTable: sourceTable?.name, targetMetricView: targetMetricView?.name });
    set({ 
      showFieldMappingDialog: show,
      fieldMappingSourceTable: sourceTable,
      fieldMappingTargetMetricView: targetMetricView
    });
  },

  // Metric View CRUD
  createMetricView: (metricViewData) => {
    console.log('📊 Creating metric view:', metricViewData);
    
    const newMetricView = {
      ...metricViewData,
      id: metricViewData.id || generateUUID(),
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString()
    };

    set(state => ({
      metricViews: [...state.metricViews, newMetricView],
      showMetricViewDialog: false,
      editingMetricView: null
    }));

    console.log('✅ Metric view created successfully:', newMetricView.name);
  },

  updateMetricView: (metricViewId, updates) => {
    console.log('📊 Updating metric view:', metricViewId, updates);
    
    set(state => ({
      metricViews: state.metricViews.map(mv => 
        mv.id === metricViewId 
          ? { ...mv, ...updates, updated_at: new Date().toISOString() }
          : mv
      ),
      showMetricViewDialog: false,
      editingMetricView: null
    }));

    console.log('✅ Metric view updated successfully');
  },

  deleteMetricView: (metricViewId) => {
    console.log('🗑️ Deleting metric view:', metricViewId);
    
    set(state => ({
      metricViews: state.metricViews.filter(mv => mv.id !== metricViewId),
      metricRelationships: state.metricRelationships.filter(rel => rel.metric_view_id !== metricViewId),
      selectedMetricViews: state.selectedMetricViews.filter(id => id !== metricViewId)
    }));

    console.log('✅ Metric view deleted successfully');
  },

  // Metric View Selection
  setSelectedMetricViews: (metricViewIds) => {
    console.log('🎯 Setting selected metric views:', metricViewIds);
    set({ selectedMetricViews: metricViewIds });
  },

  // Metric View Positioning (for ERD Canvas)
  updateMetricViewPosition: (metricViewId, x, y) => {
    set(state => ({
      metricViews: state.metricViews.map(mv => 
        mv.id === metricViewId 
          ? { ...mv, position_x: x, position_y: y }
          : mv
      )
    }));
  },

  updateMetricViewSize: (metricViewId, width, height) => {
    console.log('📏 Updating metric view size:', metricViewId, { width, height });
    
    set(state => ({
      metricViews: state.metricViews.map(mv => 
        mv.id === metricViewId 
          ? { ...mv, width, height }
          : mv
      )
    }));
  },

  // Metric Relationships
  createMetricRelationship: (sourceTableId, metricViewId, fieldMappings = []) => {
    console.log('🔗 Creating metric relationship:', { sourceTableId, metricViewId, fieldMappings });
    
    const newRelationship = {
      id: generateUUID(),
      source_table_id: sourceTableId,
      metric_view_id: metricViewId,
      relationship_type: 'source_to_metric',
      field_mappings: fieldMappings,
      line_points: [],
      created_at: new Date().toISOString()
    };

    set(state => ({
      metricRelationships: [...state.metricRelationships, newRelationship]
    }));

    console.log('✅ Metric relationship created successfully');
    return newRelationship;
  },

  deleteMetricRelationship: (relationshipId) => {
    console.log('🗑️ Deleting metric relationship:', relationshipId);
    
    set(state => ({
      metricRelationships: state.metricRelationships.filter(rel => rel.id !== relationshipId)
    }));

    console.log('✅ Metric relationship deleted successfully');
  },

  // Project Loading with Metric Views
  loadProjectWithMetricViews: (projectData) => {
    console.log('📂 Loading project with metric views and traditional views:', projectData);
    
    set({
      currentProject: projectData,
      projectName: projectData.name || '',
      catalogName: projectData.catalog_name || '',
      schemaName: projectData.schema_name || '',
      tables: projectData.tables || [],
      metricViews: projectData.metric_views || [],
      traditionalViews: projectData.traditional_views || [],
      relationships: projectData.relationships || [],
      metricRelationships: projectData.metric_relationships || []
    });

    console.log('✅ Project loaded with metric views');
  },

  // Save Project with Metric Views
  saveProjectWithMetricViews: async () => {
    try {
      const state = get();
      const currentProject = state.currentProject;
      
      if (!currentProject) {
        throw new Error('No current project to save');
      }

      console.log('💾 Saving project with metric views...');

      const projectData = {
        ...currentProject,
        tables: state.tables,
        metric_views: state.metricViews,
        relationships: state.relationships,
        metric_relationships: state.metricRelationships,
        updated_at: new Date().toISOString()
      };

      const response = await axios.put(
        `${BACKEND_HOST}/api/data_modeling/project/${currentProject.id}`,
        projectData
      );

      set({ currentProject: response.data });
      console.log('✅ Project saved with metric views successfully');
      
      return response.data;
    } catch (error) {
      console.error('❌ Error saving project with metric views:', error);
      throw error;
    }
  },

  // Apply metric view to Databricks
  applyMetricView: async (metricViewId, createOnly = false, warehouseId = null) => {
    const state = get();
    const { currentProject } = state;
    
    if (!currentProject) {
      throw new Error('No project loaded');
    }

    try {
      console.log('🚀 Applying metric view to Databricks:', metricViewId);
      
      // Ensure we have all required fields with proper fallbacks
      const projectName = currentProject.name || state.projectName || 'Untitled Project';
      const catalogName = currentProject.catalog_name || state.catalogName || 'default';
      const schemaName = currentProject.schema_name || state.schemaName || 'default';
      
      console.log('📋 Project data for apply:', { 
        name: projectName, 
        catalog_name: catalogName, 
        schema_name: schemaName,
        currentProject_catalog: currentProject.catalog_name,
        state_catalog: state.catalogName,
        currentProject_schema: currentProject.schema_name,
        state_schema: state.schemaName
      });
      
      // Helper function to ensure proper datetime format
      const ensureISODateTime = (dateValue) => {
        if (!dateValue) return new Date().toISOString();
        if (typeof dateValue === 'string') {
          // If it's already in ISO format, return as is
          if (dateValue.match(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/)) {
            return dateValue;
          }
          // Try to parse and convert to ISO
          try {
            return new Date(dateValue).toISOString();
          } catch (e) {
            return new Date().toISOString();
          }
        }
        if (dateValue instanceof Date) {
          return dateValue.toISOString();
        }
        return new Date().toISOString();
      };

      // Clean metric relationships datetime fields
      const cleanedMetricRelationships = (currentProject.metric_relationships || state.metricRelationships || []).map(rel => ({
        ...rel,
        created_at: ensureISODateTime(rel.created_at),
        updated_at: ensureISODateTime(rel.updated_at)
      }));

      // Transform tables data to ensure proper types (same as saveProject)
      const transformedTables = transformTablesForBackend(currentProject.tables || state.tables || []);
      
      // Send project data in request body (same as table apply)
      const requestData = {
        project: {
          ...currentProject,
          name: projectName,
          catalog_name: catalogName,
          schema_name: schemaName,
          tables: transformedTables,
          metric_views: currentProject.metric_views || state.metricViews || [],
          traditional_views: currentProject.traditional_views || state.traditionalViews || [],
          relationships: currentProject.relationships || state.relationships || [],
          metric_relationships: cleanedMetricRelationships,
          version: currentProject.version || '1.0',
          created_at: ensureISODateTime(currentProject.created_at),
          updated_at: ensureISODateTime(currentProject.updated_at)
        },
        create_only: createOnly,
        warehouse_id: warehouseId
      };

      const response = await axios.post(
        `${BACKEND_HOST}/api/data_modeling/project/${currentProject.id}/metric_views/${metricViewId}/apply`,
        requestData
      );

      console.log('✅ Metric view applied successfully:', response.data);
      return response.data;
    } catch (error) {
      console.error('❌ Error applying metric view:', error);
      throw error;
    }
  },

  // ===== TRADITIONAL VIEW ACTIONS =====

  // Traditional View Dialog Management
  setShowTraditionalViewDialog: (show, traditionalView = null) => {
    console.log('🎯 Setting traditional view dialog:', { show, traditionalView: traditionalView?.name });
    set({ 
      showTraditionalViewDialog: show,
      editingTraditionalView: traditionalView,
      selectedTraditionalViewForEditing: traditionalView
    });
  },

  // Traditional View CRUD
  createTraditionalView: (traditionalViewData) => {
    console.log('👁️ Creating traditional view:', traditionalViewData);
    
    const newTraditionalView = {
      ...traditionalViewData,
      id: traditionalViewData.id || generateUUID(),
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString()
    };

    set(state => ({
      traditionalViews: [...state.traditionalViews, newTraditionalView],
      showTraditionalViewDialog: false,
      editingTraditionalView: null
    }));

    console.log('✅ Traditional view created successfully:', newTraditionalView.name);
    return newTraditionalView;
  },

  updateTraditionalView: (traditionalViewId, updates) => {
    console.log('👁️ Updating traditional view:', traditionalViewId, updates);
    
    set(state => ({
      traditionalViews: state.traditionalViews.map(tv => 
        tv.id === traditionalViewId 
          ? { ...tv, ...updates, updated_at: new Date().toISOString() }
          : tv
      ),
      showTraditionalViewDialog: false,
      editingTraditionalView: null
    }));

    console.log('✅ Traditional view updated successfully');
  },

  deleteTraditionalView: (traditionalViewId) => {
    console.log('🗑️ Deleting traditional view:', traditionalViewId);
    
    set(state => ({
      traditionalViews: state.traditionalViews.filter(tv => tv.id !== traditionalViewId),
      selectedTraditionalViews: state.selectedTraditionalViews.filter(id => id !== traditionalViewId)
    }));

    console.log('✅ Traditional view deleted successfully');
  },

  // Traditional View Selection
  setSelectedTraditionalViews: (traditionalViewIds) => {
    console.log('🎯 Setting selected traditional views:', traditionalViewIds);
    set({ selectedTraditionalViews: traditionalViewIds });
  },

  // Traditional View Positioning (for ERD Canvas)
  updateTraditionalViewPosition: (traditionalViewId, x, y) => {
    set(state => ({
      traditionalViews: state.traditionalViews.map(tv => 
        tv.id === traditionalViewId 
          ? { ...tv, position_x: x, position_y: y }
          : tv
      )
    }));
  },

  updateTraditionalViewSize: (traditionalViewId, width, height) => {
    console.log('📏 Updating traditional view size:', traditionalViewId, { width, height });
    
    set(state => ({
      traditionalViews: state.traditionalViews.map(tv => 
        tv.id === traditionalViewId 
          ? { ...tv, width, height }
          : tv
      )
    }));
  },

  // Apply traditional view to Databricks
  applyTraditionalView: async (traditionalViewId, createOnly = false, warehouseId = null) => {
    const state = get();
    const { currentProject } = state;
    
    if (!currentProject) {
      throw new Error('No project loaded');
    }

    try {
      console.log('🚀 Applying traditional view to Databricks:', traditionalViewId);
      
      // Ensure we have all required fields with proper fallbacks
      const projectName = currentProject.name || state.projectName || 'Untitled Project';
      const catalogName = currentProject.catalog_name || state.catalogName || 'default';
      const schemaName = currentProject.schema_name || state.schemaName || 'default';
      
      console.log('📋 Project data for apply:', { 
        name: projectName, 
        catalog_name: catalogName, 
        schema_name: schemaName
      });
      
      // Helper function to ensure proper datetime format
      const ensureISODateTime = (dateValue) => {
        if (!dateValue) return new Date().toISOString();
        if (typeof dateValue === 'string') {
          // If it's already in ISO format, return as is
          if (dateValue.includes('T') && dateValue.includes('Z')) {
            return dateValue;
          }
          // Try to parse and convert to ISO
          const parsed = new Date(dateValue);
          if (!isNaN(parsed.getTime())) {
            return parsed.toISOString();
          }
        }
        return new Date().toISOString();
      };

        // Transform tables data to ensure proper types (same as saveProject)
        const transformedTables = transformTablesForBackend(currentProject.tables || state.tables || []);
        
        // Transform metric relationships to ensure proper date formats (same as saveProject)
        const transformedMetricRelationships = (currentProject.metric_relationships || state.metricRelationships || []).map(rel => ({
          ...rel,
          created_at: rel.created_at && typeof rel.created_at === 'string' 
            ? (rel.created_at.includes('GMT') 
                ? new Date(rel.created_at).toISOString() 
                : rel.created_at)
            : new Date().toISOString(),
          updated_at: rel.updated_at && typeof rel.updated_at === 'string'
            ? (rel.updated_at.includes('GMT')
                ? new Date(rel.updated_at).toISOString()
                : rel.updated_at)
            : new Date().toISOString()
        }));
        
        // Send project data in request body
        const requestData = {
          project: {
            ...currentProject,
            name: projectName,
            catalog_name: catalogName,
            schema_name: schemaName,
            tables: transformedTables,
            metric_views: currentProject.metric_views || state.metricViews || [],
            traditional_views: state.traditionalViews || currentProject.traditional_views || [],
            relationships: currentProject.relationships || state.relationships || [],
            metric_relationships: transformedMetricRelationships,
            version: currentProject.version || '1.0',
            created_at: ensureISODateTime(currentProject.created_at),
            updated_at: ensureISODateTime(currentProject.updated_at)
          },
          create_only: createOnly,
          warehouse_id: warehouseId
        };

      const response = await axios.post(
        `${BACKEND_HOST}/api/data_modeling/project/${currentProject.id}/traditional_views/${traditionalViewId}/apply`,
        requestData
      );

      console.log('✅ Traditional view applied successfully:', response.data);
      return response.data;
    } catch (error) {
      console.error('❌ Error applying traditional view:', error);
      throw error;
    }
  },

  // Generate DDL for traditional view
  generateTraditionalViewDDL: async (traditionalViewId) => {
    const state = get();
    const { currentProject } = state;
    
    if (!currentProject) {
      throw new Error('No project loaded');
    }

    try {
      console.log('🔧 Generating DDL for traditional view:', traditionalViewId);
      
      // Ensure we have all required fields with proper fallbacks
      const projectName = currentProject.name || state.projectName || 'Untitled Project';
      const catalogName = currentProject.catalog_name || state.catalogName || 'default';
      const schemaName = currentProject.schema_name || state.schemaName || 'default';
      
      // Helper function to ensure proper datetime format
      const ensureISODateTime = (dateValue) => {
        if (!dateValue) return new Date().toISOString();
        if (typeof dateValue === 'string') {
          // If it's already in ISO format, return as is
          if (dateValue.includes('T') && dateValue.includes('Z')) {
            return dateValue;
          }
          // Try to parse and convert to ISO
          const parsed = new Date(dateValue);
          if (!isNaN(parsed.getTime())) {
            return parsed.toISOString();
          }
        }
        return new Date().toISOString();
      };

      // Transform tables data to ensure proper types (same as saveProject)
      const transformedTables = transformTablesForBackend(currentProject.tables || state.tables || []);
      
      // Transform metric relationships to ensure proper date formats (same as saveProject)
      const transformedMetricRelationships = (currentProject.metric_relationships || state.metricRelationships || []).map(rel => ({
        ...rel,
        created_at: rel.created_at && typeof rel.created_at === 'string' 
          ? (rel.created_at.includes('GMT') 
              ? new Date(rel.created_at).toISOString() 
              : rel.created_at)
          : new Date().toISOString(),
        updated_at: rel.updated_at && typeof rel.updated_at === 'string'
          ? (rel.updated_at.includes('GMT')
              ? new Date(rel.updated_at).toISOString()
              : rel.updated_at)
          : new Date().toISOString()
      }));
      
      // Send project data in request body
      const requestData = {
        project: {
          ...currentProject,
          name: projectName,
          catalog_name: catalogName,
          schema_name: schemaName,
          tables: transformedTables,
          metric_views: currentProject.metric_views || state.metricViews || [],
          traditional_views: state.traditionalViews || currentProject.traditional_views || [],
          relationships: currentProject.relationships || state.relationships || [],
          metric_relationships: transformedMetricRelationships,
          version: currentProject.version || '1.0',
          created_at: ensureISODateTime(currentProject.created_at),
          updated_at: ensureISODateTime(currentProject.updated_at)
        }
      };

      const response = await axios.post(
        `${BACKEND_HOST}/api/data_modeling/project/${currentProject.id}/traditional_views/${traditionalViewId}/generate_ddl`,
        requestData
      );

      console.log('✅ Traditional view DDL generated successfully:', response.data);
      return response.data;
    } catch (error) {
      console.error('❌ Error generating traditional view DDL:', error);
      throw error;
    }
  },

  toggleAlertDialog: () => {
    set({ displayAlertDialog: !get().displayAlertDialog });
  },

}));

export default useDataModelingStore;
