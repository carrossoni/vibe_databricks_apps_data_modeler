# Databricks App Permissions Setup

## 🔐 **OAuth Token Scope Issue - SOLVED!**

The error you're seeing:
```
Provided OAuth token does not have required scopes
```

This happens when the user's token doesn't have Unity Catalog access permissions. Here's how to fix it:

## **Solution 1: Grant Unity Catalog Permissions (Recommended)**

### For Workspace Admins:
1. **Grant Unity Catalog Access to Users**:
   ```sql
   -- Grant access to metastore
   GRANT USE CATALOG ON CATALOG main TO `<user@company.com>`;
   GRANT USE SCHEMA ON SCHEMA main.default TO `<user@company.com>`;
   ```

2. **Grant Broader Permissions** (if needed):
   ```sql
   -- Grant catalog creation (for advanced users)
   GRANT CREATE CATALOG ON METASTORE TO `<user@company.com>`;
   
   -- Grant schema creation
   GRANT CREATE SCHEMA ON CATALOG main TO `<user@company.com>`;
   
   -- Grant table creation
   GRANT CREATE TABLE ON SCHEMA main.default TO `<user@company.com>`;
   ```

### For User Account Configuration:
1. **Ensure Service Principal Has Permissions**:
   - Go to Databricks Admin Console
   - Navigate to Service Principals
   - Grant Unity Catalog permissions to your service principal

2. **Check User Token Scopes**:
   - User tokens need `sql` and `workspace` scopes
   - Personal Access Tokens should be created with appropriate permissions

## **Solution 2: Service Principal Authentication**

If you prefer to use service principal authentication instead of user tokens:

### Setup Service Principal:
1. **Create Service Principal**:
   ```bash
   # In Databricks workspace
   Settings > Admin Console > Service Principals > Add Service Principal
   ```

2. **Set Environment Variables**:
   ```bash
   export DATABRICKS_CLIENT_ID="<service-principal-client-id>"
   export DATABRICKS_CLIENT_SECRET="<service-principal-secret>"
   export DATABRICKS_HOST="<workspace-url>"
   ```

3. **Grant Permissions to Service Principal**:
   ```sql
   GRANT USE CATALOG ON CATALOG main TO `<service-principal-id>`;
   GRANT CREATE SCHEMA ON CATALOG main TO `<service-principal-id>`;
   GRANT CREATE TABLE ON SCHEMA main.default TO `<service-principal-id>`;
   ```

## **Solution 3: Demo Mode (No Permissions Needed)**

The app automatically falls back to demo mode when no valid credentials are available:

- **Demo Features**:
  - ✅ Visual ERD design
  - ✅ Data model creation
  - ✅ DDL generation
  - ❌ Unity Catalog import (mocked data)
  - ❌ Direct table creation

## **Code Fix Applied**

✅ **Already Fixed in Latest Deployment**: The authentication logic has been updated to properly handle permission issues and gracefully fall back to demo mode.

## **Testing the Fix**

1. **Redeploy the App**:
   ```bash
   cd databricks-app
   databricks bundle deploy
   ```

2. **Test Scenarios**:
   - **With Permissions**: Full Unity Catalog access
   - **Without Permissions**: Graceful fallback to demo mode
   - **Service Principal**: Alternative authentication method

## **Common Permission Levels**

### **Minimal (View Only)**:
```sql
GRANT USE CATALOG ON CATALOG main TO `<user>`;
GRANT USE SCHEMA ON SCHEMA main.default TO `<user>`;
GRANT SELECT ON SCHEMA main.default TO `<user>`;
```

### **Standard (Create Tables)**:
```sql
GRANT USE CATALOG ON CATALOG main TO `<user>`;
GRANT USE SCHEMA ON SCHEMA main.default TO `<user>`;
GRANT CREATE TABLE ON SCHEMA main.default TO `<user>`;
GRANT SELECT ON SCHEMA main.default TO `<user>`;
```

### **Advanced (Full Access)**:
```sql
GRANT USE CATALOG ON CATALOG main TO `<user>`;
GRANT CREATE SCHEMA ON CATALOG main TO `<user>`;
GRANT CREATE TABLE ON CATALOG main TO `<user>`;
GRANT CREATE FUNCTION ON CATALOG main TO `<user>`;
```

## **Verification**

After granting permissions, verify with:
```sql
SHOW GRANTS ON CATALOG main TO `<user>`;
```

The app should now work without OAuth scope errors! 🎉
