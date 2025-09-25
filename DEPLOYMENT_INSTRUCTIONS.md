# Databricks App Deployment Instructions

## Prerequisites

1. **Databricks CLI** installed and configured
   ```bash
   pip install databricks-cli
   databricks configure
   ```

2. **Databricks Bundle CLI** (for app deployment)
   ```bash
   curl -fsSL https://raw.githubusercontent.com/databricks/cli/main/install.sh | sh
   ```

3. **Workspace Access** with permissions to:
   - Create and manage apps
   - Access Unity Catalog
   - Create tables and views

## Deployment Steps

### 1. Configure Authentication

Ensure your Databricks workspace credentials are configured:
```bash
databricks auth login
```

### 2. Deploy the App

From the `databricks-app` directory:

```bash
./deploy.sh
```
or
```bash
databricks bundle deploy
```

### 3. Verify Deployment

Check that the app is deployed:
```bash
databricks bundle validate
```

### 4. Access the App

The app will be available in your Databricks workspace under the Apps section.

## Configuration

### Environment Variables

The app automatically detects the following Databricks environment variables:
- `DATABRICKS_SERVER_HOSTNAME` - Workspace URL
- `DATABRICKS_CLIENT_ID` - Service principal client ID (if using)
- `DATABRICKS_CLIENT_SECRET` - Service principal secret (if using)

### App Permissions

The app supports two authentication modes:
1. **User Authentication** - Uses the logged-in user's credentials (on-behalf-of)
2. **Service Principal** - Uses app-level service principal credentials

## Features

- **Visual ERD Designer** - Drag-and-drop interface for database design
- **Unity Catalog Integration** - Direct import from existing catalogs/schemas
- **DDL Generation** - Automatic SQL generation for tables and views
- **Metric Views** - Create and manage Databricks metric layer definitions
- **Cross-Catalog Support** - Work across multiple catalogs and schemas
- **Real-time Collaboration** - Multiple users can work on the same data model

## Troubleshooting

### Common Issues

1. **Authentication Errors**
   - Verify Databricks CLI is configured correctly
   - Check workspace permissions

2. **Import Errors**
   - Ensure Unity Catalog is enabled
   - Verify access to source catalogs/schemas

3. **App Not Loading**
   - Check app logs in Databricks workspace
   - Verify all dependencies are installed

### Support

For issues and support, check the main repository documentation.
