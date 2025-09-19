# Bricks Data Modeler - Databricks App

A visual data modeling tool for Databricks Unity Catalog with Entity Relationship Diagram (ERD) design, DDL generation, and direct table/view creation capabilities.

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.11+-blue?style=flat&logo=python&logoColor=white)
![React](https://img.shields.io/badge/React-18+-61DAFB?style=flat&logo=react&logoColor=black)
![Flask](https://img.shields.io/badge/Flask-2.3+-000?style=flat&logo=flask&logoColor=white)

## 🚀 Features

### **Visual ERD Design**
- **Drag & Drop Interface**: Intuitive canvas for designing database schemas
- **Real-time Relationship Mapping**: Automatic detection and visualization of foreign key relationships
- **Multi-Catalog Support**: Work across different Unity Catalog namespaces
- **Interactive Tables**: Click to edit table structures, fields, and properties

### **Advanced Data Modeling**
- **Table Management**: Create, modify, and delete tables with full DDL support
- **View Creation**: Support for both traditional and metric views
- **Relationship Management**: Visual foreign key constraint creation and editing
- **Tag Management**: Apply and manage Unity Catalog tags at table and column levels

### **Code Generation & Deployment**
- **DDL Generation**: Export complete SQL DDL for tables, views, and constraints
- **Direct Apply**: Deploy changes directly to Databricks Unity Catalog
- **Real-time Progress**: Live progress tracking during deployment operations
- **Error Handling**: Comprehensive error reporting and recovery mechanisms

### **Import & Export**
- **Existing Schema Import**: Import existing tables and views from Unity Catalog
- **Project Export**: Save and share data models as JSON/YAML files
- **Batch Operations**: Import multiple tables and automatically detect relationships

## 🏗️ Architecture

- **Frontend**: React 18+ with ReactFlow for visual canvas
- **Backend**: Flask with Databricks SDK for Unity Catalog integration
- **Deployment**: Databricks Apps with automated bundle deployment
- **Authentication**: User authorization (on-behalf-of) with OAuth scopes

## 📋 Prerequisites

Before deploying the Bricks Data Modeler, ensure you have:

1. **Databricks Workspace**: Access to a Databricks workspace with Unity Catalog enabled
2. **Databricks CLI**: Version 0.100+ installed and configured
3. **Permissions**: Ability to create and manage Databricks Apps
4. **Unity Catalog Access**: Permissions to read/write catalogs, schemas, and tables

### Required OAuth Scopes

The app requires the following OAuth scopes:

- `sql`: Execute SQL statements and manage SQL resources
- `catalog.tables:read`: Allows the app to read tables in Unity Catalog.
- `catalog.schemas:read`: Allows the app to read schemas in Unity Catalog.
- `catalog.catalogs:read`: Allows the app to read catalogs in Unity Catalog.
- `iam.current-user:read`: Read current user information (default)
- `iam.access-control:read`: Read access control information (default)

## 🚀 Quick Start

### 1. Clone the Repository

```bash
git clone <repository-url>
cd bricks-data-modeler/databricks-app
```

### 2. Configure Databricks CLI

```bash
# Configure CLI with your workspace
databricks configure --host https://your-workspace.cloud.databricks.com

# Verify authentication
databricks auth describe
```

### 3. Deploy the App

```bash
# Make deploy script executable
chmod +x deploy.sh

# Run interactive deployment
./deploy.sh
```

The deployment script will:
- Validate your Databricks configuration
- Create the Databricks App (if it doesn't exist)
- Deploy the source code using Databricks Bundles
- Configure proper permissions and scopes
- Start the app and provide the access URL

### 4. Access the Application

After successful deployment, the script will provide the app URL. Navigate to the URL in your browser to start using the Data Modeler.

## 🔧 Configuration

### Environment Variables

The app can be configured using the following environment variables:

- `FLASK_ENV`: Set to `production` for production deployment (default)
- `PYTHONPATH`: Python path configuration (default: `.:backend`)

### App Configuration Files

- `databricks.yml`: Databricks Bundle configuration
- `app.yaml`: Databricks App runtime configuration
- `requirements.txt`: Python dependencies


## 📖 Usage Guide

### Creating a New Data Model

1. **Start a New Project**: Click "New Project" and provide a name and description
2. **Select Target Catalog/Schema**: Choose where tables will be created
3. **Design Your Schema**: 
   - Use "Add Table" to create new tables
   - Add fields with appropriate data types
   - Set primary keys and configure relationships
4. **Apply Changes**: Use "Apply Changes" to deploy to Unity Catalog

### Importing Existing Tables

1. **Import Tables**: Click "Import Tables" and select your catalog/schema
2. **Select Tables**: Choose which tables to import
3. **Auto-Relationship Detection**: The tool automatically detects and creates visual relationships
4. **Modify as Needed**: Edit imported tables or add new relationships

### Working with Views

1. **Traditional Views**: Create SQL-based views with custom queries
2. **Metric Views**: Design analytical views with dimensions and measures
3. **Join Configuration**: Visual interface for configuring complex joins

### Generating DDL

1. **Individual Tables**: Click "Generate DDL" on any table
2. **Complete Schema**: Use "Generate All Tables DDL" for full schema export
3. **Copy or Download**: Copy to clipboard or download as SQL file

## 🛠️ Development

### Local Development Setup

```bash
# Install Python dependencies
pip install -r requirements.txt

# Install frontend dependencies (if building from source)
cd frontend
npm install
npm run build
cd ..

# Run locally (development mode)
python app.py
```

### Building Frontend Assets

If you need to rebuild the frontend:

```bash
cd frontend
npm run build
# Copy build assets to static/assets/
```

### Running Tests

```bash
# Run backend tests
python -m pytest backend/tests/

# Run linting
flake8 backend/
```

## 🚨 Troubleshooting

### Common Issues

#### Authentication Problems
- **Solution**: Ensure Databricks CLI is properly configured and you have the required permissions

#### App Won't Start
- **Check**: App compute status in Databricks workspace
- **Solution**: Use the deployment script's status check and restart functionality

#### Permission Errors
- **Check**: Unity Catalog permissions for target catalogs/schemas
- **Solution**: Request appropriate permissions from your workspace administrator

#### Import Failures
- **Check**: Table existence and accessibility
- **Solution**: Verify catalog/schema names and table permissions

### Debug Mode

To enable detailed logging, the app provides comprehensive logging in the Databricks Apps logs. Check the app logs for detailed error information.

### Getting Help

1. **Check Logs**: Always check Databricks Apps logs for detailed error information
2. **Verify Permissions**: Ensure you have appropriate Unity Catalog permissions
3. **Test Connection**: Use the app's connection test features
4. **Review Documentation**: Check Databricks Apps and Unity Catalog documentation

## 📝 License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📞 Support

No support :) But for questions and comments:
- Create an issue in the GitHub repository
- Check the Databricks Community forums
- Review Databricks Apps documentation

---

**Built with ❤️ for the Databricks Community**
