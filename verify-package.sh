#!/bin/bash

echo "🔍 Verifying Databricks App Package..."
echo "================================================"

# Check required files
echo "📁 Checking required files..."
required_files=(
    "app.py"
    "app.yaml" 
    "databricks.yml"
    "requirements.txt"
    "static/index.html"
    "backend/data_modeling_routes.py"
    "backend/databricks_integration.py"
    "backend/models/data_modeling.py"
)

missing_files=()
for file in "${required_files[@]}"; do
    if [[ -f "$file" ]]; then
        echo "✅ $file"
    else
        echo "❌ $file (MISSING)"
        missing_files+=("$file")
    fi
done

echo ""

# Check static assets
echo "🎨 Checking frontend assets..."
if [[ -f "static/assets/index-B6otl_JQ.css" && -f "static/assets/index-Cz1RHMCm.js" ]]; then
    echo "✅ Frontend assets built"
else
    echo "❌ Frontend assets missing"
    missing_files+=("frontend-assets")
fi

echo ""

# Check Python imports
echo "🐍 Checking Python imports..."
python3 -c "
import sys
sys.path.insert(0, 'backend')
try:
    from data_modeling_routes import data_modeling_bp
    from databricks_integration import DatabricksUnityService  
    from models.data_modeling import DataModelProject
    print('✅ All Python imports successful')
except ImportError as e:
    print(f'❌ Import error: {e}')
    exit(1)
"

echo ""

# Summary
if [[ ${#missing_files[@]} -eq 0 ]]; then
    echo "🎉 Package verification PASSED!"
    echo "Ready for deployment to Databricks Apps"
    echo ""
    echo "To deploy:"
    echo "  databricks bundle deploy"
    exit 0
else
    echo "❌ Package verification FAILED!"
    echo "Missing files: ${missing_files[*]}"
    exit 1
fi
