#!/bin/bash

# Bricks Data Modeler - Deployment Verification Script
# This script verifies that the app is ready for GitHub deployment

set -e

echo "🔍 Bricks Data Modeler - Deployment Verification"
echo "================================================"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Color codes for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

ERRORS=0
WARNINGS=0

# Function to check if file exists
check_file_exists() {
    local file="$1"
    local description="$2"
    
    if [ -f "$file" ]; then
        echo -e "✅ ${GREEN}$description${NC}: $file"
    else
        echo -e "❌ ${RED}$description${NC}: $file (MISSING)"
        ((ERRORS++))
    fi
}

# Function to check for personal information
check_no_personal_info() {
    local pattern="$1"
    local description="$2"
    
    local matches=$(grep -r -i "$pattern" . --exclude-dir=.git --exclude="*.log" --exclude="verify-deployment.sh" 2>/dev/null || true)
    
    if [ -z "$matches" ]; then
        echo -e "✅ ${GREEN}No $description found${NC}"
    else
        echo -e "⚠️ ${YELLOW}Found $description:${NC}"
        echo "$matches"
        ((WARNINGS++))
    fi
}

echo "📋 Checking required files..."
check_file_exists "README.md" "README file"
check_file_exists "LICENSE" "License file"
check_file_exists ".gitignore" "Git ignore file"
check_file_exists "databricks.yml" "Databricks bundle config"
check_file_exists "app.yaml" "App runtime config"
check_file_exists "requirements.txt" "Python requirements"
check_file_exists "deploy.sh" "Deployment script"
check_file_exists "app.py" "Main application file"
check_file_exists "backend/data_modeling_routes.py" "API routes"
check_file_exists "backend/databricks_integration.py" "Databricks integration"

echo ""
echo "🔍 Checking for personal information..."
check_no_personal_info "luiz\.carrossoni" "personal email references"
check_no_personal_info "carrossoni\." "personal catalog references" 
check_no_personal_info "@databricks\.com" "Databricks email addresses"

echo ""
echo "🔍 Checking for hardcoded tokens/secrets..."
check_no_personal_info "DATABRICKS_TOKEN" "hardcoded tokens"
check_no_personal_info "DATABRICKS_PAT" "hardcoded PATs"
check_no_personal_info "dapi[a-z0-9]" "API tokens"

echo ""
echo "📁 Checking file structure..."

# Check that unnecessary files are removed
unnecessary_files=("test_minimal.py" "app.yml" "static/databricks_integration.py")
for file in "${unnecessary_files[@]}"; do
    if [ -f "$file" ]; then
        echo -e "⚠️ ${YELLOW}Unnecessary file still present:${NC} $file"
        ((WARNINGS++))
    else
        echo -e "✅ ${GREEN}Unnecessary file removed:${NC} $file"
    fi
done

echo ""
echo "🔧 Checking deployment script..."

# Check that deploy.sh is executable
if [ -x "deploy.sh" ]; then
    echo -e "✅ ${GREEN}deploy.sh is executable${NC}"
else
    echo -e "❌ ${RED}deploy.sh is not executable${NC}"
    echo "   Run: chmod +x deploy.sh"
    ((ERRORS++))
fi

# Check that deploy.sh doesn't have hardcoded personal info
if grep -q "luiz.carrossoni@databricks.com" deploy.sh; then
    echo -e "❌ ${RED}deploy.sh contains hardcoded email${NC}"
    ((ERRORS++))
else
    echo -e "✅ ${GREEN}deploy.sh has no hardcoded email${NC}"
fi

echo ""
echo "📦 Checking Python dependencies..."

# Check that requirements.txt has necessary dependencies
required_deps=("flask" "databricks-sdk" "pydantic" "flask-cors")
missing_deps=()

for dep in "${required_deps[@]}"; do
    if grep -q "$dep" requirements.txt; then
        echo -e "✅ ${GREEN}Required dependency:${NC} $dep"
    else
        echo -e "❌ ${RED}Missing dependency:${NC} $dep"
        missing_deps+=("$dep")
        ((ERRORS++))
    fi
done

echo ""
echo "📊 Summary:"
echo "=========="

if [ $ERRORS -eq 0 ] && [ $WARNINGS -eq 0 ]; then
    echo -e "🎉 ${GREEN}PERFECT!${NC} The app is ready for GitHub deployment!"
    exit 0
elif [ $ERRORS -eq 0 ]; then
    echo -e "✅ ${GREEN}READY!${NC} The app can be deployed with ${WARNINGS} warning(s)."
    echo -e "   ${YELLOW}Review warnings above before deploying.${NC}"
    exit 0
else
    echo -e "❌ ${RED}NOT READY!${NC} Found ${ERRORS} error(s) and ${WARNINGS} warning(s)."
    echo -e "   ${RED}Fix errors before deploying to GitHub.${NC}"
    exit 1
fi

