#!/bin/bash

# Bricks Data Modeler - Databricks App Deployment Script
# This script helps deploy the Bricks Data Modeler to Databricks Apps using Databricks Bundles

set -e

APP_NAME="bricks-data-modeler"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "🚀 Bricks Data Modeler - Databricks App Deployment"
echo "=================================================="

# Check if Databricks CLI is installed
if ! command -v databricks &> /dev/null; then
    echo "❌ Databricks CLI not found. Please install it first:"
    echo "   curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh"
    exit 1
fi

# Check CLI version
CLI_VERSION=$(databricks --version 2>&1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1)
echo "📋 Databricks CLI version: $CLI_VERSION"

# Check if authenticated
if ! databricks auth describe &> /dev/null; then
    echo "❌ Not authenticated with Databricks. Please run:"
    echo "   databricks configure --host <your-workspace-url>"
    exit 1
fi

WORKSPACE_HOST=$(databricks auth describe | grep -oE 'https://[^[:space:]]+' | head -1)
echo "🏢 Workspace: $WORKSPACE_HOST"

# Navigate to app directory
cd "$SCRIPT_DIR"

# Check if databricks.yml exists
if [ ! -f "databricks.yml" ]; then
    echo "❌ databricks.yml not found in current directory"
    exit 1
fi

echo "📋 Found databricks.yml configuration"

# Validate bundle configuration
echo "🔍 Validating bundle configuration..."

# Handle Terraform download permission issues
if [ ! -d ".databricks" ]; then
    echo "🔧 Creating databricks bundle directory..."
    mkdir -p .databricks/bundle/default/bin
    chmod -R 755 .databricks/
fi

# Try validation with automatic retry for Terraform download issues
validate_attempts=0
max_validate_attempts=3

while [ $validate_attempts -lt $max_validate_attempts ]; do
    validate_attempts=$((validate_attempts + 1))
    echo "🔄 Bundle validation attempt $validate_attempts/$max_validate_attempts"
    
    if databricks bundle validate --target default 2>/dev/null; then
        echo "✅ Bundle configuration is valid"
        break
    else
        echo "⚠️  Bundle validation failed on attempt $validate_attempts"
        
        if [ $validate_attempts -lt $max_validate_attempts ]; then
            echo "🧹 Cleaning bundle state and fixing permissions..."
            rm -rf .databricks/bundle/default/bin/ 2>/dev/null || true
            mkdir -p .databricks/bundle/default/bin
            chmod -R 755 .databricks/
            sleep 2
        else
            echo "❌ Bundle validation failed after $max_validate_attempts attempts"
            echo "🔧 Showing detailed error:"
            databricks bundle validate --target default
            exit 1
        fi
    fi
done

# Check if bundle is already deployed
BUNDLE_EXISTS=false
if databricks bundle status &> /dev/null; then
    BUNDLE_EXISTS=true
    echo "📦 Bundle already exists in workspace"
else
    echo "📦 Bundle not yet deployed - will create new deployment"
fi

# Ask for confirmation
echo ""
if [ "$BUNDLE_EXISTS" = true ]; then
    echo "⚠️  This will update the existing bundle deployment. Continue? (y/N)"
else
    echo "🆕 This will create a new bundle deployment. Continue? (y/N)"
fi

read -r CONFIRM
if [[ ! "$CONFIRM" =~ ^[Yy]$ ]]; then
    echo "❌ Deployment cancelled"
    exit 0
fi

echo ""
echo "🔧 Starting bundle deployment..."


# Proactive state check and cleanup before deployment
echo "🔍 Performing proactive state check and cleanup..."

# Function to prepare deployment state (without deleting existing app)
prepare_deployment_state() {
    echo "🔍 Checking deployment state..."
    
    # Check if app exists in Databricks
    if databricks apps get "$APP_NAME" &> /dev/null; then
        echo "✅ App exists - will deploy to existing app"
        APP_EXISTS=true
        
        # Check app state
        APP_INFO=$(databricks apps get "$APP_NAME" 2>/dev/null)
        if echo "$APP_INFO" | grep -q '"compute_status".*"state":\s*"STARTING"'; then
            echo "⏳ App compute is starting, waiting for it to stabilize..."
            
            # Wait for app to reach stable state
            wait_time=0
            max_wait=180  # 3 minutes
            while [ $wait_time -lt $max_wait ]; do
                sleep 15
                wait_time=$((wait_time + 15))
                
                APP_INFO=$(databricks apps get "$APP_NAME" 2>/dev/null)
                if echo "$APP_INFO" | grep -q '"compute_status".*"state":\s*"ACTIVE"'; then
                    echo "✅ App compute is now ACTIVE"
                    break
                elif echo "$APP_INFO" | grep -q '"compute_status".*"state":\s*"STOPPED"'; then
                    echo "✅ App compute is STOPPED"
                    break
                fi
                
                echo "⏳ Still waiting for compute to stabilize... (${wait_time}s/${max_wait}s)"
            done
        fi
    else
        echo "📱 App doesn't exist - will create new app"
        APP_EXISTS=false
    fi
    
    # Clean only local state (keep workspace state if app exists)
    echo "🧹 Cleaning local bundle state..."
    rm -rf .databricks/ 2>/dev/null || true
    
    # Only clean workspace state if app doesn't exist
    if [ "$APP_EXISTS" = false ]; then
        echo "🧹 Cleaning workspace bundle state for fresh deployment..."
        USER_EMAIL=$(databricks auth describe | grep -oE '[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}' | head -1)
        if [ -n "$USER_EMAIL" ]; then
            databricks workspace delete "/Workspace/Users/$USER_EMAIL/.bundle/$APP_NAME" --recursive 2>/dev/null || true
        fi
    else
        echo "ℹ️  Keeping workspace state for existing app"
    fi
    
    echo "✅ Deployment state prepared"
}

# Prepare deployment state
prepare_deployment_state

# Deploy the bundle to existing or new app
echo "📦 Deploying bundle..."

# Smart deployment function that handles existing and new apps
deploy_with_prepared_state() {
    echo "🚀 Starting deployment..."
    
    # Create app if it doesn't exist (APP_EXISTS is set by prepare_deployment_state)
    if [ "$APP_EXISTS" = false ]; then
        echo "📱 Creating new app..."
        if databricks apps create "$APP_NAME" --description "Visual data modeling tool for Databricks Unity Catalog with ERD design, DDL generation, and direct table/view creation"; then
            echo "✅ App created successfully"
            sleep 10  # Give it time to initialize
        else
            echo "❌ Failed to create app"
            return 1
        fi
    else
        echo "✅ Using existing app"
    fi
    
    # Deploy bundle to the app
    echo "🚀 Deploying bundle..."
    
    # Try deployment first
    deploy_output=$(databricks bundle deploy --target default 2>&1)
    deploy_exit_code=$?
    
    if [ $deploy_exit_code -eq 0 ]; then
        echo "✅ Bundle deployed successfully"
        return 0
    else
        # Check if it's the "app already exists" error (handle potential line breaks)
        if echo "$deploy_output" | tr '\n' ' ' | grep -q "An app with the same name already exists"; then
            echo "🔍 Bundle deployment failed because app already exists but state is mismatched"
            echo "🧹 This indicates a state synchronization issue - will reset and recreate cleanly..."
            
            # Since bundle and app state are out of sync, reset everything and start fresh
            echo "⏸️  Stopping existing app..."
            databricks apps stop "$APP_NAME" 2>/dev/null || true
            sleep 5
            
            echo "🗑️  Deleting existing app to resolve state conflict..."
            if databricks apps delete "$APP_NAME" 2>/dev/null; then
                echo "✅ App deleted successfully"
                sleep 15
                
                # Verify deletion
                max_wait=60
                wait_time=0
                while [ $wait_time -lt $max_wait ]; do
                    if ! databricks apps get "$APP_NAME" &> /dev/null; then
                        echo "✅ App deletion confirmed"
                        break
                    fi
                    sleep 5
                    wait_time=$((wait_time + 5))
                    echo "⏳ Still waiting for deletion... (${wait_time}s/${max_wait}s)"
                done
            else
                echo "⚠️  Failed to delete app"
            fi
            
            # Clean all state
            echo "🧹 Cleaning all state for fresh deployment..."
            rm -rf .databricks/ 2>/dev/null || true
            
            USER_EMAIL=$(databricks auth describe | grep -oE '[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}' | head -1)
            if [ -n "$USER_EMAIL" ]; then
                databricks workspace delete "/Workspace/Users/$USER_EMAIL/.bundle/$APP_NAME" --recursive 2>/dev/null || true
            fi
            
            sleep 10
            
            # Try fresh deployment
            echo "🔄 Attempting fresh deployment with clean state..."
            if databricks bundle deploy --target default; then
                echo "✅ Bundle deployed successfully after reset"
                return 0
            else
                echo "❌ Bundle deployment failed even after complete reset"
                return 1
            fi
        else
            echo "❌ Bundle deployment failed with error:"
            echo "$deploy_output"
            return 1
        fi
    fi
}

# Execute deployment with prepared state
if ! deploy_with_prepared_state; then
    echo ""
    echo "❌ Bundle deployment failed after all retries"
    echo ""
    echo "🔧 Attempting final recovery with fresh deployment..."
    
    # Last resort: completely reset everything and use simple deployment
    echo "🧹 Performing complete reset..."
    rm -rf .databricks/ 2>/dev/null || true
    
    # Try a simple bundle deployment one more time
    echo "🔄 Final deployment attempt with clean slate..."
    if databricks bundle deploy --target default --force-lock 2>&1; then
        echo "✅ Final deployment attempt succeeded!"
    else
        echo ""
        echo "❌ All deployment attempts failed"
        echo ""
        echo "🔧 Manual troubleshooting steps:"
        echo "   1. Check if app exists: databricks apps get $APP_NAME"
        echo "   2. Try manual app creation: databricks apps create $APP_NAME --source-code-path ."
        echo "   3. Clean state manually: rm -rf .databricks/"
        echo "   4. Try deployment again: databricks bundle deploy --target default"
        echo "   5. Check bundle status: databricks bundle status"
        echo ""
        echo "💡 Alternative: Use direct app deployment:"
        echo "   databricks apps deploy $APP_NAME --source-code-path ."
        exit 1
    fi
fi

# Get deployment status
echo ""
echo "📋 Bundle Deployment Status:"
echo "============================"
databricks bundle status

echo ""
echo "⏳ Waiting for bundle deployment to complete before source deployment..."
echo "======================================================================="

# Wait for app to be ready for source deployment
wait_for_app_ready() {
    echo "🔍 Checking if app is ready for source deployment..."
    
    # Wait a bit for bundle deployment to complete
    echo "⏳ Waiting 15 seconds for bundle deployment to stabilize..."
    sleep 15
    
    # Check app status and wait for it to be ready
    local max_wait=60  # 1 minute - reduced from 5 minutes
    local wait_time=0
    
    while [ $wait_time -lt $max_wait ]; do
        if databricks apps get "$APP_NAME" &> /dev/null; then
            APP_INFO=$(databricks apps get "$APP_NAME" 2>/dev/null)
            
            # Check if app is in a deployable state
            if echo "$APP_INFO" | grep -A 10 '"compute_status"' | grep -q '"state":"ACTIVE"'; then
                echo "✅ App compute is ACTIVE, ready for source deployment"
                return 0
            elif echo "$APP_INFO" | grep -A 10 '"compute_status"' | grep -q '"state":"STOPPED"'; then
                echo "🔍 App compute is STOPPED, starting it for source deployment..."
                if databricks apps start "$APP_NAME"; then
                    echo "✅ App compute started successfully"
                    # Wait a bit for it to become active
                    echo "⏳ Waiting for app compute to become active..."
                    sleep 30
                    return 0
                else
                    echo "❌ Failed to start app compute"
                    return 1
                fi
            else
                echo "⏳ App is still initializing... waiting (${wait_time}s/${max_wait}s)"
                sleep 15
                wait_time=$((wait_time + 15))
            fi
        else
            echo "⚠️  App not found, waiting for it to appear... (${wait_time}s/${max_wait}s)"
            sleep 15
            wait_time=$((wait_time + 15))
        fi
    done
    
    echo "⚠️  App didn't reach ready state within ${max_wait}s, proceeding with source deployment..."
    return 0
}

# Wait for app to be ready
wait_for_app_ready

echo ""
echo "📦 Deploying source code to app..."
echo "=================================="

# Deploy source code to the app  
USER_EMAIL=$(databricks current-user me | grep '"userName"' | grep -oE '[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
if [ -z "$USER_EMAIL" ]; then
    # Fallback method - get from workspace CLI info
    USER_EMAIL=$(databricks auth describe | grep 'Username:' | awk '{print $2}')
fi
if [ -z "$USER_EMAIL" ]; then
    # Fallback method - hardcode based on the workspace we can see
    # Extract user email from Databricks CLI auth
    USER_EMAIL=$(databricks auth describe | grep -oE '[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}' | head -1)
fi

BUNDLE_FILES_PATH="/Workspace/Users/$USER_EMAIL/.bundle/$APP_NAME/default/files"
echo "🔍 Bundle source path: $BUNDLE_FILES_PATH"
echo "🔍 Using current directory as source: $(pwd)"

# Deploy with enhanced retry logic for "active deployment" errors
deploy_source_attempts=0
max_source_attempts=5  # Increased attempts for deployment timing issues

while [ $deploy_source_attempts -lt $max_source_attempts ]; do
    deploy_source_attempts=$((deploy_source_attempts + 1))
    echo "🔄 Source deployment attempt $deploy_source_attempts/$max_source_attempts"
    
    # Capture deployment output to check for specific errors
    # Try bundle path first, fallback to current directory
    deploy_output=$(databricks apps deploy "$APP_NAME" --source-code-path "$BUNDLE_FILES_PATH" 2>&1)
    deploy_exit_code=$?
    
    # Show deployment output for debugging
    if [ $deploy_exit_code -ne 0 ]; then
        echo "⚠️  Source deployment failed, error output:"
        echo "$deploy_output"
    fi
    
    if [ $deploy_exit_code -eq 0 ]; then
        echo "✅ Source code deployed successfully!"
        break
    else
        echo "⚠️  Source deployment attempt $deploy_source_attempts failed"
        
        # Check if it's an "active deployment in progress" error
        if echo "$deploy_output" | grep -q "active deployment in progress"; then
            echo "🔍 Detected 'active deployment in progress' - waiting for deployment to complete..."
            if [ $deploy_source_attempts -lt $max_source_attempts ]; then
                # Wait longer for active deployments
                wait_time=60
                echo "⏳ Waiting ${wait_time} seconds for active deployment to complete..."
                sleep $wait_time
            fi
        else
            echo "❌ Different error occurred:"
            echo "$deploy_output"
            if [ $deploy_source_attempts -lt $max_source_attempts ]; then
                echo "🔄 Waiting 15 seconds before retry..."
                sleep 15
            fi
        fi
        
        if [ $deploy_source_attempts -eq $max_source_attempts ]; then
            echo "❌ Source deployment failed after $max_source_attempts attempts"
            echo "🔧 Manual deployment command:"
            echo "   databricks apps deploy $APP_NAME --source-code-path \"$BUNDLE_FILES_PATH\""
            exit 1
        fi
    fi
done

# Try to get the app information via the deployed resources
echo ""
echo "📱 Final App Information:"
echo "========================"

# Check if app was created successfully
if databricks apps get "$APP_NAME" &> /dev/null 2>&1; then
    # Get app information
    APP_INFO=$(databricks apps get "$APP_NAME")
    echo "$APP_INFO" | grep -E "(name|status|url)" || echo "$APP_INFO"
    
    # Start the app if it's not running
    APP_STATUS=$(echo "$APP_INFO" | grep -oE '"status":\s*"[^"]+' | cut -d'"' -f4)
    if [ "$APP_STATUS" != "RUNNING" ]; then
        echo ""
        echo "▶️  Starting app..."
        databricks apps start "$APP_NAME"
        echo "✅ App started successfully"
    else
        echo "✅ App is already running"
    fi
    
    # Get app URL
    APP_URL=$(echo "$APP_INFO" | grep -oE 'https://[^[:space:]]+' | head -1)
    if [ -n "$APP_URL" ]; then
        echo ""
        echo "🌐 App URL: $APP_URL"
        echo ""
        echo "🎉 Deployment completed successfully!"
        echo "📝 You can now access the Bricks Data Modeler at the URL above"
    else
        echo ""
        echo "⚠️  Deployment completed but couldn't retrieve app URL"
        echo "📝 Check 'databricks apps list' to get the app URL"
    fi
else
    echo "⚠️  App deployment completed but app is not accessible yet"
    echo "📝 This might be normal for first-time deployments. Try again in a few minutes."
fi

echo ""
echo "📚 Useful commands:"
echo "  View logs:       databricks apps logs $APP_NAME --follow"
echo "  Check status:    databricks apps get $APP_NAME"
echo "  Bundle status:   databricks bundle status"
echo "  Restart app:     databricks apps restart $APP_NAME"
echo "  Stop app:        databricks apps stop $APP_NAME"
echo "  Redeploy bundle: databricks bundle deploy --target default"
