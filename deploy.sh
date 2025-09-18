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

# Function to create app with robust error handling
create_app_if_needed() {
    echo "🔍 Checking if app exists..."
    
    # Check if app exists (with simple retry for network issues)
    if databricks apps get "$APP_NAME" &> /dev/null; then
        echo "✅ App already exists"
        
        # Check app state and wait if necessary
        echo "🔍 Checking app state..."
        APP_INFO=$(databricks apps get "$APP_NAME" 2>/dev/null)
        if echo "$APP_INFO" | grep -q '"state":\s*"STARTING"'; then
            echo "⏳ App compute is in STARTING state, waiting for it to become ACTIVE..."
            
            # Wait for app to reach stable state
            wait_time=0
            max_wait=300  # 5 minutes
            while [ $wait_time -lt $max_wait ]; do
                sleep 15
                wait_time=$((wait_time + 15))
                
                APP_INFO=$(databricks apps get "$APP_NAME" 2>/dev/null)
                if echo "$APP_INFO" | grep -q '"state":\s*"ACTIVE"'; then
                    echo "✅ App compute is now ACTIVE"
                    break
                elif echo "$APP_INFO" | grep -q '"state":\s*"STOPPED"'; then
                    echo "✅ App compute is now STOPPED"
                    break
                elif echo "$APP_INFO" | grep -q '"state":\s*"ERROR"'; then
                    echo "❌ App compute is in ERROR state, stopping it first..."
                    databricks apps stop "$APP_NAME" 2>/dev/null || true
                    sleep 10
                    break
                fi
                
                echo "⏳ Still waiting for compute... (${wait_time}s/${max_wait}s)"
            done
            
            if [ $wait_time -ge $max_wait ]; then
                echo "⚠️  App compute is taking too long to start, attempting to stop it..."
                databricks apps stop "$APP_NAME" 2>/dev/null || true
                sleep 10
            fi
        fi
        return 0
    else
        echo "📱 App does not exist, creating it first..."
        
        # Clean any stale state before creating
        echo "🧹 Cleaning any stale bundle state..."
        rm -rf .databricks/ 2>/dev/null || true
        
        # Create the app directly with retries
        echo "🚀 Creating app: $APP_NAME"
        
        local create_attempts=0
        local max_create_attempts=3
        
        while [ $create_attempts -lt $max_create_attempts ]; do
            create_attempts=$((create_attempts + 1))
            echo "🔄 App creation attempt $create_attempts/$max_create_attempts"
            
            # Try to create the app (handle network timeouts gracefully)
            if databricks apps create "$APP_NAME" 2>/dev/null; then
                echo "✅ App created successfully"
                sleep 10  # Give it time to initialize
                
                # Verify app was created
                if databricks apps get "$APP_NAME" &> /dev/null; then
                    echo "✅ App creation verified"
                    return 0
                else
                    echo "⚠️  App creation command succeeded but app not found, retrying..."
                fi
            else
                echo "⚠️  App creation attempt $create_attempts failed"
                if [ $create_attempts -lt $max_create_attempts ]; then
                    echo "🔄 Waiting 15 seconds before retry..."
                    sleep 15
                fi
            fi
        done
        
        echo "⚠️  Direct app creation failed after $max_create_attempts attempts"
        echo "📦 Will attempt creation through bundle deployment..."
        return 1
    fi
}

# Create app if needed
create_app_if_needed

# Deploy the bundle with automatic state cleanup
echo "📦 Deploying bundle..."

# Function to deploy with retry and state cleanup
deploy_with_retry() {
    local max_retries=3
    local retry_count=0
    
    while [ $retry_count -lt $max_retries ]; do
        echo "🔄 Deployment attempt $((retry_count + 1))/$max_retries"
        
        # Capture deployment output to check for specific errors
        local deploy_output
        deploy_output=$(databricks bundle deploy --target default 2>&1)
        local exit_code=$?
        
        if [ $exit_code -eq 0 ]; then
            echo "✅ Bundle deployed successfully"
            return 0
        else
            echo "❌ Deployment failed with exit code: $exit_code"
            
            # Check if it's a stale state issue
                if echo "$deploy_output" | grep -q "does not exist or is deleted"; then
                    echo "🔍 Detected stale Terraform state (app was deleted)"
                    echo "🧹 Cleaning up stale Terraform state..."
                    
                    # Clean up the Terraform state completely
                    if [ -d ".databricks/bundle/default/terraform" ]; then
                        rm -rf .databricks/bundle/default/terraform/
                        echo "✅ Terraform state directory removed"
                    fi
                    
                    # Clean up any other bundle state
                    if [ -d ".databricks/bundle/default" ]; then
                        rm -rf .databricks/bundle/default/
                        echo "✅ Bundle state directory removed"
                    fi
                    
                    # Try to destroy any remaining bundle resources
                    echo "🧹 Attempting to clean bundle resources..."
                    databricks bundle destroy --auto-approve --target default 2>/dev/null || echo "ℹ️  No bundle resources to clean"
                    
                    # If app doesn't exist, try to create it first
                    echo "🔄 Attempting to create app before retry..."
                    create_app_if_needed
                    
                    retry_count=$((retry_count + 1))
                    if [ $retry_count -lt $max_retries ]; then
                        echo "🔄 Retrying deployment with completely clean state..."
                        sleep 3
                    fi
            elif echo "$deploy_output" | grep -q "failed to read app"; then
                echo "🔍 Detected app reference issue in Terraform state"
                echo "🧹 Performing complete state cleanup and config fix..."
                
                # More aggressive cleanup
                rm -rf .databricks/bundle/ 2>/dev/null || true
                
                # Try to fix the issue by creating app with a temporary name first
                if [ $retry_count -eq 0 ]; then
                    echo "🔧 Attempting to create app with bundle deployment using clean workspace state..."
                    
                    # Clean workspace-level state
                    databricks workspace delete "/Workspace/Users/$(databricks auth describe | grep -oE '[^@]+@[^@]+\.[^@]+')/.bundle" -r 2>/dev/null || true
                fi
                
                retry_count=$((retry_count + 1))
                if [ $retry_count -lt $max_retries ]; then
                    echo "🔄 Retrying with fresh bundle state..."
                    sleep 3
                fi
            elif echo "$deploy_output" | grep -q "compute is in STARTING state"; then
                echo "🔍 Detected app is in STARTING state, waiting for it to stabilize..."
                
                # Wait for app to reach stable state
                wait_time=0
                max_wait=180  # 3 minutes
                while [ $wait_time -lt $max_wait ]; do
                    sleep 15
                    wait_time=$((wait_time + 15))
                    
                    APP_INFO=$(databricks apps get "$APP_NAME" 2>/dev/null)
                    if echo "$APP_INFO" | grep -q '"status":\s*"ACTIVE"'; then
                        echo "✅ App is now ACTIVE, retrying deployment..."
                        break
                    elif echo "$APP_INFO" | grep -q '"status":\s*"STOPPED"'; then
                        echo "✅ App is now STOPPED, retrying deployment..."
                        break
                    elif echo "$APP_INFO" | grep -q '"status":\s*"ERROR"'; then
                        echo "❌ App is in ERROR state, stopping it..."
                        databricks apps stop "$APP_NAME" 2>/dev/null || true
                        sleep 10
                        break
                    fi
                    
                    echo "⏳ Still waiting for app to stabilize... (${wait_time}s/${max_wait}s)"
                done
                
                retry_count=$((retry_count + 1))
                if [ $retry_count -lt $max_retries ]; then
                    echo "🔄 Retrying deployment with stabilized app..."
                    sleep 3
                fi
            else
                echo "❌ Deployment failed with different error:"
                echo "$deploy_output"
                return $exit_code
            fi
        fi
    done
    
    echo "❌ Deployment failed after $max_retries attempts"
    return 1
}

# Execute deployment with retry logic
if ! deploy_with_retry; then
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
echo "🔍 Source code path: $BUNDLE_FILES_PATH"

# Deploy with retry logic
deploy_source_attempts=0
max_source_attempts=3

while [ $deploy_source_attempts -lt $max_source_attempts ]; do
    deploy_source_attempts=$((deploy_source_attempts + 1))
    echo "🔄 Source deployment attempt $deploy_source_attempts/$max_source_attempts"
    
    if databricks apps deploy "$APP_NAME" --source-code-path "$BUNDLE_FILES_PATH"; then
        echo "✅ Source code deployed successfully!"
        break
    else
        echo "⚠️  Source deployment attempt $deploy_source_attempts failed"
        if [ $deploy_source_attempts -lt $max_source_attempts ]; then
            echo "🔄 Waiting 10 seconds before retry..."
            sleep 10
        else
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
