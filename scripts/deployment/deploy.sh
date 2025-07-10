#!/bin/bash

# NWSL Analytics MCP Server Deployment Script

set -e

PROJECT_ID="nwsl-data"
REGION="us-central1"
SERVICE_NAME="nwsl-analytics-mcp"

echo "🚀 Deploying NWSL Analytics MCP Server to Cloud Run..."

# Enable required APIs
echo "📡 Enabling required Google Cloud APIs..."
gcloud services enable cloudbuild.googleapis.com
gcloud services enable run.googleapis.com
gcloud services enable containerregistry.googleapis.com

# Set project
gcloud config set project $PROJECT_ID

# Submit build to Cloud Build
echo "🏗️ Starting Cloud Build..."
gcloud builds submit --config cloudbuild.yaml .

echo "✅ Deployment complete!"
echo "🌐 Service URL:"
gcloud run services describe $SERVICE_NAME --region=$REGION --format="value(status.url)"

echo ""
echo "🔍 To view logs:"
echo "gcloud logs read --service=$SERVICE_NAME --region=$REGION"

echo ""
echo "📊 To view service details:"
echo "gcloud run services describe $SERVICE_NAME --region=$REGION"
