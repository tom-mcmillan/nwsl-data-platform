#!/bin/bash

# Set default port for Cloud Run
export PORT=${PORT:-8080}
export HOST=${HOST:-0.0.0.0}

echo "🚀 Starting NWSL Analytics MCP Server..."
echo "📊 Project: ${GCP_PROJECT_ID}"
echo "🌐 Host: ${HOST}:${PORT}"

# Start the HTTP server (Cloud Run compatible)
cd /app
python -m src.nwsl_analytics.mcp.http_server_v2
