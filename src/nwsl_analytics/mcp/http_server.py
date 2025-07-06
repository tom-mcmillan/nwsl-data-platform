"""
HTTP wrapper for NWSL Analytics MCP Server (Cloud Run compatible)
"""

import os
import logging
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import uvicorn

from .server import NWSLAnalyticsServer
from ..config.settings import settings

logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="NWSL Analytics MCP Server",
    description="Model Context Protocol server for NWSL soccer analytics",
    version="1.0.0"
)

# Global MCP server instance
mcp_server = None

@app.on_event("startup")
async def startup_event():
    """Initialize MCP server on startup"""
    global mcp_server
    logger.info("🚀 Initializing NWSL Analytics MCP Server...")
    mcp_server = NWSLAnalyticsServer()
    logger.info("✅ MCP Server initialized successfully")

@app.get("/health")
async def health_check():
    """Health check endpoint for Cloud Run"""
    return {"status": "healthy", "service": "nwsl-analytics-mcp"}

@app.get("/ready")
async def readiness_check():
    """Readiness check endpoint for Cloud Run"""
    if mcp_server is None:
        raise HTTPException(status_code=503, detail="MCP Server not ready")
    return {"status": "ready", "service": "nwsl-analytics-mcp"}

@app.get("/")
async def root():
    """Root endpoint with service information"""
    return {
        "service": "NWSL Analytics MCP Server",
        "version": "1.0.0",
        "description": "Model Context Protocol server for NWSL soccer analytics",
        "endpoints": {
            "health": "/health",
            "ready": "/ready",
            "mcp": "/mcp"
        },
        "tools": [
            "get_team_performance",
            "get_attendance_analysis", 
            "get_recent_games",
            "get_league_standings"
        ]
    }

@app.post("/mcp")
async def mcp_endpoint(request: dict):
    """MCP protocol endpoint"""
    global mcp_server
    
    if mcp_server is None:
        raise HTTPException(status_code=503, detail="MCP Server not initialized")
    
    try:
        # Handle MCP request (simplified for HTTP)
        # In a real implementation, you'd need to adapt the MCP protocol
        return {"status": "MCP endpoint - implement protocol adapter"}
    except Exception as e:
        logger.error(f"MCP request error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

def main():
    """Main entry point for Cloud Run"""
    port = int(os.getenv("PORT", 8080))
    host = os.getenv("HOST", "0.0.0.0")
    
    logger.info(f"🚀 Starting NWSL Analytics MCP Server on {host}:{port}")
    
    uvicorn.run(
        "src.nwsl_analytics.mcp.http_server:app",
        host=host,
        port=port,
        log_level="info",
        access_log=True
    )

if __name__ == "__main__":
    main()
