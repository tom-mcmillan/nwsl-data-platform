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
            "get_league_standings",
            "get_raw_data (includes FBRef professional stats)"
        ]
    }

@app.post("/mcp")
async def mcp_endpoint(request: dict):
    """MCP protocol endpoint"""
    global mcp_server
    
    if mcp_server is None:
        raise HTTPException(status_code=503, detail="MCP Server not initialized")
    
    try:
        # Handle MCP JSON-RPC request
        if "jsonrpc" not in request:
            request["jsonrpc"] = "2.0"
        
        method = request.get("method")
        params = request.get("params", {})
        request_id = request.get("id")
        
        if method == "initialize":
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "protocolVersion": "2024-11-05",
                    "serverInfo": {
                        "name": "nwsl-analytics",
                        "version": "1.0.0"
                    },
                    "capabilities": {
                        "tools": {}
                    }
                }
            }
        
        elif method == "tools/list":
            # Get tools from MCP server
            tools = [
                {
                    "name": "get_team_performance",
                    "description": "Get team performance metrics for a specific season",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "season": {"type": "string", "description": "Season year (e.g., '2024')"},
                            "team_id": {"type": "string", "description": "Team ID (optional)"}
                        },
                        "required": ["season"]
                    }
                },
                {
                    "name": "get_attendance_analysis",
                    "description": "Analyze attendance patterns across teams and seasons",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "season": {"type": "string", "description": "Season year (e.g., '2024')"}
                        },
                        "required": ["season"]
                    }
                },
                {
                    "name": "get_recent_games",
                    "description": "Get recent NWSL games with scores and details",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "limit": {"type": "integer", "description": "Number of games to return (default: 10)"},
                            "season": {"type": "string", "description": "Season year (e.g., '2024')"}
                        },
                        "required": ["season"]
                    }
                },
                {
                    "name": "get_league_standings",
                    "description": "Calculate league standings for a season",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "season": {"type": "string", "description": "Season year (e.g., '2024')"}
                        },
                        "required": ["season"]
                    }
                },
                {
                    "name": "get_raw_data",
                    "description": "Get raw statistical data - squad stats, player stats, games data, etc.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "data_type": {"type": "string", "description": "Type of data: 'squad_stats', 'player_stats', 'games', 'team_info', 'fbref_team_stats', 'fbref_player_stats', 'fbref_matches', 'fbref_player_match_stats'"},
                            "season": {"type": "string", "description": "Season year (e.g., '2024')"},
                            "team_id": {"type": "string", "description": "Optional: Filter by specific team"},
                            "limit": {"type": "integer", "description": "Optional: Limit number of rows returned (default: 50)"}
                        },
                        "required": ["data_type", "season"]
                    }
                }
            ]
            
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "tools": tools
                }
            }
        
        elif method == "tools/call":
            tool_name = params.get("name")
            tool_args = params.get("arguments", {})
            
            # Call the appropriate tool method
            if tool_name == "get_team_performance":
                result = await mcp_server._get_team_performance(tool_args)
            elif tool_name == "get_attendance_analysis":
                result = await mcp_server._get_attendance_analysis(tool_args)
            elif tool_name == "get_recent_games":
                result = await mcp_server._get_recent_games(tool_args)
            elif tool_name == "get_league_standings":
                result = await mcp_server._get_league_standings(tool_args)
            elif tool_name == "get_raw_data":
                result = await mcp_server._get_raw_data(tool_args)
            else:
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {
                        "code": -32601,
                        "message": f"Unknown tool: {tool_name}"
                    }
                }
            
            # Convert TextContent to dict format
            content = []
            for item in result:
                content.append({
                    "type": "text",
                    "text": item.text
                })
            
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "content": content
                }
            }
        
        else:
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32601,
                    "message": f"Method not found: {method}"
                }
            }
            
    except Exception as e:
        logger.error(f"MCP request error: {e}")
        return {
            "jsonrpc": "2.0",
            "id": request.get("id"),
            "error": {
                "code": -32603,
                "message": str(e)
            }
        }

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
