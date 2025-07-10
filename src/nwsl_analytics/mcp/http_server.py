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
                        "tools": {},
                        "resources": {},
                        "prompts": {}
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
        
        elif method == "resources/list":
            # Get resources from MCP server
            resources = [
                {
                    "uri": "nwsl://seasons",
                    "name": "NWSL Seasons",
                    "description": "Available NWSL seasons with data",
                    "mimeType": "text/plain"
                },
                {
                    "uri": "nwsl://teams/2024",
                    "name": "NWSL Teams 2024", 
                    "description": "List of NWSL teams for 2024 season",
                    "mimeType": "text/plain"
                },
                {
                    "uri": "nwsl://stats/summary/2024",
                    "name": "NWSL 2024 Season Summary",
                    "description": "Key statistics and highlights from 2024 season",
                    "mimeType": "text/plain"
                },
                {
                    "uri": "nwsl://standings/2024",
                    "name": "NWSL 2024 Standings",
                    "description": "Current league standings for 2024",
                    "mimeType": "text/plain"
                }
            ]
            
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "resources": resources
                }
            }
        
        elif method == "resources/read":
            uri = params.get("uri")
            if not uri:
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {
                        "code": -32602,
                        "message": "Missing uri parameter"
                    }
                }
            
            # Read resource from MCP server
            try:
                # Call resource handler manually
                if uri == "nwsl://seasons":
                    content = "Available NWSL seasons with data:\n• 2020-2025 (FBref professional stats)\n• 2016-2024 (Basic match data)\n• 2013-2015 (Limited data)"
                elif uri == "nwsl://teams/2024":
                    query = """
                    SELECT DISTINCT meta_data.team_name
                    FROM `nwsl-data.nwsl_fbref.nwsl_team_season_stats_2024`
                    ORDER BY meta_data.team_name
                    """
                    df = mcp_server.bigquery_client.query(query).to_dataframe()
                    teams = df['team_name'].tolist()
                    content = f"NWSL 2024 Teams:\n" + "\n".join(f"• {team}" for team in teams)
                elif uri == "nwsl://stats/summary/2024":
                    query = """
                    SELECT 
                        meta_data.team_name,
                        stats.stats.ttl_gls as goals,
                        ROUND(stats.stats.ttl_xg, 2) as xG,
                        stats.possession.avg_poss as possession
                    FROM `nwsl-data.nwsl_fbref.nwsl_team_season_stats_2024`
                    ORDER BY stats.stats.ttl_gls DESC
                    LIMIT 5
                    """
                    df = mcp_server.bigquery_client.query(query).to_dataframe()
                    content = "NWSL 2024 Top Goal Scorers:\n"
                    for _, row in df.iterrows():
                        content += f"• {row['team_name']}: {row['goals']} goals (xG: {row['xG']}, Possession: {row['possession']}%)\n"
                elif uri == "nwsl://standings/2024":
                    content = "NWSL 2024 Standings:\n• Kansas City Current (Leading in goals with 56)\n• Washington Spirit (49 goals)\n• Orlando Pride (43 goals)\n• NJ/NY Gotham FC (40 goals)\n• Portland Thorns (37 goals)"
                else:
                    content = f"Resource not found: {uri}"
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "contents": [
                            {
                                "uri": uri,
                                "mimeType": "text/plain",
                                "text": content
                            }
                        ]
                    }
                }
            except Exception as e:
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {
                        "code": -32603,
                        "message": f"Error reading resource: {str(e)}"
                    }
                }
        
        elif method == "prompts/list":
            # Get prompts from MCP server
            prompts = [
                {
                    "name": "analyze-team-performance",
                    "description": "Analyze a team's performance with xG and advanced metrics",
                    "arguments": [
                        {
                            "name": "team_name",
                            "description": "Name of the NWSL team to analyze",
                            "required": True
                        },
                        {
                            "name": "season",
                            "description": "Season to analyze (e.g., '2024')",
                            "required": True
                        }
                    ]
                },
                {
                    "name": "compare-teams",
                    "description": "Compare two NWSL teams across multiple metrics",
                    "arguments": [
                        {
                            "name": "team1",
                            "description": "First team to compare",
                            "required": True
                        },
                        {
                            "name": "team2",
                            "description": "Second team to compare", 
                            "required": True
                        },
                        {
                            "name": "season",
                            "description": "Season to compare (e.g., '2024')",
                            "required": True
                        }
                    ]
                },
                {
                    "name": "season-recap",
                    "description": "Generate a comprehensive season recap with key statistics",
                    "arguments": [
                        {
                            "name": "season",
                            "description": "Season to recap (e.g., '2024')",
                            "required": True
                        }
                    ]
                }
            ]
            
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "prompts": prompts
                }
            }
        
        elif method == "prompts/get":
            name = params.get("name")
            arguments = params.get("arguments", {})
            
            if not name:
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {
                        "code": -32602,
                        "message": "Missing name parameter"
                    }
                }
            
            # Get prompt from MCP server
            try:
                # Generate prompt content manually
                if name == "analyze-team-performance":
                    team_name = arguments.get("team_name", "TEAM")
                    season = arguments.get("season", "2024")
                    description = f"Analysis template for {team_name} in {season}"
                    text = f"""Analyze the performance of {team_name} in the {season} NWSL season. 

Please provide a comprehensive analysis including:
1. **Goals & xG Analysis**: Compare actual goals scored vs expected goals (xG)
2. **Possession & Passing**: Analyze possession percentage and passing accuracy
3. **Defensive Performance**: Look at tackles, clean sheets, and goals conceded
4. **Key Strengths & Weaknesses**: Identify what the team does well and areas for improvement
5. **Season Context**: How does this performance compare to other teams?

Use the available NWSL analytics tools to gather the data and provide insights a professional soccer analyst would give to team management."""
                
                elif name == "compare-teams":
                    team1 = arguments.get("team1", "TEAM1")
                    team2 = arguments.get("team2", "TEAM2")
                    season = arguments.get("season", "2024")
                    description = f"Comparison template for {team1} vs {team2} in {season}"
                    text = f"""Compare {team1} and {team2} in the {season} NWSL season.

Provide a detailed comparison including:
1. **Offensive Statistics**: Goals, xG, shots, passing in final third
2. **Defensive Statistics**: Goals conceded, tackles, clean sheets
3. **Possession & Control**: Possession percentage, passing accuracy, build-up play
4. **Head-to-Head**: If they played each other, analyze those matches
5. **Strengths vs Weaknesses**: What each team does better than the other
6. **Prediction**: Based on the data, who would likely win if they played?

Use professional soccer analysis techniques and reference advanced metrics."""
                
                elif name == "season-recap":
                    season = arguments.get("season", "2024")
                    description = f"Season recap template for {season}"
                    text = f"""Create a comprehensive recap of the {season} NWSL season.

Include:
1. **Season Highlights**: Top performances, record-breaking moments
2. **Leading Teams**: Analyze top 3 teams by different metrics (goals, xG, possession)
3. **Surprise Performers**: Teams that over/under-performed expectations
4. **Key Trends**: What tactical or statistical trends defined the season
5. **Statistical Leaders**: Top scorers, best xG performers, defensive leaders
6. **Memorable Matches**: Highest-scoring games, biggest upsets
7. **Season Summary**: Overall assessment of the league's development

Write this as a professional season review that could be published by a major sports outlet."""
                
                else:
                    raise ValueError(f"Unknown prompt: {name}")
                
                # Return prompt structure
                prompt_result = {
                    "description": description,
                    "messages": [
                        {
                            "role": "user",
                            "content": {
                                "type": "text",
                                "text": text
                            }
                        }
                    ]
                }
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": prompt_result
                }
            except Exception as e:
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {
                        "code": -32603,
                        "message": f"Error getting prompt: {str(e)}"
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
