"""
HTTP wrapper for NWSL Analytics MCP Server (Cloud Run compatible)
Fully compliant with MCP specification
"""

import os
import logging
from typing import Dict, Any, Optional
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

try:
    from .analytics_server import NWSLAnalyticsServer
    print("✅ Successfully imported analytics_server")
    SERVER_TYPE = "analytics"
except ImportError as e:
    print(f"❌ Failed to import analytics_server: {e}")
    import traceback
    traceback.print_exc()
    print("📋 Falling back to original server")
    from .server import NWSLAnalyticsServer
    SERVER_TYPE = "basic"
from ..config.settings import settings

logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="NWSL Analytics MCP Server",
    description="Model Context Protocol server for NWSL soccer analytics",
    version="1.0.0"
)

# Add CORS middleware for browser-based clients
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["POST", "OPTIONS"],
    allow_headers=["*"],
)

# Global MCP server instance
mcp_server: Optional[NWSLAnalyticsServer] = None

# MCP Protocol Version
MCP_PROTOCOL_VERSION = "2024-11-05"

# JSON-RPC Error Codes (from spec)
PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603

@app.on_event("startup")
async def startup_event():
    """Initialize MCP server on startup"""
    global mcp_server
    logger.info(f"🚀 Initializing NWSL Analytics MCP Server ({SERVER_TYPE})...")
    try:
        mcp_server = NWSLAnalyticsServer()
        logger.info("✅ MCP Server initialized successfully")
    except Exception as e:
        logger.error(f"❌ MCP Server initialization failed: {e}")
        import traceback
        traceback.print_exc()
        raise

@app.get("/")
async def root():
    """Root endpoint with server information"""
    return {
        "name": "NWSL Analytics MCP Server",
        "version": "1.0.0",
        "description": "Professional NWSL soccer analytics with xG, possession stats, and more",
        "mcp_endpoint": "/mcp",
        "capabilities": [
            "tools", "resources", "prompts"
        ],
        "data_available": [
            "LIVE 2025 NWSL season data (current season in progress)",
            "FBref professional statistics (2020-2025)",
            "Complete NWSL player roster (1,016 players, 2016-2025)",
            "NWSL team information (17 teams)",
            "Match data (686 games, 2021-2025)",
            "xG, possession, passing accuracy, defensive stats"
        ]
    }

@app.post("/deploy-nwsl-data")
async def deploy_nwsl_data():
    """Deploy NWSL player and team data to BigQuery"""
    try:
        # Import the deployment function
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "scripts"))
        
        from deploy_nwsl_data import deploy_nwsl_data_to_bigquery
        
        # Run the deployment
        result = deploy_nwsl_data_to_bigquery()
        
        return {
            "status": "success" if result['success'] else "error",
            "message": "NWSL data deployment completed",
            "details": result
        }
        
    except Exception as e:
        logger.error(f"NWSL data deployment failed: {e}")
        return {
            "status": "error",
            "message": f"Deployment failed: {str(e)}"
        }

def create_error_response(id: Any, code: int, message: str, data: Any = None) -> Dict:
    """Create a JSON-RPC error response"""
    error = {
        "code": code,
        "message": message
    }
    if data is not None:
        error["data"] = data
    
    return {
        "jsonrpc": "2.0",
        "id": id,
        "error": error
    }

def create_success_response(id: Any, result: Any) -> Dict:
    """Create a JSON-RPC success response"""
    return {
        "jsonrpc": "2.0",
        "id": id,
        "result": result
    }

@app.post("/mcp")
async def mcp_endpoint(request: Request):
    """MCP protocol endpoint - handles all JSON-RPC requests"""
    global mcp_server
    
    if mcp_server is None:
        raise HTTPException(status_code=503, detail="MCP Server not initialized")
    
    try:
        # Parse JSON-RPC request
        try:
            json_data = await request.json()
        except Exception as e:
            return JSONResponse(
                create_error_response(None, PARSE_ERROR, "Parse error", str(e))
            )
        
        # Validate JSON-RPC structure
        if not isinstance(json_data, dict):
            return JSONResponse(
                create_error_response(None, INVALID_REQUEST, "Request must be an object")
            )
        
        # Extract request components
        jsonrpc = json_data.get("jsonrpc", "2.0")
        method = json_data.get("method")
        params = json_data.get("params", {})
        request_id = json_data.get("id")
        
        # Validate JSON-RPC version
        if jsonrpc != "2.0":
            return JSONResponse(
                create_error_response(request_id, INVALID_REQUEST, "Invalid JSON-RPC version")
            )
        
        # Method is required
        if not method:
            return JSONResponse(
                create_error_response(request_id, INVALID_REQUEST, "Method is required")
            )
        
        # Route to appropriate handler
        try:
            result = await handle_method(method, params)
            return JSONResponse(create_success_response(request_id, result))
        except ValueError as e:
            return JSONResponse(
                create_error_response(request_id, METHOD_NOT_FOUND, str(e))
            )
        except TypeError as e:
            return JSONResponse(
                create_error_response(request_id, INVALID_PARAMS, str(e))
            )
            
    except Exception as e:
        logger.error(f"MCP request error: {e}")
        return JSONResponse(
            create_error_response(
                json_data.get("id") if 'json_data' in locals() else None,
                INTERNAL_ERROR,
                "Internal error",
                str(e)
            )
        )

async def handle_method(method: str, params: Dict[str, Any]) -> Any:
    """Handle specific MCP methods"""
    
    # Core protocol methods
    if method == "initialize":
        return handle_initialize(params)
    
    # Tool methods
    elif method == "tools/list":
        return handle_tools_list()
    elif method == "tools/call":
        return await handle_tools_call(params)
    
    # Resource methods
    elif method == "resources/list":
        return handle_resources_list()
    elif method == "resources/read":
        return await handle_resources_read(params)
    
    # Prompt methods
    elif method == "prompts/list":
        return handle_prompts_list()
    elif method == "prompts/get":
        return handle_prompts_get(params)
    
    else:
        raise ValueError(f"Method not found: {method}")

def handle_initialize(params: Dict[str, Any]) -> Dict[str, Any]:
    """Handle initialize request"""
    # Extract client info
    protocol_version = params.get("protocolVersion", MCP_PROTOCOL_VERSION)
    capabilities = params.get("capabilities", {})
    client_info = params.get("clientInfo", {})
    
    logger.info(f"Client initialization: {client_info.get('name', 'Unknown')} v{client_info.get('version', 'Unknown')}")
    
    return {
        "protocolVersion": MCP_PROTOCOL_VERSION,
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

def handle_tools_list() -> Dict[str, Any]:
    """Return list of available tools"""
    
    # If using analytics server, get tools from MCP server
    if SERVER_TYPE == "analytics" and mcp_server:
        try:
            # The analytics server uses MCP protocol, so we need to get tools differently
            # For now, return the research analytics tools if available
            tools = []
            
            # Check if research analytics are available
            print(f"Debug: SERVER_TYPE={SERVER_TYPE}, mcp_server exists={mcp_server is not None}")
            if mcp_server:
                print(f"Debug: has xg_calculator attr={hasattr(mcp_server, 'xg_calculator')}")
                if hasattr(mcp_server, 'xg_calculator'):
                    print(f"Debug: xg_calculator value={mcp_server.xg_calculator}")
            
            if hasattr(mcp_server, 'xg_calculator') and mcp_server.xg_calculator:
                print("Debug: Adding research analytics tools")
                tools.extend([
                    {
                        "name": "expected_goals_analysis",
                        "title": "Expected Goals Calculator",
                        "description": "Analyze expected goals patterns for the LIVE 2025 NWSL season and historical data. Research includes xG efficiency, overperformers, and goal generation patterns. The 2025 season is currently in progress with real-time data updates.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "analysis_type": {
                                    "type": "string",
                                    "enum": ["player_xg", "league_patterns", "overperformers", "team_efficiency"],
                                    "description": "Type of xG analysis to perform"
                                },
                                "season": {"type": "string", "description": "Season year (e.g., '2025', '2024', '2023')"},
                                "player_name": {"type": "string", "description": "Specific player name (optional)"},
                                "team": {"type": "string", "description": "Specific team (optional)"},
                                "min_minutes": {"type": "integer", "default": 450}
                            },
                            "required": ["analysis_type", "season"]
                        }
                    },
                    {
                        "name": "shot_quality_analysis",
                        "title": "Shot Quality Profiler",
                        "description": "Analyze shot quality and finishing patterns for the LIVE 2025 NWSL season and historical data. Breaks down shooting by volume, quality, position, and conversion rates. The 2025 season is in progress with real-time updates after each match week.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "analysis_type": {
                                    "type": "string",
                                    "enum": ["player_profiles", "positional_patterns", "quality_leaders", "team_styles"],
                                    "description": "Type of shot quality analysis"
                                },
                                "season": {"type": "string", "description": "Season year (e.g., '2025', '2024', '2023')"},
                                "min_minutes": {"type": "integer", "default": 450},
                                "min_shots": {"type": "number", "default": 2.0}
                            },
                            "required": ["analysis_type", "season"]
                        }
                    },
                    {
                        "name": "replacement_value_analysis",
                        "title": "Replacement Value Estimator (WAR)",
                        "description": "Calculate player value above replacement level for the LIVE 2025 NWSL season and historical data. Provides WAR estimates and roster construction analysis. The 2025 season is currently in progress, enabling real-time player valuation and roster analysis.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "analysis_type": {
                                    "type": "string",
                                    "enum": ["replacement_baselines", "player_war", "team_construction", "undervalued_players"],
                                    "description": "Type of replacement value analysis"
                                },
                                "season": {"type": "string", "description": "Season year (e.g., '2025', '2024', '2023')"},
                                "min_minutes": {"type": "integer", "default": 450},
                                "min_war": {"type": "number", "default": 0.5}
                            },
                            "required": ["analysis_type", "season"]
                        }
                    },
                    {
                        "name": "query_raw_data",
                        "title": "NWSL Raw Data Query",
                        "description": "Execute SQL queries against NWSL BigQuery datasets for custom analysis",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "query": {
                                    "type": "string", 
                                    "description": "SQL query to execute against NWSL datasets"
                                },
                                "dataset": {
                                    "type": "string",
                                    "description": "Dataset to query (nwsl_fbref, nwsl_player_stats)",
                                    "default": "nwsl_fbref"
                                }
                            },
                            "required": ["query"]
                        }
                    },
                    {
                        "name": "get_team_roster",
                        "title": "Team Roster Analysis",
                        "description": "Get detailed player-by-player stats for a specific team, perfect for lineup optimization and player analysis.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "team": {
                                    "type": "string",
                                    "description": "Team name (e.g., 'Courage', 'Current', 'Spirit')"
                                },
                                "season": {"type": "string", "description": "Season year (e.g., '2025', '2024', '2023')"},
                                "min_minutes": {"type": "integer", "default": 450, "description": "Minimum minutes played to include player"},
                                "sort_by": {"type": "string", "default": "total_contributions", "enum": ["total_contributions", "goals", "assists", "expected_goals", "minutes_played"], "description": "How to sort players"}
                            },
                            "required": ["team", "season"]
                        }
                    },
                    {
                        "name": "roster_intelligence",
                        "title": "Smart Roster Analysis",
                        "description": "Advanced roster analysis with recency weighting, form detection, and lineup optimization suggestions. Accounts for recent performance trends.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "team": {"type": "string", "description": "Team name"},
                                "season": {"type": "string", "description": "Season year"},
                                "analysis_type": {
                                    "type": "string",
                                    "enum": ["current_form", "best_xi", "key_contributors", "underperformers", "recent_signings"],
                                    "description": "Type of roster analysis"
                                },
                                "position_focus": {"type": "string", "description": "Optional: Focus on specific position (FW, MF, DF, GK)"},
                                "recency_days": {"type": "integer", "default": 30, "description": "Weight recent games more heavily (days)"}
                            },
                            "required": ["team", "season", "analysis_type"]
                        }
                    },
                    {
                        "name": "ingest_current_roster",
                        "title": "Ingest Current Team Roster",
                        "description": "Scrape and ingest current 2025 roster data from FBref team pages to ensure lineup recommendations use active players only.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "team": {"type": "string", "description": "Team name (e.g., 'Angel City', 'North Carolina Courage')"},
                                "fbref_url": {"type": "string", "description": "FBref team stats URL (e.g., 'https://fbref.com/en/squads/ae38d267/Angel-City-FC-Stats')"},
                                "update_database": {"type": "boolean", "default": false, "description": "Whether to update the database with current roster"}
                            },
                            "required": ["team", "fbref_url"]
                        }
                    }
                ])
            
            # Add basic tools that are always available
            if tools:  # Only add basic tools if research tools are available
                return {"tools": tools}
                
        except Exception as e:
            print(f"Error getting analytics tools: {e}")
    
    # Fall back to basic tools list
    tools = [
        {
            "name": "get_raw_data",
            "title": "NWSL Raw Data Access",
            "description": "Get raw statistical data including squad stats, player stats, games data, team info, and professional FBref statistics from NWSL seasons.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "data_type": {
                        "type": "string",
                        "description": "Type of data to retrieve",
                        "enum": ["squad_stats", "player_stats", "games", "team_info", "fbref_team_stats", "fbref_player_stats", "fbref_matches", "fbref_player_match_stats"]
                    },
                    "season": {
                        "type": "string",
                        "description": "Season year (e.g., '2024')",
                        "pattern": "^20[0-9]{2}$"
                    },
                    "team_id": {
                        "type": "string",
                        "description": "Optional: Filter by specific team ID"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Optional: Limit number of rows returned (default: 50)",
                        "minimum": 1,
                        "maximum": 1000
                    }
                },
                "required": ["data_type", "season"]
            }
        },
        # Phase 1 Basic Analytics Tools
        {
            "name": "get_player_stats",
            "title": "Player Statistics",
            "description": "Get comprehensive player statistics with search by name/team. Returns goals, assists, minutes, and performance metrics.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "season": {
                        "type": "string",
                        "description": "Season year (e.g., '2024')",
                        "pattern": "^20[0-9]{2}$"
                    },
                    "player_name": {
                        "type": "string",
                        "description": "Optional: Search for specific player by name (partial matches allowed)"
                    },
                    "team_name": {
                        "type": "string",
                        "description": "Optional: Filter by team name (e.g., 'North Carolina Courage')"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Number of players to return (default: 20)",
                        "minimum": 1,
                        "maximum": 100
                    }
                },
                "required": ["season"]
            }
        },
        {
            "name": "get_team_stats",
            "title": "Team Statistics",
            "description": "Get comprehensive team statistics including goals, xG, possession, and defensive metrics.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "season": {
                        "type": "string",
                        "description": "Season year (e.g., '2024')",
                        "pattern": "^20[0-9]{2}$"
                    },
                    "team_name": {
                        "type": "string",
                        "description": "Optional: Filter by specific team name"
                    }
                },
                "required": ["season"]
            }
        },
        {
            "name": "get_standings",
            "title": "League Standings",
            "description": "Get current league standings with points, wins, losses, and goal difference.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "season": {
                        "type": "string",
                        "description": "Season year (e.g., '2024')",
                        "pattern": "^20[0-9]{2}$"
                    }
                },
                "required": ["season"]
            }
        },
        {
            "name": "get_match_results",
            "title": "Match Results",
            "description": "Get recent match results with scores, attendance, and basic match statistics.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "season": {
                        "type": "string",
                        "description": "Season year (e.g., '2024')",
                        "pattern": "^20[0-9]{2}$"
                    },
                    "team_name": {
                        "type": "string",
                        "description": "Optional: Filter by specific team"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Number of matches to return (default: 10)",
                        "minimum": 1,
                        "maximum": 50
                    }
                },
                "required": ["season"]
            }
        },
        # Phase 2 Advanced Analytics Tools
        {
            "name": "analyze_player_performance",
            "title": "Advanced Player Analysis",
            "description": "Deep analysis of player performance including efficiency metrics, xG analysis, and performance trends.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "player_name": {
                        "type": "string",
                        "description": "Name of the player to analyze"
                    },
                    "season": {
                        "type": "string",
                        "description": "Season year (e.g., '2024')",
                        "pattern": "^20[0-9]{2}$"
                    }
                },
                "required": ["player_name", "season"]
            }
        },
        {
            "name": "analyze_team_performance",
            "title": "Advanced Team Analysis",
            "description": "Deep analysis of team performance including tactical insights, efficiency metrics, and comparative analysis.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "team_name": {
                        "type": "string",
                        "description": "Name of the team to analyze"
                    },
                    "season": {
                        "type": "string",
                        "description": "Season year (e.g., '2024')",
                        "pattern": "^20[0-9]{2}$"
                    }
                },
                "required": ["team_name", "season"]
            }
        },
        {
            "name": "find_correlations",
            "title": "Statistical Correlations",
            "description": "Find statistical correlations and patterns in team/player performance to uncover insights about what drives success.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "analysis_type": {
                        "type": "string",
                        "description": "Type of correlation analysis",
                        "enum": ["team_performance", "player_performance", "match_outcomes"]
                    },
                    "season": {
                        "type": "string",
                        "description": "Season year (e.g., '2024')",
                        "pattern": "^20[0-9]{2}$"
                    },
                    "metric_focus": {
                        "type": "string",
                        "description": "Optional: Focus on specific metrics (e.g., 'goals', 'possession', 'xG')"
                    }
                },
                "required": ["analysis_type", "season"]
            }
        },
        {
            "name": "compare_teams",
            "title": "Team Comparison",
            "description": "Compare two teams across multiple dimensions including tactical analysis, strengths/weaknesses, and head-to-head performance.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "team1": {
                        "type": "string",
                        "description": "First team to compare"
                    },
                    "team2": {
                        "type": "string",
                        "description": "Second team to compare"
                    },
                    "season": {
                        "type": "string",
                        "description": "Season year (e.g., '2024')",
                        "pattern": "^20[0-9]{2}$"
                    }
                },
                "required": ["team1", "team2", "season"]
            }
        },
        # New NWSL Player Statistics Tools
        {
            "name": "get_nwsl_players",
            "title": "NWSL Player Roster",
            "description": "Get comprehensive NWSL player roster data including positions, nationalities, and seasons played. Covers all players from 2016-2024.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "player_name": {
                        "type": "string",
                        "description": "Optional: Search for specific player by name (partial matches allowed)"
                    },
                    "position": {
                        "type": "string",
                        "description": "Optional: Filter by position (GK, DF, MF, ST, etc.)"
                    },
                    "nationality": {
                        "type": "string",
                        "description": "Optional: Filter by nationality (e.g., 'USA', 'Canada')"
                    },
                    "team_name": {
                        "type": "string",
                        "description": "Optional: Filter by team name"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Number of players to return (default: 50)",
                        "minimum": 1,
                        "maximum": 500
                    }
                },
                "required": []
            }
        },
        {
            "name": "get_nwsl_teams",
            "title": "NWSL Team Information",
            "description": "Get comprehensive NWSL team information including names, abbreviations, and identifiers.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "team_name": {
                        "type": "string",
                        "description": "Optional: Search for specific team by name (partial matches allowed)"
                    }
                },
                "required": []
            }
        },
        {
            "name": "get_nwsl_games",
            "title": "NWSL Match Data",
            "description": "Get detailed NWSL match data including scores, attendance, and match information from 2021-2024.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "season": {
                        "type": "string",
                        "description": "Season year (2021, 2022, 2023, 2024, or 'all')",
                        "enum": ["2021", "2022", "2023", "2024", "all"]
                    },
                    "team_name": {
                        "type": "string",
                        "description": "Optional: Filter games for specific team"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Number of games to return (default: 20)",
                        "minimum": 1,
                        "maximum": 200
                    }
                },
                "required": ["season"]
            }
        }
    ]
    
    return {"tools": tools}

async def handle_tools_call(params: Dict[str, Any]) -> Dict[str, Any]:
    """Handle tool execution"""
    tool_name = params.get("name")
    if not tool_name:
        raise TypeError("Missing required parameter: name")
    
    tool_args = params.get("arguments", {})
    
    # Execute the tool
    if tool_name == "expected_goals_analysis":
        result = await mcp_server._handle_xg_analysis(tool_args)
    elif tool_name == "shot_quality_analysis":
        result = await mcp_server._handle_shot_analysis(tool_args)
    elif tool_name == "replacement_value_analysis":
        result = await mcp_server._handle_war_analysis(tool_args)
    elif tool_name == "query_raw_data":
        result = await mcp_server._handle_raw_query(tool_args)
    elif tool_name == "get_team_roster":
        result = await mcp_server._get_team_roster(tool_args)
    elif tool_name == "roster_intelligence":
        result = await mcp_server._roster_intelligence(tool_args)
    elif tool_name == "ingest_current_roster":
        result = await mcp_server._ingest_current_roster(tool_args)
    elif tool_name == "get_raw_data":
        result = await mcp_server._get_raw_data(tool_args)
    elif tool_name == "get_player_stats":
        result = await mcp_server._get_player_stats(tool_args)
    elif tool_name == "get_team_stats":
        result = await mcp_server._get_team_stats(tool_args)
    elif tool_name == "get_standings":
        result = await mcp_server._get_standings(tool_args)
    elif tool_name == "get_match_results":
        result = await mcp_server._get_match_results(tool_args)
    elif tool_name == "analyze_player_performance":
        result = await mcp_server._analyze_player_performance(tool_args)
    elif tool_name == "analyze_team_performance":
        result = await mcp_server._analyze_team_performance(tool_args)
    elif tool_name == "find_correlations":
        result = await mcp_server._find_correlations(tool_args)
    elif tool_name == "compare_teams":
        result = await mcp_server._compare_teams(tool_args)
    elif tool_name == "get_nwsl_players":
        result = await mcp_server._get_nwsl_players(tool_args)
    elif tool_name == "get_nwsl_teams":
        result = await mcp_server._get_nwsl_teams(tool_args)
    elif tool_name == "get_nwsl_games":
        result = await mcp_server._get_nwsl_games(tool_args)
    else:
        raise ValueError(f"Unknown tool: {tool_name}")
    
    # Convert result to proper format
    content = []
    for item in result:
        content.append({
            "type": "text",
            "text": item.text
        })
    
    return {"content": content}

def handle_resources_list() -> Dict[str, Any]:
    """Return list of available resources"""
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
    
    return {"resources": resources}

async def handle_resources_read(params: Dict[str, Any]) -> Dict[str, Any]:
    """Handle resource reading"""
    uri = params.get("uri")
    if not uri:
        raise TypeError("Missing required parameter: uri")
    
    # Read resource content
    if uri == "nwsl://seasons":
        content = "Available NWSL seasons with data:\n• 2020-2025 (FBref professional stats)\n• 2016-2024 (Basic match data)\n• 2013-2015 (Limited data)"
    
    elif uri == "nwsl://teams/2024":
        try:
            query = """
            SELECT DISTINCT meta_data.team_name
            FROM `nwsl-data.nwsl_fbref.nwsl_team_season_stats_2024`
            ORDER BY meta_data.team_name
            """
            df = mcp_server.bigquery_client.query(query).to_dataframe()
            teams = df['team_name'].tolist()
            content = "NWSL 2024 Teams:\n" + "\n".join(f"• {team}" for team in teams)
        except Exception as e:
            content = f"Error fetching teams: {str(e)}"
    
    elif uri == "nwsl://stats/summary/2024":
        try:
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
        except Exception as e:
            content = f"Error fetching stats: {str(e)}"
    
    elif uri == "nwsl://standings/2024":
        content = "NWSL 2024 Standings:\n• Kansas City Current (56 goals)\n• Washington Spirit (49 goals)\n• Orlando Pride (43 goals)\n• NJ/NY Gotham FC (40 goals)\n• Portland Thorns (37 goals)"
    
    else:
        raise ValueError(f"Resource not found: {uri}")
    
    return {
        "contents": [
            {
                "uri": uri,
                "mimeType": "text/plain",
                "text": content
            }
        ]
    }

def handle_prompts_list() -> Dict[str, Any]:
    """Return list of available prompts"""
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
    
    return {"prompts": prompts}

def handle_prompts_get(params: Dict[str, Any]) -> Dict[str, Any]:
    """Handle prompt retrieval"""
    name = params.get("name")
    if not name:
        raise TypeError("Missing required parameter: name")
    
    arguments = params.get("arguments", {})
    
    # Generate prompt based on name
    if name == "analyze-team-performance":
        team_name = arguments.get("team_name", "[TEAM]")
        season = arguments.get("season", "2024")
        
        return {
            "description": f"Analysis template for {team_name} in {season}",
            "messages": [
                {
                    "role": "user",
                    "content": {
                        "type": "text",
                        "text": f"""Analyze the performance of {team_name} in the {season} NWSL season.

Please provide a comprehensive analysis including:
1. **Goals & xG Analysis**: Compare actual goals scored vs expected goals (xG)
2. **Possession & Passing**: Analyze possession percentage and passing accuracy
3. **Defensive Performance**: Look at tackles, clean sheets, and goals conceded
4. **Key Strengths & Weaknesses**: Identify what the team does well and areas for improvement
5. **Season Context**: How does this performance compare to other teams?

Use the available NWSL analytics tools to gather the data and provide insights a professional soccer analyst would give to team management."""
                    }
                }
            ]
        }
    
    elif name == "compare-teams":
        team1 = arguments.get("team1", "[TEAM1]")
        team2 = arguments.get("team2", "[TEAM2]")
        season = arguments.get("season", "2024")
        
        return {
            "description": f"Comparison template for {team1} vs {team2} in {season}",
            "messages": [
                {
                    "role": "user",
                    "content": {
                        "type": "text",
                        "text": f"""Compare {team1} and {team2} in the {season} NWSL season.

Provide a detailed comparison including:
1. **Offensive Statistics**: Goals, xG, shots, passing in final third
2. **Defensive Statistics**: Goals conceded, tackles, clean sheets
3. **Possession & Control**: Possession percentage, passing accuracy, build-up play
4. **Head-to-Head**: If they played each other, analyze those matches
5. **Strengths vs Weaknesses**: What each team does better than the other
6. **Prediction**: Based on the data, who would likely win if they played?

Use professional soccer analysis techniques and reference advanced metrics."""
                    }
                }
            ]
        }
    
    elif name == "season-recap":
        season = arguments.get("season", "2024")
        
        return {
            "description": f"Season recap template for {season}",
            "messages": [
                {
                    "role": "user",
                    "content": {
                        "type": "text",
                        "text": f"""Create a comprehensive recap of the {season} NWSL season.

Include:
1. **Season Highlights**: Top performances, record-breaking moments
2. **Leading Teams**: Analyze top 3 teams by different metrics (goals, xG, possession)
3. **Surprise Performers**: Teams that over/under-performed expectations
4. **Key Trends**: What tactical or statistical trends defined the season
5. **Statistical Leaders**: Top scorers, best xG performers, defensive leaders
6. **Memorable Matches**: Highest-scoring games, biggest upsets
7. **Season Summary**: Overall assessment of the league's development

Write this as a professional season review that could be published by a major sports outlet."""
                    }
                }
            ]
        }
    
    else:
        raise ValueError(f"Unknown prompt: {name}")

def main():
    """Main entry point for Cloud Run"""
    import sys
    
    # Check if port is provided as command line argument
    if len(sys.argv) > 1 and sys.argv[1].startswith("--port"):
        port = int(sys.argv[1].split("=")[1])
    else:
        port = int(os.getenv("PORT", 8080))
    
    host = os.getenv("HOST", "0.0.0.0")
    
    logger.info(f"🚀 Starting NWSL Analytics MCP Server on {host}:{port}")
    
    uvicorn.run(
        "src.nwsl_analytics.mcp.http_server_v2:app",
        host=host,
        port=port,
        log_level="info",
        access_log=True
    )

if __name__ == "__main__":
    main()