"""
NWSL Analytics MCP Server
Provides AI tools for soccer data analysis
"""

import logging
import asyncio
from typing import Dict, List, Any, Optional
import pandas as pd
from google.cloud import bigquery

from mcp.server import Server, NotificationOptions
from mcp.server.models import InitializationOptions
import mcp.server.stdio
import mcp.types as types

from ..config.settings import settings

logger = logging.getLogger(__name__)

class NWSLAnalyticsServer:
    """MCP Server for NWSL Analytics"""
    
    def __init__(self):
        self.server = Server("nwsl-analytics")
        self.bigquery_client = bigquery.Client(project=settings.gcp_project_id)
        self.dataset_id = settings.bigquery_dataset_id
        
        # Register MCP tools, resources, and prompts
        self._register_tools()
        self._register_resources()
        self._register_prompts()
    
    def _register_tools(self):
        """Register all NWSL analytics tools"""
        
        @self.server.list_tools()
        async def handle_list_tools() -> List[types.Tool]:
            """List available NWSL analytics tools"""
            return [
                types.Tool(
                    name="get_raw_data",
                    title="NWSL Raw Data Access",
                    description="Get raw statistical data including squad stats, player stats, games data, team info, and professional FBref statistics from NWSL seasons.",
                    inputSchema={
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
                ),
                # Phase 1 Basic Analytics Tools
                types.Tool(
                    name="get_player_stats",
                    title="Player Statistics",
                    description="Get comprehensive player statistics with search by name/team. Returns goals, assists, minutes, and performance metrics.",
                    inputSchema={
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
                ),
                types.Tool(
                    name="get_team_stats",
                    title="Team Statistics",
                    description="Get comprehensive team statistics including goals, xG, form, and performance metrics with user-friendly team names.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "season": {
                                "type": "string",
                                "description": "Season year (e.g., '2024')",
                                "pattern": "^20[0-9]{2}$"
                            },
                            "team_name": {
                                "type": "string",
                                "description": "Optional: Specific team name (e.g., 'North Carolina Courage', 'Orlando Pride')"
                            }
                        },
                        "required": ["season"]
                    }
                ),
                types.Tool(
                    name="get_standings",
                    title="League Standings",
                    description="Get current NWSL league standings with points, goal difference, form, and trends.",
                    inputSchema={
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
                ),
                types.Tool(
                    name="get_match_results",
                    title="Match Results",
                    description="Get recent match results with scores, attendance, and basic match statistics.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "season": {
                                "type": "string",
                                "description": "Season year (e.g., '2024')",
                                "pattern": "^20[0-9]{2}$"
                            },
                            "team_name": {
                                "type": "string",
                                "description": "Optional: Filter matches for specific team"
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
                ),
                # Phase 2 Advanced Analytics Tools
                types.Tool(
                    name="analyze_player_performance",
                    title="Advanced Player Performance Analysis",
                    description="Deep dive into player performance with xG analysis, form trends, and efficiency metrics.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "season": {
                                "type": "string",
                                "description": "Season year (e.g., '2024')",
                                "pattern": "^20[0-9]{2}$"
                            },
                            "player_name": {
                                "type": "string",
                                "description": "Player name to analyze"
                            }
                        },
                        "required": ["season", "player_name"]
                    }
                ),
                types.Tool(
                    name="analyze_team_performance",
                    title="Advanced Team Performance Analysis",
                    description="Comprehensive team analysis including xG vs results, tactical patterns, and performance correlations.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "season": {
                                "type": "string",
                                "description": "Season year (e.g., '2024')",
                                "pattern": "^20[0-9]{2}$"
                            },
                            "team_name": {
                                "type": "string",
                                "description": "Team name to analyze (e.g., 'North Carolina Courage')"
                            }
                        },
                        "required": ["season", "team_name"]
                    }
                ),
                types.Tool(
                    name="find_correlations",
                    title="Pattern Discovery & Correlations",
                    description="Discover statistical correlations and patterns across teams, players, or matches to uncover insights.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "season": {
                                "type": "string",
                                "description": "Season year (e.g., '2024')",
                                "pattern": "^20[0-9]{2}$"
                            },
                            "analysis_type": {
                                "type": "string",
                                "description": "Type of correlation analysis to perform",
                                "enum": ["team_performance", "attendance_impact", "xg_accuracy", "home_advantage", "form_patterns"]
                            }
                        },
                        "required": ["season", "analysis_type"]
                    }
                ),
                types.Tool(
                    name="compare_teams",
                    title="Team Comparison Analysis",
                    description="Compare two teams across multiple dimensions including head-to-head, performance metrics, and tactical analysis.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "season": {
                                "type": "string",
                                "description": "Season year (e.g., '2024')",
                                "pattern": "^20[0-9]{2}$"
                            },
                            "team1_name": {
                                "type": "string",
                                "description": "First team name"
                            },
                            "team2_name": {
                                "type": "string",
                                "description": "Second team name"
                            }
                        },
                        "required": ["season", "team1_name", "team2_name"]
                    }
                )
            ]

        @self.server.call_tool()
        async def handle_call_tool(name: str, arguments: Dict[str, Any]) -> List[types.TextContent]:
            """Handle tool calls"""
            
            if name == "get_raw_data":
                return await self._get_raw_data(arguments)
            elif name == "get_player_stats":
                return await self._get_player_stats(arguments)
            elif name == "get_team_stats":
                return await self._get_team_stats(arguments)
            elif name == "get_standings":
                return await self._get_standings(arguments)
            elif name == "get_match_results":
                return await self._get_match_results(arguments)
            elif name == "analyze_player_performance":
                return await self._analyze_player_performance(arguments)
            elif name == "analyze_team_performance":
                return await self._analyze_team_performance(arguments)
            elif name == "find_correlations":
                return await self._find_correlations(arguments)
            elif name == "compare_teams":
                return await self._compare_teams(arguments)
            else:
                raise ValueError(f"Unknown tool: {name}")


    async def _get_raw_data(self, args: Dict[str, Any]) -> List[types.TextContent]:
        """Get raw statistical data"""
        data_type = args.get("data_type")
        season = args.get("season")
        team_id = args.get("team_id")
        limit = args.get("limit", 50)
        
        if not data_type or not season:
            return [types.TextContent(
                type="text",
                text="Error: Both 'data_type' and 'season' parameters are required"
            )]
        
        try:
            if data_type == "squad_stats":
                # Aggregate team statistics like goals, assists, possession, etc.
                query = f"""
                SELECT 
                    home_team_id as team_id,
                    COUNT(*) as matches_played,
                    SUM(home_score) as goals_for,
                    SUM(away_score) as goals_against,
                    SUM(CASE WHEN home_score > away_score THEN 1 ELSE 0 END) as wins,
                    SUM(CASE WHEN home_score = away_score THEN 1 ELSE 0 END) as draws,
                    SUM(CASE WHEN home_score < away_score THEN 1 ELSE 0 END) as losses,
                    AVG(attendance) as avg_attendance,
                    (SUM(CASE WHEN home_score > away_score THEN 1 ELSE 0 END) * 3 + 
                     SUM(CASE WHEN home_score = away_score THEN 1 ELSE 0 END)) as points
                FROM `{settings.gcp_project_id}.nwsl_analytics.nwsl_games_{season}`
                {"WHERE home_team_id = '" + team_id + "'" if team_id else ""}
                GROUP BY home_team_id
                
                UNION ALL
                
                SELECT 
                    away_team_id as team_id,
                    COUNT(*) as matches_played,
                    SUM(away_score) as goals_for,
                    SUM(home_score) as goals_against,
                    SUM(CASE WHEN away_score > home_score THEN 1 ELSE 0 END) as wins,
                    SUM(CASE WHEN away_score = home_score THEN 1 ELSE 0 END) as draws,
                    SUM(CASE WHEN away_score < home_score THEN 1 ELSE 0 END) as losses,
                    AVG(attendance) as avg_attendance,
                    (SUM(CASE WHEN away_score > home_score THEN 1 ELSE 0 END) * 3 + 
                     SUM(CASE WHEN away_score = home_score THEN 1 ELSE 0 END)) as points
                FROM `{settings.gcp_project_id}.nwsl_analytics.nwsl_games_{season}`
                {"WHERE away_team_id = '" + team_id + "'" if team_id else ""}
                GROUP BY away_team_id
                ORDER BY points DESC
                LIMIT {limit}
                """
                
            elif data_type == "player_stats":
                # For now, return team-level data since we don't have individual player stats
                query = f"""
                SELECT 
                    home_team_id as team,
                    game_id,
                    date_time_utc,
                    home_score as goals,
                    away_score as goals_against,
                    attendance
                FROM `{settings.gcp_project_id}.nwsl_analytics.nwsl_games_{season}`
                {"WHERE home_team_id = '" + team_id + "'" if team_id else ""}
                ORDER BY date_time_utc DESC
                LIMIT {limit}
                """
                
            elif data_type == "games":
                # Raw games data - all match information
                query = f"""
                SELECT 
                    game_id,
                    date_time_utc,
                    home_team_id,
                    away_team_id,
                    home_score,
                    away_score,
                    attendance,
                    (home_score - away_score) as goal_difference
                FROM `{settings.gcp_project_id}.nwsl_analytics.nwsl_games_{season}`
                {"WHERE home_team_id = '" + team_id + "' OR away_team_id = '" + team_id + "'" if team_id else ""}
                ORDER BY date_time_utc DESC
                LIMIT {limit}
                """
                
            elif data_type == "team_info":
                # Team information from teams table
                query = f"""
                SELECT *
                FROM `{settings.gcp_project_id}.nwsl_analytics.nwsl_teams_all`
                {"WHERE team_id = '" + team_id + "'" if team_id else ""}
                LIMIT {limit}
                """
                
            else:
                # FBref data types (from BigQuery)
                if data_type == "fbref_team_stats":
                    # Get professional team statistics from BigQuery
                    query = f"""
                    SELECT *
                    FROM `{settings.gcp_project_id}.{self.dataset_id}.nwsl_team_season_stats_{season}`
                    {f"WHERE team_id = '{team_id}'" if team_id else ""}
                    ORDER BY points DESC, goal_difference DESC
                    LIMIT {limit}
                    """
                    
                elif data_type == "fbref_player_stats":
                    # Get professional player statistics from BigQuery
                    query = f"""
                    SELECT *
                    FROM `{settings.gcp_project_id}.{self.dataset_id}.nwsl_player_season_stats_{season}`
                    {f"WHERE team_id = '{team_id}'" if team_id else ""}
                    ORDER BY xg DESC, goals DESC
                    LIMIT {limit}
                    """
                    
                elif data_type == "fbref_matches":
                    # Get professional match data from BigQuery
                    query = f"""
                    SELECT *
                    FROM `{settings.gcp_project_id}.{self.dataset_id}.nwsl_matches_{season}`
                    {f"WHERE (home_team_id = '{team_id}' OR away_team_id = '{team_id}')" if team_id else ""}
                    ORDER BY match_date DESC
                    LIMIT {limit}
                    """
                    
                elif data_type == "fbref_player_match_stats":
                    # Get detailed player match statistics from BigQuery
                    query = f"""
                    SELECT *
                    FROM `{settings.gcp_project_id}.{self.dataset_id}.nwsl_player_match_stats_{season}`
                    {f"WHERE team_id = '{team_id}'" if team_id else ""}
                    ORDER BY match_date DESC, xg DESC
                    LIMIT {limit}
                    """
                    
                else:
                    return [types.TextContent(
                        type="text",
                        text=f"Error: Unknown data_type '{data_type}'. Available types: squad_stats, player_stats, games, team_info, fbref_team_stats, fbref_player_stats, fbref_matches, fbref_player_match_stats"
                    )]
            
            df = self.bigquery_client.query(query).to_dataframe()
            
            if df.empty:
                return [types.TextContent(
                    type="text",
                    text=f"No {data_type} data found for season {season}"
                )]
            
            # Format the data as CSV-like output for easy analysis
            result = f"Raw {data_type.replace('_', ' ').title()} Data - NWSL {season}\n"
            result += "=" * 60 + "\n\n"
            
            # Add column headers
            result += "\t".join(df.columns) + "\n"
            result += "-" * 60 + "\n"
            
            # Add data rows
            for _, row in df.iterrows():
                formatted_row = []
                for col in df.columns:
                    value = row[col]
                    if pd.isna(value):
                        formatted_row.append("NULL")
                    elif isinstance(value, float):
                        formatted_row.append(f"{value:.2f}")
                    else:
                        formatted_row.append(str(value))
                result += "\t".join(formatted_row) + "\n"
            
            result += f"\nTotal Records: {len(df)}\n"
            
            return [types.TextContent(type="text", text=result)]
            
        except Exception as e:
            logger.error(f"Error getting raw data: {e}")
            return [types.TextContent(
                type="text",
                text=f"Error retrieving {data_type} data: {str(e)}"
            )]

    # Helper method to get team ID from name
    async def _get_team_id_from_name(self, team_name: str) -> str:
        """Get team ID from team name"""
        query = f"""
        SELECT team_id, team_name 
        FROM `{settings.gcp_project_id}.nwsl_analytics.nwsl_teams_all`
        WHERE LOWER(team_name) LIKE LOWER('%{team_name}%')
        LIMIT 1
        """
        try:
            df = self.bigquery_client.query(query).to_dataframe()
            if not df.empty:
                return df.iloc[0]['team_id']
            return None
        except Exception:
            return None

    # Phase 1 Basic Analytics Tools
    async def _get_player_stats(self, args: Dict[str, Any]) -> List[types.TextContent]:
        """Get player statistics with search by name/team"""
        season = args.get("season")
        player_name = args.get("player_name")
        team_name = args.get("team_name")
        limit = args.get("limit", 20)
        
        try:
            # Use FBref player stats for comprehensive data
            query = f"""
            SELECT 
                p.player_name,
                p.team_name,
                p.position,
                p.age,
                p.minutes_played,
                p.goals,
                p.assists,
                p.expected_goals as xG,
                p.expected_assists as xA,
                ROUND(p.goals / NULLIF(p.expected_goals, 0), 2) as goal_efficiency,
                ROUND(p.minutes_played / 90, 1) as games_90s
            FROM `{settings.gcp_project_id}.nwsl_fbref.nwsl_player_season_stats_{season}` p
            WHERE 1=1
            """
            
            if player_name:
                query += f" AND LOWER(p.player_name) LIKE LOWER('%{player_name}%')"
            if team_name:
                query += f" AND LOWER(p.team_name) LIKE LOWER('%{team_name}%')"
                
            query += f"""
            ORDER BY p.goals DESC, p.expected_goals DESC
            LIMIT {limit}
            """
            
            df = self.bigquery_client.query(query).to_dataframe()
            
            if df.empty:
                return [types.TextContent(
                    type="text",
                    text=f"No player statistics found for {season} season"
                )]
            
            result = f"NWSL {season} Player Statistics\n"
            result += "=" * 50 + "\n\n"
            
            for _, player in df.iterrows():
                result += f"🏃‍♀️ {player['player_name']} ({player['team_name']})\n"
                result += f"   Position: {player['position']} | Age: {player['age']}\n"
                result += f"   Goals: {player['goals']} (xG: {player['xG']:.2f}) | Assists: {player['assists']} (xA: {player['xA']:.2f})\n"
                result += f"   Minutes: {player['minutes_played']} ({player['games_90s']} x 90min games)\n"
                if player['goal_efficiency'] and player['goal_efficiency'] != float('inf'):
                    result += f"   Goal Efficiency: {player['goal_efficiency']:.2f}x expected\n"
                result += "\n"
                
            return [types.TextContent(type="text", text=result)]
            
        except Exception as e:
            logger.error(f"Error getting player stats: {e}")
            return [types.TextContent(
                type="text",
                text=f"Error retrieving player statistics: {str(e)}"
            )]

    async def _get_team_stats(self, args: Dict[str, Any]) -> List[types.TextContent]:
        """Get comprehensive team statistics"""
        season = args.get("season")
        team_name = args.get("team_name")
        
        try:
            # Use FBref team stats for comprehensive data
            query = f"""
            SELECT 
                t.team_name,
                t.matches_played,
                t.wins,
                t.draws,
                t.losses,
                t.goals_for,
                t.goals_against,
                t.goal_difference,
                t.points,
                t.expected_goals as xG,
                t.expected_goals_against as xGA,
                ROUND(t.expected_goals - t.expected_goals_against, 2) as xG_diff,
                ROUND(t.goals_for / NULLIF(t.expected_goals, 0), 2) as goal_efficiency,
                t.possession_pct,
                t.pass_completion_pct
            FROM `{settings.gcp_project_id}.nwsl_fbref.nwsl_team_season_stats_{season}` t
            WHERE 1=1
            """
            
            if team_name:
                query += f" AND LOWER(t.team_name) LIKE LOWER('%{team_name}%')"
                
            query += " ORDER BY t.points DESC, t.goal_difference DESC"
            
            df = self.bigquery_client.query(query).to_dataframe()
            
            if df.empty:
                return [types.TextContent(
                    type="text",
                    text=f"No team statistics found for {season} season"
                )]
            
            result = f"NWSL {season} Team Statistics\n"
            result += "=" * 50 + "\n\n"
            
            for i, team in df.iterrows():
                result += f"🏆 #{i+1} {team['team_name']}\n"
                result += f"   Record: {team['wins']}W-{team['draws']}D-{team['losses']}L ({team['points']} pts)\n"
                result += f"   Goals: {team['goals_for']}-{team['goals_against']} (GD: {team['goal_difference']:+d})\n"
                result += f"   Expected: xG {team['xG']:.1f} - xGA {team['xGA']:.1f} (xGD: {team['xG_diff']:+.1f})\n"
                if team['goal_efficiency'] and team['goal_efficiency'] != float('inf'):
                    result += f"   Goal Efficiency: {team['goal_efficiency']:.2f}x expected\n"
                result += f"   Possession: {team['possession_pct']:.1f}% | Pass Completion: {team['pass_completion_pct']:.1f}%\n"
                result += "\n"
                
            return [types.TextContent(type="text", text=result)]
            
        except Exception as e:
            logger.error(f"Error getting team stats: {e}")
            return [types.TextContent(
                type="text",
                text=f"Error retrieving team statistics: {str(e)}"
            )]

    async def _get_standings(self, args: Dict[str, Any]) -> List[types.TextContent]:
        """Get league standings"""
        season = args.get("season")
        
        try:
            query = f"""
            SELECT 
                t.team_name,
                t.matches_played,
                t.wins,
                t.draws, 
                t.losses,
                t.goals_for,
                t.goals_against,
                t.goal_difference,
                t.points,
                ROUND(t.points / NULLIF(t.matches_played, 0), 2) as ppg
            FROM `{settings.gcp_project_id}.nwsl_fbref.nwsl_team_season_stats_{season}` t
            ORDER BY t.points DESC, t.goal_difference DESC, t.goals_for DESC
            """
            
            df = self.bigquery_client.query(query).to_dataframe()
            
            if df.empty:
                return [types.TextContent(
                    type="text",
                    text=f"No standings data found for {season} season"
                )]
            
            result = f"NWSL {season} League Standings\n"
            result += "=" * 60 + "\n"
            result += f"{'Pos':<3} {'Team':<22} {'MP':<3} {'W':<3} {'D':<3} {'L':<3} {'GF':<3} {'GA':<3} {'GD':<4} {'Pts':<3} {'PPG':<4}\n"
            result += "-" * 60 + "\n"
            
            for i, team in df.iterrows():
                pos = i + 1
                result += f"{pos:<3} {team['team_name'][:22]:<22} "
                result += f"{team['matches_played']:<3} {team['wins']:<3} {team['draws']:<3} {team['losses']:<3} "
                result += f"{team['goals_for']:<3} {team['goals_against']:<3} {team['goal_difference']:+4} "
                result += f"{team['points']:<3} {team['ppg']:<4}\n"
                
            return [types.TextContent(type="text", text=result)]
            
        except Exception as e:
            logger.error(f"Error getting standings: {e}")
            return [types.TextContent(
                type="text",
                text=f"Error retrieving standings: {str(e)}"
            )]

    async def _get_match_results(self, args: Dict[str, Any]) -> List[types.TextContent]:
        """Get recent match results"""
        season = args.get("season")
        team_name = args.get("team_name")
        limit = args.get("limit", 10)
        
        try:
            # Get team ID if team name provided
            team_filter = ""
            if team_name:
                team_id = await self._get_team_id_from_name(team_name)
                if team_id:
                    team_filter = f"AND (home_team_id = '{team_id}' OR away_team_id = '{team_id}')"
            
            query = f"""
            SELECT 
                g.date_time_utc,
                ht.team_name as home_team,
                at.team_name as away_team,
                g.home_score,
                g.away_score,
                g.attendance
            FROM `{settings.gcp_project_id}.nwsl_analytics.nwsl_games_{season}` g
            LEFT JOIN `{settings.gcp_project_id}.nwsl_analytics.nwsl_teams_all` ht ON g.home_team_id = ht.team_id
            LEFT JOIN `{settings.gcp_project_id}.nwsl_analytics.nwsl_teams_all` at ON g.away_team_id = at.team_id
            WHERE 1=1 {team_filter}
            ORDER BY g.date_time_utc DESC
            LIMIT {limit}
            """
            
            df = self.bigquery_client.query(query).to_dataframe()
            
            if df.empty:
                return [types.TextContent(
                    type="text",
                    text=f"No match results found for {season} season"
                )]
            
            result = f"NWSL {season} Recent Match Results\n"
            result += "=" * 50 + "\n\n"
            
            for _, match in df.iterrows():
                date = pd.to_datetime(match['date_time_utc']).strftime('%Y-%m-%d')
                result += f"📅 {date}\n"
                result += f"   {match['home_team']} {match['home_score']}-{match['away_score']} {match['away_team']}\n"
                if pd.notna(match['attendance']):
                    result += f"   👥 Attendance: {int(match['attendance']):,}\n"
                result += "\n"
                
            return [types.TextContent(type="text", text=result)]
            
        except Exception as e:
            logger.error(f"Error getting match results: {e}")
            return [types.TextContent(
                type="text",
                text=f"Error retrieving match results: {str(e)}"
            )]

    # Phase 2 Advanced Analytics Tools
    async def _analyze_player_performance(self, args: Dict[str, Any]) -> List[types.TextContent]:
        """Advanced player performance analysis"""
        season = args.get("season")
        player_name = args.get("player_name")
        
        try:
            query = f"""
            SELECT 
                p.player_name,
                p.team_name,
                p.position,
                p.age,
                p.minutes_played,
                p.goals,
                p.assists,
                p.expected_goals as xG,
                p.expected_assists as xA,
                p.shots,
                p.shots_on_target,
                ROUND(p.goals / NULLIF(p.expected_goals, 0), 2) as goal_efficiency,
                ROUND(p.shots_on_target / NULLIF(p.shots, 0) * 100, 1) as shot_accuracy,
                ROUND(p.goals / NULLIF(p.shots_on_target, 0) * 100, 1) as conversion_rate
            FROM `{settings.gcp_project_id}.nwsl_fbref.nwsl_player_season_stats_{season}` p
            WHERE LOWER(p.player_name) LIKE LOWER('%{player_name}%')
            ORDER BY p.goals DESC
            LIMIT 1
            """
            
            df = self.bigquery_client.query(query).to_dataframe()
            
            if df.empty:
                return [types.TextContent(
                    type="text",
                    text=f"No player found matching '{player_name}' in {season} season"
                )]
            
            player = df.iloc[0]
            
            result = f"🔍 Advanced Performance Analysis: {player['player_name']}\n"
            result += "=" * 60 + "\n\n"
            
            result += f"📊 Basic Info\n"
            result += f"   Team: {player['team_name']}\n"
            result += f"   Position: {player['position']} | Age: {player['age']}\n"
            result += f"   Minutes: {player['minutes_played']} ({player['minutes_played']/90:.1f} full games)\n\n"
            
            result += f"⚽ Goal Scoring Analysis\n"
            result += f"   Goals: {player['goals']} | Expected Goals (xG): {player['xG']:.2f}\n"
            if player['goal_efficiency'] and player['goal_efficiency'] != float('inf'):
                if player['goal_efficiency'] > 1.2:
                    result += f"   🔥 Exceptional finisher: {player['goal_efficiency']:.2f}x expected goals\n"
                elif player['goal_efficiency'] > 0.8:
                    result += f"   ✅ Good finisher: {player['goal_efficiency']:.2f}x expected goals\n"
                else:
                    result += f"   📉 Below expected: {player['goal_efficiency']:.2f}x expected goals\n"
            result += f"   Shot Accuracy: {player['shot_accuracy']:.1f}% ({player['shots_on_target']}/{player['shots']})\n"
            result += f"   Conversion Rate: {player['conversion_rate']:.1f}% (goals/shots on target)\n\n"
            
            result += f"🎯 Playmaking\n"
            result += f"   Assists: {player['assists']} | Expected Assists (xA): {player['xA']:.2f}\n"
            assist_efficiency = player['assists'] / max(player['xA'], 0.1)
            if assist_efficiency > 1.2:
                result += f"   🎭 Creative overperformer: {assist_efficiency:.2f}x expected assists\n"
            elif assist_efficiency > 0.8:
                result += f"   🎯 Solid playmaker: {assist_efficiency:.2f}x expected assists\n"
            else:
                result += f"   📉 Below expected creativity: {assist_efficiency:.2f}x expected assists\n"
                
            return [types.TextContent(type="text", text=result)]
            
        except Exception as e:
            logger.error(f"Error analyzing player performance: {e}")
            return [types.TextContent(
                type="text",
                text=f"Error analyzing player performance: {str(e)}"
            )]

    async def _analyze_team_performance(self, args: Dict[str, Any]) -> List[types.TextContent]:
        """Advanced team performance analysis"""
        season = args.get("season")
        team_name = args.get("team_name")
        
        try:
            query = f"""
            SELECT 
                t.team_name,
                t.matches_played,
                t.wins,
                t.draws,
                t.losses,
                t.goals_for,
                t.goals_against,
                t.goal_difference,
                t.points,
                t.expected_goals as xG,
                t.expected_goals_against as xGA,
                t.possession_pct,
                t.pass_completion_pct,
                ROUND(t.goals_for / NULLIF(t.expected_goals, 0), 2) as attack_efficiency,
                ROUND(t.goals_against / NULLIF(t.expected_goals_against, 0), 2) as defense_efficiency
            FROM `{settings.gcp_project_id}.nwsl_fbref.nwsl_team_season_stats_{season}` t
            WHERE LOWER(t.team_name) LIKE LOWER('%{team_name}%')
            LIMIT 1
            """
            
            df = self.bigquery_client.query(query).to_dataframe()
            
            if df.empty:
                return [types.TextContent(
                    type="text",
                    text=f"No team found matching '{team_name}' in {season} season"
                )]
            
            team = df.iloc[0]
            
            result = f"🔍 Advanced Team Analysis: {team['team_name']}\n"
            result += "=" * 60 + "\n\n"
            
            result += f"📊 Season Overview\n"
            result += f"   Record: {team['wins']}W-{team['draws']}D-{team['losses']}L ({team['points']} points)\n"
            result += f"   Points per game: {team['points']/team['matches_played']:.2f}\n"
            result += f"   Goal difference: {team['goal_difference']:+d}\n\n"
            
            result += f"⚽ Attack Analysis\n"
            result += f"   Goals scored: {team['goals_for']} | Expected goals: {team['xG']:.1f}\n"
            if team['attack_efficiency'] > 1.1:
                result += f"   🔥 Clinical finishing: {team['attack_efficiency']:.2f}x expected\n"
            elif team['attack_efficiency'] > 0.9:
                result += f"   ✅ Efficient attack: {team['attack_efficiency']:.2f}x expected\n"
            else:
                result += f"   📉 Wasteful finishing: {team['attack_efficiency']:.2f}x expected\n"
            
            result += f"🛡️ Defense Analysis\n"
            result += f"   Goals conceded: {team['goals_against']} | Expected against: {team['xGA']:.1f}\n"
            if team['defense_efficiency'] < 0.9:
                result += f"   🛡️ Solid defense: {team['defense_efficiency']:.2f}x expected (good)\n"
            elif team['defense_efficiency'] < 1.1:
                result += f"   ⚖️ Average defense: {team['defense_efficiency']:.2f}x expected\n"
            else:
                result += f"   🚨 Leaky defense: {team['defense_efficiency']:.2f}x expected (poor)\n"
            
            result += f"\n🎯 Style Analysis\n"
            result += f"   Possession: {team['possession_pct']:.1f}%\n"
            result += f"   Pass completion: {team['pass_completion_pct']:.1f}%\n"
            
            if team['possession_pct'] > 55:
                result += f"   🎭 Possession-based team\n"
            elif team['possession_pct'] < 45:
                result += f"   ⚡ Counter-attacking style\n"
            else:
                result += f"   ⚖️ Balanced approach\n"
                
            return [types.TextContent(type="text", text=result)]
            
        except Exception as e:
            logger.error(f"Error analyzing team performance: {e}")
            return [types.TextContent(
                type="text",
                text=f"Error analyzing team performance: {str(e)}"
            )]

    async def _find_correlations(self, args: Dict[str, Any]) -> List[types.TextContent]:
        """Find correlations and patterns in the data"""
        season = args.get("season")
        analysis_type = args.get("analysis_type")
        
        try:
            if analysis_type == "team_performance":
                query = f"""
                SELECT 
                    team_name,
                    points,
                    expected_goals as xG,
                    expected_goals_against as xGA,
                    possession_pct,
                    pass_completion_pct,
                    goals_for,
                    goals_against
                FROM `{settings.gcp_project_id}.nwsl_fbref.nwsl_team_season_stats_{season}`
                ORDER BY points DESC
                """
                
                df = self.bigquery_client.query(query).to_dataframe()
                
                # Calculate correlations
                corr_xg_points = df['xG'].corr(df['points'])
                corr_possession_points = df['possession_pct'].corr(df['points'])
                corr_pass_points = df['pass_completion_pct'].corr(df['points'])
                
                result = f"🔍 Team Performance Correlations - NWSL {season}\n"
                result += "=" * 55 + "\n\n"
                
                result += f"📊 What drives success?\n"
                result += f"   Expected Goals vs Points: {corr_xg_points:.3f}\n"
                result += f"   Possession vs Points: {corr_possession_points:.3f}\n"
                result += f"   Pass Accuracy vs Points: {corr_pass_points:.3f}\n\n"
                
                if corr_xg_points > 0.7:
                    result += f"🎯 Strong correlation: Teams that create quality chances win more\n"
                if corr_possession_points > 0.5:
                    result += f"🎭 Possession matters: Ball control leads to success\n"
                elif corr_possession_points < 0.2:
                    result += f"⚡ Possession overrated: Direct play can be effective\n"
                    
            elif analysis_type == "attendance_impact":
                # Analyze attendance correlation with performance
                query = f"""
                SELECT 
                    ht.team_name,
                    AVG(g.attendance) as avg_attendance,
                    AVG(CASE WHEN g.home_score > g.away_score THEN 1 ELSE 0 END) as home_win_rate,
                    COUNT(*) as home_games
                FROM `{settings.gcp_project_id}.nwsl_analytics.nwsl_games_{season}` g
                JOIN `{settings.gcp_project_id}.nwsl_analytics.nwsl_teams_all` ht ON g.home_team_id = ht.team_id
                WHERE g.attendance IS NOT NULL
                GROUP BY ht.team_name
                HAVING COUNT(*) >= 5
                """
                
                df = self.bigquery_client.query(query).to_dataframe()
                
                corr_attendance_wins = df['avg_attendance'].corr(df['home_win_rate'])
                
                result = f"🏟️ Attendance Impact Analysis - NWSL {season}\n"
                result += "=" * 50 + "\n\n"
                
                result += f"📊 Does crowd support matter?\n"
                result += f"   Attendance vs Home Win Rate: {corr_attendance_wins:.3f}\n\n"
                
                if corr_attendance_wins > 0.4:
                    result += f"🔥 Strong home advantage: Bigger crowds boost performance\n"
                elif corr_attendance_wins > 0.2:
                    result += f"👥 Moderate impact: Attendance helps but isn't decisive\n"
                else:
                    result += f"🤷 Minimal impact: Performance drives attendance, not vice versa\n"
                    
                # Top attendance teams
                result += f"\n🎪 Attendance Leaders:\n"
                top_attendance = df.nlargest(3, 'avg_attendance')
                for _, team in top_attendance.iterrows():
                    result += f"   {team['team_name']}: {team['avg_attendance']:,.0f} avg (Win rate: {team['home_win_rate']:.1%})\n"
                    
            else:
                result = f"📊 Analysis type '{analysis_type}' not yet implemented\n"
                result += "Available: team_performance, attendance_impact\n"
                
            return [types.TextContent(type="text", text=result)]
            
        except Exception as e:
            logger.error(f"Error finding correlations: {e}")
            return [types.TextContent(
                type="text",
                text=f"Error finding correlations: {str(e)}"
            )]

    async def _compare_teams(self, args: Dict[str, Any]) -> List[types.TextContent]:
        """Compare two teams across multiple dimensions"""
        season = args.get("season")
        team1_name = args.get("team1_name")
        team2_name = args.get("team2_name")
        
        try:
            query = f"""
            SELECT 
                t.team_name,
                t.points,
                t.wins,
                t.draws,
                t.losses,
                t.goals_for,
                t.goals_against,
                t.goal_difference,
                t.expected_goals as xG,
                t.expected_goals_against as xGA,
                t.possession_pct,
                t.pass_completion_pct
            FROM `{settings.gcp_project_id}.nwsl_fbref.nwsl_team_season_stats_{season}` t
            WHERE LOWER(t.team_name) LIKE LOWER('%{team1_name}%') 
               OR LOWER(t.team_name) LIKE LOWER('%{team2_name}%')
            ORDER BY t.points DESC
            """
            
            df = self.bigquery_client.query(query).to_dataframe()
            
            if len(df) < 2:
                return [types.TextContent(
                    type="text",
                    text=f"Could not find both teams matching '{team1_name}' and '{team2_name}'"
                )]
            
            team1 = df.iloc[0]
            team2 = df.iloc[1]
            
            result = f"⚔️ Team Comparison: {team1['team_name']} vs {team2['team_name']}\n"
            result += "=" * 70 + "\n\n"
            
            result += f"📊 League Position\n"
            result += f"   {team1['team_name']}: {team1['points']} points\n"
            result += f"   {team2['team_name']}: {team2['points']} points\n"
            if team1['points'] > team2['points']:
                result += f"   🏆 {team1['team_name']} leads by {team1['points'] - team2['points']} points\n\n"
            else:
                result += f"   🏆 {team2['team_name']} leads by {team2['points'] - team1['points']} points\n\n"
            
            result += f"⚽ Attack Comparison\n"
            result += f"   Goals: {team1['team_name']} {team1['goals_for']} - {team2['goals_for']} {team2['team_name']}\n"
            result += f"   xG: {team1['team_name']} {team1['xG']:.1f} - {team2['xG']:.1f} {team2['team_name']}\n"
            
            if team1['goals_for'] > team2['goals_for']:
                result += f"   🎯 {team1['team_name']} has the superior attack\n"
            else:
                result += f"   🎯 {team2['team_name']} has the superior attack\n"
                
            result += f"\n🛡️ Defense Comparison\n"
            result += f"   Goals Against: {team1['team_name']} {team1['goals_against']} - {team2['goals_against']} {team2['team_name']}\n"
            result += f"   xGA: {team1['team_name']} {team1['xGA']:.1f} - {team2['xGA']:.1f} {team2['team_name']}\n"
            
            if team1['goals_against'] < team2['goals_against']:
                result += f"   🛡️ {team1['team_name']} has the better defense\n"
            else:
                result += f"   🛡️ {team2['team_name']} has the better defense\n"
                
            result += f"\n🎭 Style Comparison\n"
            result += f"   Possession: {team1['team_name']} {team1['possession_pct']:.1f}% - {team2['possession_pct']:.1f}% {team2['team_name']}\n"
            result += f"   Pass Accuracy: {team1['team_name']} {team1['pass_completion_pct']:.1f}% - {team2['pass_completion_pct']:.1f}% {team2['team_name']}\n"
            
            if abs(team1['possession_pct'] - team2['possession_pct']) > 5:
                if team1['possession_pct'] > team2['possession_pct']:
                    result += f"   🎭 {team1['team_name']} dominates possession, {team2['team_name']} likely more direct\n"
                else:
                    result += f"   🎭 {team2['team_name']} dominates possession, {team1['team_name']} likely more direct\n"
            else:
                result += f"   ⚖️ Similar playing styles in terms of possession\n"
                
            return [types.TextContent(type="text", text=result)]
            
        except Exception as e:
            logger.error(f"Error comparing teams: {e}")
            return [types.TextContent(
                type="text",
                text=f"Error comparing teams: {str(e)}"
            )]

    def _register_resources(self):
        """Register MCP resources"""
        
        @self.server.list_resources()
        async def handle_list_resources() -> List[types.Resource]:
            """List available NWSL resources"""
            return [
                types.Resource(
                    uri="nwsl://seasons",
                    name="NWSL Seasons",
                    description="Available NWSL seasons with data",
                    mimeType="text/plain"
                ),
                types.Resource(
                    uri="nwsl://teams/2024",
                    name="NWSL Teams 2024",
                    description="List of NWSL teams for 2024 season",
                    mimeType="text/plain"
                ),
                types.Resource(
                    uri="nwsl://stats/summary/2024",
                    name="NWSL 2024 Season Summary",
                    description="Key statistics and highlights from 2024 season",
                    mimeType="text/plain"
                ),
                types.Resource(
                    uri="nwsl://standings/2024",
                    name="NWSL 2024 Standings",
                    description="Current league standings for 2024",
                    mimeType="text/plain"
                )
            ]
        
        @self.server.read_resource()
        async def handle_read_resource(uri: str) -> str:
            """Read a specific NWSL resource"""
            try:
                if uri == "nwsl://seasons":
                    return "Available NWSL seasons with data:\n• 2020-2025 (FBref professional stats)\n• 2016-2024 (Basic match data)\n• 2013-2015 (Limited data)"
                
                elif uri == "nwsl://teams/2024":
                    query = """
                    SELECT DISTINCT meta_data.team_name
                    FROM `nwsl-data.nwsl_fbref.nwsl_team_season_stats_2024`
                    ORDER BY meta_data.team_name
                    """
                    df = self.bigquery_client.query(query).to_dataframe()
                    teams = df['team_name'].tolist()
                    return f"NWSL 2024 Teams:\n" + "\n".join(f"• {team}" for team in teams)
                
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
                    df = self.bigquery_client.query(query).to_dataframe()
                    result = "NWSL 2024 Top Goal Scorers:\n"
                    for _, row in df.iterrows():
                        result += f"• {row['team_name']}: {row['goals']} goals (xG: {row['xG']}, Possession: {row['possession']}%)\n"
                    return result
                
                elif uri == "nwsl://standings/2024":
                    # This would need to be calculated from match results
                    return "NWSL 2024 Standings:\n• Kansas City Current (Leading in goals with 56)\n• Washington Spirit (49 goals)\n• Orlando Pride (43 goals)\n• NJ/NY Gotham FC (40 goals)\n• Portland Thorns (37 goals)"
                
                else:
                    return f"Resource not found: {uri}"
                    
            except Exception as e:
                logger.error(f"Error reading resource {uri}: {e}")
                return f"Error reading resource {uri}: {str(e)}"

    def _register_prompts(self):
        """Register MCP prompts"""
        
        @self.server.list_prompts()
        async def handle_list_prompts() -> List[types.Prompt]:
            """List available NWSL prompts"""
            return [
                types.Prompt(
                    name="analyze-team-performance",
                    description="Analyze a team's performance with xG and advanced metrics",
                    arguments=[
                        types.PromptArgument(
                            name="team_name",
                            description="Name of the NWSL team to analyze",
                            required=True
                        ),
                        types.PromptArgument(
                            name="season",
                            description="Season to analyze (e.g., '2024')",
                            required=True
                        )
                    ]
                ),
                types.Prompt(
                    name="compare-teams",
                    description="Compare two NWSL teams across multiple metrics",
                    arguments=[
                        types.PromptArgument(
                            name="team1",
                            description="First team to compare",
                            required=True
                        ),
                        types.PromptArgument(
                            name="team2", 
                            description="Second team to compare",
                            required=True
                        ),
                        types.PromptArgument(
                            name="season",
                            description="Season to compare (e.g., '2024')",
                            required=True
                        )
                    ]
                ),
                types.Prompt(
                    name="season-recap",
                    description="Generate a comprehensive season recap with key statistics",
                    arguments=[
                        types.PromptArgument(
                            name="season",
                            description="Season to recap (e.g., '2024')",
                            required=True
                        )
                    ]
                )
            ]
        
        @self.server.get_prompt()
        async def handle_get_prompt(name: str, arguments: Dict[str, str]) -> types.GetPromptResult:
            """Get a specific NWSL prompt"""
            try:
                if name == "analyze-team-performance":
                    team_name = arguments.get("team_name")
                    season = arguments.get("season")
                    
                    prompt_text = f"""Analyze the performance of {team_name} in the {season} NWSL season. 

Please provide a comprehensive analysis including:
1. **Goals & xG Analysis**: Compare actual goals scored vs expected goals (xG)
2. **Possession & Passing**: Analyze possession percentage and passing accuracy
3. **Defensive Performance**: Look at tackles, clean sheets, and goals conceded
4. **Key Strengths & Weaknesses**: Identify what the team does well and areas for improvement
5. **Season Context**: How does this performance compare to other teams?

Use the available NWSL analytics tools to gather the data and provide insights a professional soccer analyst would give to team management."""
                    
                    return types.GetPromptResult(
                        description=f"Analysis template for {team_name} in {season}",
                        messages=[
                            types.PromptMessage(
                                role="user",
                                content=types.TextContent(
                                    type="text",
                                    text=prompt_text
                                )
                            )
                        ]
                    )
                
                elif name == "compare-teams":
                    team1 = arguments.get("team1")
                    team2 = arguments.get("team2")
                    season = arguments.get("season")
                    
                    prompt_text = f"""Compare {team1} and {team2} in the {season} NWSL season.

Provide a detailed comparison including:
1. **Offensive Statistics**: Goals, xG, shots, passing in final third
2. **Defensive Statistics**: Goals conceded, tackles, clean sheets
3. **Possession & Control**: Possession percentage, passing accuracy, build-up play
4. **Head-to-Head**: If they played each other, analyze those matches
5. **Strengths vs Weaknesses**: What each team does better than the other
6. **Prediction**: Based on the data, who would likely win if they played?

Use professional soccer analysis techniques and reference advanced metrics."""
                    
                    return types.GetPromptResult(
                        description=f"Comparison template for {team1} vs {team2} in {season}",
                        messages=[
                            types.PromptMessage(
                                role="user",
                                content=types.TextContent(
                                    type="text",
                                    text=prompt_text
                                )
                            )
                        ]
                    )
                
                elif name == "season-recap":
                    season = arguments.get("season")
                    
                    prompt_text = f"""Create a comprehensive recap of the {season} NWSL season.

Include:
1. **Season Highlights**: Top performances, record-breaking moments
2. **Leading Teams**: Analyze top 3 teams by different metrics (goals, xG, possession)
3. **Surprise Performers**: Teams that over/under-performed expectations
4. **Key Trends**: What tactical or statistical trends defined the season
5. **Statistical Leaders**: Top scorers, best xG performers, defensive leaders
6. **Memorable Matches**: Highest-scoring games, biggest upsets
7. **Season Summary**: Overall assessment of the league's development

Write this as a professional season review that could be published by a major sports outlet."""
                    
                    return types.GetPromptResult(
                        description=f"Season recap template for {season}",
                        messages=[
                            types.PromptMessage(
                                role="user",
                                content=types.TextContent(
                                    type="text",
                                    text=prompt_text
                                )
                            )
                        ]
                    )
                
                else:
                    raise ValueError(f"Unknown prompt: {name}")
                    
            except Exception as e:
                logger.error(f"Error getting prompt {name}: {e}")
                raise

    async def run(self):
        """Run the MCP server"""
        async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
            await self.server.run(
                read_stream,
                write_stream, 
                InitializationOptions(
                    server_name="nwsl-analytics",
                    server_version="1.0.0",
                    capabilities=self.server.get_capabilities(
                        notification_options=NotificationOptions(),
                        experimental_capabilities={}
                    )
                )
            )


async def main():
    """Main entry point"""
    logging.basicConfig(level=logging.INFO)
    logger.info("🚀 Starting NWSL Analytics MCP Server...")
    
    server = NWSLAnalyticsServer()
    await server.run()


if __name__ == "__main__":
    asyncio.run(main())
