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
        
        # Register MCP tools
        self._register_tools()
    
    def _register_tools(self):
        """Register all NWSL analytics tools"""
        
        @self.server.list_tools()
        async def handle_list_tools() -> List[types.Tool]:
            """List available NWSL analytics tools"""
            return [
                types.Tool(
                    name="get_team_performance",
                    description="Get team performance metrics for a specific season",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "season": {
                                "type": "string",
                                "description": "Season year (e.g., '2024')"
                            },
                            "team_id": {
                                "type": "string", 
                                "description": "Team ID (optional - leave empty for all teams)"
                            }
                        },
                        "required": ["season"]
                    }
                ),
                types.Tool(
                    name="get_attendance_analysis",
                    description="Analyze attendance patterns across teams and seasons",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "season": {
                                "type": "string",
                                "description": "Season year (e.g., '2024')"
                            }
                        },
                        "required": ["season"]
                    }
                ),
                types.Tool(
                    name="get_recent_games",
                    description="Get recent NWSL games with scores and details",
                    inputSchema={
                        "type": "object", 
                        "properties": {
                            "limit": {
                                "type": "integer",
                                "description": "Number of games to return (default: 10)"
                            },
                            "season": {
                                "type": "string",
                                "description": "Season year (e.g., '2024')"
                            }
                        },
                        "required": ["season"]
                    }
                ),
                types.Tool(
                    name="get_league_standings",
                    description="Calculate league standings for a season",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "season": {
                                "type": "string", 
                                "description": "Season year (e.g., '2024')"
                            }
                        },
                        "required": ["season"]
                    }
                ),
                types.Tool(
                    name="get_raw_data",
                    description="Get raw statistical data - squad stats, player stats, games data, etc.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "data_type": {
                                "type": "string",
                                "description": "Type of data: 'squad_stats', 'player_stats', 'games', 'team_info', 'fbref_team_stats', 'fbref_player_stats', 'fbref_matches', 'fbref_player_match_stats'",
                                "enum": ["squad_stats", "player_stats", "games", "team_info", "fbref_team_stats", "fbref_player_stats", "fbref_matches", "fbref_player_match_stats"]
                            },
                            "season": {
                                "type": "string",
                                "description": "Season year (e.g., '2024')"
                            },
                            "team_id": {
                                "type": "string",
                                "description": "Optional: Filter by specific team"
                            },
                            "limit": {
                                "type": "integer",
                                "description": "Optional: Limit number of rows returned (default: 50)"
                            }
                        },
                        "required": ["data_type", "season"]
                    }
                )
            ]

        @self.server.call_tool()
        async def handle_call_tool(name: str, arguments: Dict[str, Any]) -> List[types.TextContent]:
            """Handle tool calls"""
            
            if name == "get_team_performance":
                return await self._get_team_performance(arguments)
            elif name == "get_attendance_analysis": 
                return await self._get_attendance_analysis(arguments)
            elif name == "get_recent_games":
                return await self._get_recent_games(arguments)
            elif name == "get_league_standings":
                return await self._get_league_standings(arguments)
            elif name == "get_raw_data":
                return await self._get_raw_data(arguments)
            else:
                raise ValueError(f"Unknown tool: {name}")

    async def _get_team_performance(self, args: Dict[str, Any]) -> List[types.TextContent]:
        """Get team performance metrics"""
        season = args.get("season")
        if not season:
            return [types.TextContent(
                type="text",
                text="Error: 'season' parameter is required (e.g., '2024')"
            )]
        team_id = args.get("team_id")
        
        try:
            # Query for team performance
            query = f"""
            SELECT 
                home_team_id as team_id,
                COUNT(*) as games_played,
                SUM(CASE WHEN home_score > away_score THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN home_score = away_score THEN 1 ELSE 0 END) as draws,
                SUM(CASE WHEN home_score < away_score THEN 1 ELSE 0 END) as losses,
                SUM(home_score) as goals_for,
                SUM(away_score) as goals_against,
                AVG(attendance) as avg_attendance
            FROM `{settings.gcp_project_id}.{self.dataset_id}.nwsl_games_{season}`
            {"WHERE home_team_id = '" + team_id + "'" if team_id else ""}
            GROUP BY home_team_id
            
            UNION ALL
            
            SELECT 
                away_team_id as team_id,
                COUNT(*) as games_played,
                SUM(CASE WHEN away_score > home_score THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN away_score = home_score THEN 1 ELSE 0 END) as draws,
                SUM(CASE WHEN away_score < home_score THEN 1 ELSE 0 END) as losses,
                SUM(away_score) as goals_for,
                SUM(home_score) as goals_against,
                AVG(attendance) as avg_attendance
            FROM `{settings.gcp_project_id}.{self.dataset_id}.nwsl_games_{season}`
            {"WHERE away_team_id = '" + team_id + "'" if team_id else ""}
            GROUP BY away_team_id
            """
            
            df = self.bigquery_client.query(query).to_dataframe()
            
            if df.empty:
                return [types.TextContent(
                    type="text",
                    text=f"No performance data found for season {season}"
                )]
            
            # Aggregate by team
            team_stats = df.groupby('team_id').agg({
                'games_played': 'sum',
                'wins': 'sum', 
                'draws': 'sum',
                'losses': 'sum',
                'goals_for': 'sum',
                'goals_against': 'sum',
                'avg_attendance': 'mean'
            }).round(2)
            
            # Calculate points (3 for win, 1 for draw)
            team_stats['points'] = team_stats['wins'] * 3 + team_stats['draws']
            team_stats['goal_diff'] = team_stats['goals_for'] - team_stats['goals_against']
            
            # Sort by points
            team_stats = team_stats.sort_values('points', ascending=False)
            
            result = f"NWSL {season} Team Performance:\n\n"
            for team, stats in team_stats.iterrows():
                result += f"Team {team}:\n"
                result += f"  Games: {int(stats['games_played'])} | "
                result += f"Points: {int(stats['points'])} | "
                result += f"W-D-L: {int(stats['wins'])}-{int(stats['draws'])}-{int(stats['losses'])} | "
                result += f"Goals: {int(stats['goals_for'])}-{int(stats['goals_against'])} ({stats['goal_diff']:+.0f}) | "
                result += f"Avg Attendance: {stats['avg_attendance']:,.0f}\n\n"
            
            return [types.TextContent(type="text", text=result)]
            
        except Exception as e:
            logger.error(f"Error getting team performance: {e}")
            return [types.TextContent(
                type="text", 
                text=f"Error retrieving team performance: {str(e)}"
            )]

    async def _get_attendance_analysis(self, args: Dict[str, Any]) -> List[types.TextContent]:
        """Analyze attendance patterns"""
        season = args.get("season")
        if not season:
            return [types.TextContent(
                type="text",
                text="Error: 'season' parameter is required (e.g., '2024')"
            )]
        
        try:
            query = f"""
            SELECT 
                AVG(attendance) as avg_attendance,
                MIN(attendance) as min_attendance,
                MAX(attendance) as max_attendance,
                COUNT(*) as total_games,
                SUM(attendance) as total_attendance
            FROM `{settings.gcp_project_id}.{self.dataset_id}.nwsl_games_{season}`
            WHERE attendance IS NOT NULL
            """
            
            df = self.bigquery_client.query(query).to_dataframe()
            
            if df.empty:
                return [types.TextContent(
                    type="text",
                    text=f"No attendance data found for season {season}"
                )]
            
            stats = df.iloc[0]
            
            result = f"""NWSL {season} Attendance Analysis:

📊 Overall Statistics:
- Average Attendance: {stats['avg_attendance']:,.0f}
- Total Attendance: {stats['total_attendance']:,.0f}
- Total Games: {int(stats['total_games'])}
- Lowest Attendance: {stats['min_attendance']:,.0f}
- Highest Attendance: {stats['max_attendance']:,.0f}
- Attendance Range: {stats['max_attendance'] - stats['min_attendance']:,.0f}
            """
            
            return [types.TextContent(type="text", text=result)]
            
        except Exception as e:
            logger.error(f"Error getting attendance analysis: {e}")
            return [types.TextContent(
                type="text",
                text=f"Error retrieving attendance analysis: {str(e)}"
            )]

    async def _get_recent_games(self, args: Dict[str, Any]) -> List[types.TextContent]:
        """Get recent games"""
        season = args.get("season")
        if not season:
            return [types.TextContent(
                type="text",
                text="Error: 'season' parameter is required (e.g., '2024')"
            )]
        limit = args.get("limit", 10)
        
        try:
            query = f"""
            SELECT 
                game_id,
                home_team_id,
                away_team_id, 
                home_score,
                away_score,
                date_time_utc,
                attendance
            FROM `{settings.gcp_project_id}.{self.dataset_id}.nwsl_games_{season}`
            ORDER BY date_time_utc DESC
            LIMIT {limit}
            """
            
            df = self.bigquery_client.query(query).to_dataframe()
            
            if df.empty:
                return [types.TextContent(
                    type="text",
                    text=f"No recent games found for season {season}"
                )]
            
            result = f"Recent NWSL {season} Games:\n\n"
            
            for _, game in df.iterrows():
                date = pd.to_datetime(game['date_time_utc']).strftime('%Y-%m-%d')
                result += f"🗓️ {date}: {game['home_team_id']} {game['home_score']}-{game['away_score']} {game['away_team_id']}"
                if pd.notna(game['attendance']):
                    result += f" (👥 {int(game['attendance']):,})"
                result += "\n"
            
            return [types.TextContent(type="text", text=result)]
            
        except Exception as e:
            logger.error(f"Error getting recent games: {e}")
            return [types.TextContent(
                type="text",
                text=f"Error retrieving recent games: {str(e)}"
            )]

    async def _get_league_standings(self, args: Dict[str, Any]) -> List[types.TextContent]:
        """Calculate league standings"""
        # This is similar to team performance but formatted as standings table
        return await self._get_team_performance(args)

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
                FROM `{settings.gcp_project_id}.{self.dataset_id}.nwsl_games_{season}`
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
                FROM `{settings.gcp_project_id}.{self.dataset_id}.nwsl_games_{season}`
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
                FROM `{settings.gcp_project_id}.{self.dataset_id}.nwsl_games_{season}`
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
                FROM `{settings.gcp_project_id}.{self.dataset_id}.nwsl_games_{season}`
                {"WHERE home_team_id = '" + team_id + "' OR away_team_id = '" + team_id + "'" if team_id else ""}
                ORDER BY date_time_utc DESC
                LIMIT {limit}
                """
                
            elif data_type == "team_info":
                # Team information from teams table
                query = f"""
                SELECT *
                FROM `{settings.gcp_project_id}.{self.dataset_id}.nwsl_teams_all`
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
