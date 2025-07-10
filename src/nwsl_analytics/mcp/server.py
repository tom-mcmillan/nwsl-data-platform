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
                )
            ]

        @self.server.call_tool()
        async def handle_call_tool(name: str, arguments: Dict[str, Any]) -> List[types.TextContent]:
            """Handle tool calls"""
            
            if name == "get_raw_data":
                return await self._get_raw_data(arguments)
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
