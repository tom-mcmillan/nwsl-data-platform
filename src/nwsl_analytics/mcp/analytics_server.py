"""
Enhanced NWSL Analytics MCP Server
Provides advanced soccer analytics tools based on research questions
"""

import logging
import asyncio
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional
import pandas as pd
from google.cloud import bigquery
import json

from mcp.server import Server, NotificationOptions
from mcp.server.models import InitializationOptions
import mcp.server.stdio
import mcp.types as types

# Add analytics modules to path
analytics_path = Path(__file__).parent.parent.parent.parent / "analytics"
sys.path.append(str(analytics_path))

try:
    from expected_goals.calculator import ExpectedGoalsCalculator
    from shot_quality.profiler import ShotQualityProfiler  
    from replacement_value.estimator import ReplacementValueEstimator
except ImportError as e:
    logging.warning(f"Could not import analytics modules: {e}")
    ExpectedGoalsCalculator = None
    ShotQualityProfiler = None
    ReplacementValueEstimator = None

logger = logging.getLogger(__name__)

class NWSLAnalyticsServer:
    """Enhanced MCP Server for NWSL Analytics with research-based tools"""
    
    def __init__(self, project_id: str = "nwsl-data"):
        self.server = Server("nwsl-analytics-research")
        self.project_id = project_id
        
        # Initialize analytics tools (with error handling)
        try:
            self.bigquery_client = bigquery.Client(project=project_id)
            if ExpectedGoalsCalculator:
                self.xg_calculator = ExpectedGoalsCalculator(project_id)
            if ShotQualityProfiler:
                self.shot_profiler = ShotQualityProfiler(project_id)
            if ReplacementValueEstimator:
                self.war_estimator = ReplacementValueEstimator(project_id)
            logger.info("✅ Analytics tools initialized successfully")
        except Exception as e:
            logger.warning(f"Could not initialize BigQuery client: {e}")
            self.bigquery_client = None
            self.xg_calculator = None
            self.shot_profiler = None
            self.war_estimator = None
        
        # Team name mappings for user-friendly queries
        self.team_mappings = {
            'north carolina courage': 'Courage',
            'nc courage': 'Courage',
            'courage': 'Courage',
            'chicago red stars': 'Red Stars',
            'red stars': 'Red Stars',
            'houston dash': 'Dash',
            'dash': 'Dash',
            'orlando pride': 'Pride',
            'pride': 'Pride',
            'portland thorns': 'Thorns',
            'thorns': 'Thorns',
            'washington spirit': 'Spirit',
            'spirit': 'Spirit',
            'gotham fc': 'Gotham FC',
            'gotham': 'Gotham FC',
            'kansas city current': 'Current',
            'current': 'Current',
            'san diego wave': 'Wave',
            'wave': 'Wave',
            'angel city': 'Angel City',
            'racing louisville': 'Louisville',
            'louisville': 'Louisville',
            'seattle reign': 'Reign',
            'reign': 'Reign',
            'utah royals': 'Royals',
            'royals': 'Royals',
            'bay fc': 'Bay FC'
        }
        
        # Register MCP tools, resources, and prompts
        self._register_tools()
        self._register_resources()
        self._register_prompts()
    
    def _normalize_team_name(self, team_name: str) -> str:
        """Convert user-friendly team names to database Squad names"""
        if not team_name:
            return team_name
        normalized = team_name.lower().strip()
        return self.team_mappings.get(normalized, team_name)
    
    def _register_tools(self):
        """Register all NWSL research analytics tools"""
        
        @self.server.list_tools()
        async def handle_list_tools() -> List[types.Tool]:
            """List available NWSL research analytics tools"""
            tools = [
                types.Tool(
                    name="query_raw_data",
                    title="NWSL Raw Data Query",
                    description="Execute SQL queries against NWSL BigQuery datasets for custom analysis",
                    inputSchema={
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
                )
            ]
            
            # Add research analytics tools if available
            if self.xg_calculator:
                tools.extend([
                    types.Tool(
                        name="expected_goals_analysis",
                        title="Expected Goals Calculator",
                        description="Analyze expected goals patterns to answer 'What truly generates goals?' Research includes xG efficiency, overperformers, and goal generation patterns. Updated regularly with current 2025 season data.",
                        inputSchema={
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
                    ),
                    types.Tool(
                        name="shot_quality_analysis", 
                        title="Shot Quality Profiler",
                        description="Analyze shot quality and finishing patterns. Breaks down shooting by volume, quality, position, and conversion rates to understand goal generation. Includes current 2025 season data updated after each match week.",
                        inputSchema={
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
                    ),
                    types.Tool(
                        name="replacement_value_analysis",
                        title="Replacement Value Estimator (WAR)",
                        description="Calculate player value above replacement level to answer 'What is replacement level in soccer?' Provides WAR estimates and roster construction analysis. Current 2025 season data enables real-time player valuation.",
                        inputSchema={
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
                    )
                ])
            
            return tools
        
        @self.server.call_tool()
        async def handle_call_tool(name: str, arguments: Dict[str, Any]) -> List[types.TextContent]:
            """Handle tool calls"""
            
            if name == "query_raw_data":
                return await self._handle_raw_query(arguments)
            elif name == "expected_goals_analysis":
                return await self._handle_xg_analysis(arguments)
            elif name == "shot_quality_analysis":
                return await self._handle_shot_analysis(arguments)
            elif name == "replacement_value_analysis":
                return await self._handle_war_analysis(arguments)
            else:
                raise ValueError(f"Unknown tool: {name}")
    
    async def _handle_raw_query(self, args: Dict[str, Any]) -> List[types.TextContent]:
        """Handle raw SQL queries"""
        if not self.bigquery_client:
            return [types.TextContent(type="text", text="BigQuery client not available")]
        
        try:
            query = args["query"]
            dataset = args.get("dataset", "nwsl_fbref")
            
            # Add project prefix if not present
            if f"{self.project_id}." not in query:
                query = query.replace(f"{dataset}.", f"{self.project_id}.{dataset}.")
            
            df = self.bigquery_client.query(query).to_dataframe()
            
            # Format results
            result = f"Query Results ({len(df)} rows):\n\n"
            result += df.to_string(max_rows=50, max_cols=10)
            
            if len(df) > 50:
                result += f"\n\n... showing first 50 of {len(df)} rows"
            
            return [types.TextContent(type="text", text=result)]
            
        except Exception as e:
            return [types.TextContent(type="text", text=f"Query failed: {str(e)}")]
    
    async def _handle_xg_analysis(self, args: Dict[str, Any]) -> List[types.TextContent]:
        """Handle Expected Goals analysis"""
        if not self.xg_calculator:
            return [types.TextContent(type="text", text="Expected Goals Calculator not available")]
        
        try:
            analysis_type = args["analysis_type"]
            if "season" not in args:
                return [types.TextContent(type="text", text="Error: Season parameter is required. Please specify a season (e.g., '2025', '2024', '2023')")]
            season = args["season"]
            
            if analysis_type == "player_xg":
                team_name = self._normalize_team_name(args.get("team")) if args.get("team") else None
                df = self.xg_calculator.get_player_xg_analysis(
                    player_name=args.get("player_name"),
                    season=season,
                    team=team_name
                )
                
                result = f"Player xG Analysis for {season}:\n\n"
                result += "Top Performers by Expected Goals:\n"
                for _, player in df.head(15).iterrows():
                    result += f"• {player['player_name']} ({player['team']}): {player['expected_goals']:.2f} xG, {player['goals']} goals (conversion: {player['goal_conversion_rate']:.2f})\n"
                
            elif analysis_type == "league_patterns":
                patterns = self.xg_calculator.analyze_goal_generation_patterns(season)
                
                result = f"League-wide Goal Generation Patterns ({season}):\n\n"
                result += "League Metrics:\n"
                for metric, value in patterns['league_metrics'].items():
                    result += f"• {metric.replace('_', ' ').title()}: {value}\n"
                
                result += "\nPosition Breakdown:\n"
                for breakdown in patterns.get('position_metrics', {}).get('position_breakdown', []):
                    result += f"• {breakdown}\n"
            
            elif analysis_type == "overperformers":
                df = self.xg_calculator.find_xg_overperformers(season, args.get("min_minutes", 900))
                
                result = f"xG Over/Under-performers ({season}):\n\n"
                result += "Biggest Overperformers:\n"
                overperformers = df[df['goals_vs_expected'] > 0].head(10)
                for _, player in overperformers.iterrows():
                    result += f"• {player['player_name']}: +{player['goals_vs_expected']:.2f} vs expected ({player['performance_category']})\n"
                
                result += "\nBiggest Underperformers:\n"
                underperformers = df[df['goals_vs_expected'] < 0].tail(5)
                for _, player in underperformers.iterrows():
                    result += f"• {player['player_name']}: {player['goals_vs_expected']:.2f} vs expected ({player['performance_category']})\n"
            
            elif analysis_type == "team_efficiency":
                df = self.xg_calculator.calculate_team_xg_efficiency(season)
                
                result = f"Team xG Efficiency ({season}):\n\n"
                for _, team in df.head(10).iterrows():
                    result += f"• {team['team_name']}: {team['team_conversion_rate']:.2f} conversion rate, {team['goals_vs_expected']:.1f} vs expected\n"
            
            return [types.TextContent(type="text", text=result)]
            
        except Exception as e:
            return [types.TextContent(type="text", text=f"xG Analysis failed: {str(e)}")]
    
    async def _handle_shot_analysis(self, args: Dict[str, Any]) -> List[types.TextContent]:
        """Handle Shot Quality analysis"""
        if not self.shot_profiler:
            return [types.TextContent(type="text", text="Shot Quality Profiler not available")]
        
        try:
            analysis_type = args["analysis_type"]
            if "season" not in args:
                return [types.TextContent(type="text", text="Error: Season parameter is required. Please specify a season (e.g., '2025', '2024', '2023')")]
            season = args["season"]
            
            if analysis_type == "player_profiles":
                df = self.shot_profiler.analyze_shooting_profiles(season, args.get("min_minutes", 450))
                
                result = f"Player Shooting Profiles ({season}):\n\n"
                result += "Top Shot Profiles:\n"
                for _, player in df.head(15).iterrows():
                    result += f"• {player['player_name']} ({player['team']}): {player['xg_per_90']:.2f} xG/90, {player['shooter_type']}, {player['finishing_quality']}\n"
            
            elif analysis_type == "positional_patterns":
                patterns = self.shot_profiler.analyze_positional_shooting_patterns(season)
                
                result = f"Positional Shooting Patterns ({season}):\n\n"
                for pos_data in patterns['position_data']:
                    result += f"• {pos_data['position_group']}: {pos_data['avg_xg_per_90']:.2f} avg xG/90, {pos_data['position_conversion_rate']:.2f} conversion rate\n"
                
                result += f"\nSummary:\n"
                for key, value in patterns['summary'].items():
                    result += f"• {key.replace('_', ' ').title()}: {value}\n"
            
            elif analysis_type == "quality_leaders":
                df = self.shot_profiler.find_shot_quality_leaders(season, args.get("min_shots", 2.0))
                
                result = f"Shot Quality Leaders ({season}):\n\n"
                for _, player in df.head(10).iterrows():
                    result += f"• {player['player_name']}: {player['estimated_xg_per_shot']:.3f} xG/shot, {player['volume_category']}\n"
            
            elif analysis_type == "team_styles":
                df = self.shot_profiler.analyze_team_shooting_styles(season)
                
                result = f"Team Shooting Styles ({season}):\n\n"
                for _, team in df.head(10).iterrows():
                    result += f"• {team['team_name']}: {team['attacking_style']}, {team['finishing_quality']} finishing\n"
            
            return [types.TextContent(type="text", text=result)]
            
        except Exception as e:
            return [types.TextContent(type="text", text=f"Shot Analysis failed: {str(e)}")]
    
    async def _handle_war_analysis(self, args: Dict[str, Any]) -> List[types.TextContent]:
        """Handle Replacement Value (WAR) analysis"""
        if not self.war_estimator:
            return [types.TextContent(type="text", text="Replacement Value Estimator not available")]
        
        try:
            analysis_type = args["analysis_type"]
            if "season" not in args:
                return [types.TextContent(type="text", text="Error: Season parameter is required. Please specify a season (e.g., '2025', '2024', '2023')")]
            season = args["season"]
            
            if analysis_type == "replacement_baselines":
                baselines = self.war_estimator.calculate_replacement_baselines(season, args.get("min_minutes", 450))
                
                result = f"Replacement Level Baselines ({season}):\n\n"
                for position, stats in baselines['replacement_baselines'].items():
                    result += f"• {position.title()}: {stats['replacement_contribution_per_90']:.3f} contributions/90 (from {stats['total_players']} players)\n"
            
            elif analysis_type == "player_war":
                df = self.war_estimator.calculate_player_war_estimates(season, args.get("min_minutes", 450))
                
                result = f"Player WAR Estimates ({season}):\n\n"
                result += "Top WAR Performers:\n"
                for _, player in df.head(15).iterrows():
                    result += f"• {player['player_name']} ({player['team']}): {player['estimated_wins_above_replacement']:.2f} WAR ({player['value_tier']})\n"
            
            elif analysis_type == "team_construction":
                df = self.war_estimator.analyze_team_roster_construction(season)
                
                result = f"Team Roster Construction ({season}):\n\n"
                for _, team in df.head(10).iterrows():
                    result += f"• {team['team']}: {team['total_war']:.1f} total WAR, {team['roster_style']}\n"
            
            elif analysis_type == "undervalued_players":
                df = self.war_estimator.find_undervalued_players(season, args.get("min_war", 0.5))
                
                result = f"High-Value Players ({season}):\n\n"
                for _, player in df.head(15).iterrows():
                    result += f"• {player['player_name']} ({player['team']}): {player['estimated_wins_above_replacement']:.2f} WAR, {player['war_per_90']:.3f} WAR/90\n"
            
            return [types.TextContent(type="text", text=result)]
            
        except Exception as e:
            return [types.TextContent(type="text", text=f"WAR Analysis failed: {str(e)}")]
    
    # HTTP-compatible wrapper methods for compatibility with http_server_v2.py
    async def _get_raw_data(self, args: Dict[str, Any]) -> List[types.TextContent]:
        """HTTP wrapper for raw data queries"""
        return await self._handle_raw_query(args)
    
    async def _get_player_stats(self, args: Dict[str, Any]) -> List[types.TextContent]:
        """HTTP wrapper for player stats (basic implementation)"""
        try:
            if "season" not in args:
                return [types.TextContent(type="text", text="Error: Season parameter is required. Please specify a season (e.g., '2025', '2024', '2023')")]
            season = args["season"]
            player_name = args.get("player_name")
            team_name = self._normalize_team_name(args.get("team_name")) if args.get("team_name") else None
            limit = args.get("limit", 20)
            
            query = f"""
            SELECT player_name, team, goals, assists, minutes_played, 
                   expected_goals, expected_assists, shots_on_target_pct
            FROM `{self.project_id}.nwsl_fbref.player_stats_all_years`
            WHERE season = {int(season)}
            """
            
            if player_name:
                query += f" AND LOWER(player_name) LIKE '%{player_name.lower()}%'"
            if team_name:
                query += f" AND team = '{team_name}'"
            
            query += f" ORDER BY goals DESC LIMIT {limit}"
            
            df = self.bigquery_client.query(query).to_dataframe()
            
            result = f"Player Statistics ({season}):\n\n"
            for _, player in df.iterrows():
                result += f"• {player['player_name']} ({player['team']}): {player['goals']} goals, {player['assists']} assists, {player['minutes_played']} minutes\n"
            
            return [types.TextContent(type="text", text=result)]
            
        except Exception as e:
            return [types.TextContent(type="text", text=f"Player stats failed: {str(e)}")]
    
    async def _get_team_stats(self, args: Dict[str, Any]) -> List[types.TextContent]:
        """HTTP wrapper for team stats"""
        try:
            if "season" not in args:
                return [types.TextContent(type="text", text="Error: Season parameter is required. Please specify a season (e.g., '2025', '2024', '2023')")]
            season = args["season"]
            team_name = args.get("team_name")
            
            # Aggregate player stats to team level
            query = f"""
            SELECT team, 
                   SUM(goals) as total_goals,
                   SUM(assists) as total_assists,
                   SUM(expected_goals) as total_xg,
                   COUNT(*) as squad_size
            FROM `{self.project_id}.nwsl_fbref.player_stats_all_years`
            WHERE season = {int(season)}
            """
            
            if team_name:
                normalized_team = self._normalize_team_name(team_name)
                query += f" AND team = '{normalized_team}'"
            
            query += " GROUP BY team ORDER BY total_goals DESC"
            
            df = self.bigquery_client.query(query).to_dataframe()
            
            result = f"Team Statistics ({season}):\n\n"
            for _, team in df.iterrows():
                result += f"• {team['team']}: {team['total_goals']} goals, {team['total_xg']:.1f} xG, {team['squad_size']} players\n"
            
            return [types.TextContent(type="text", text=result)]
            
        except Exception as e:
            return [types.TextContent(type="text", text=f"Team stats failed: {str(e)}")]
    
    async def _get_standings(self, args: Dict[str, Any]) -> List[types.TextContent]:
        """HTTP wrapper for league standings"""
        try:
            if "season" not in args:
                return [types.TextContent(type="text", text="Error: Season parameter is required. Please specify a season (e.g., '2025', '2024', '2023')")]
            season = args["season"]
            
            # Calculate standings from team stats
            query = f"""
            SELECT team,
                   SUM(goals) as goals_for,
                   COUNT(*) as games_played
            FROM `{self.project_id}.nwsl_fbref.player_stats_all_years`
            WHERE season = {int(season)}
            GROUP BY team
            ORDER BY goals_for DESC
            """
            
            df = self.bigquery_client.query(query).to_dataframe()
            
            result = f"League Standings ({season}) - by Goals:\n\n"
            for i, team in df.iterrows():
                result += f"{i+1}. {team['team']}: {team['goals_for']} goals\n"
            
            return [types.TextContent(type="text", text=result)]
            
        except Exception as e:
            return [types.TextContent(type="text", text=f"Standings failed: {str(e)}")]
    
    async def _get_match_results(self, args: Dict[str, Any]) -> List[types.TextContent]:
        """HTTP wrapper for match results"""
        try:
            if "season" not in args:
                return [types.TextContent(type="text", text="Error: Season parameter is required. Please specify a season (e.g., '2025', '2024', '2023')")]
            season = args["season"]
            limit = args.get("limit", 10)
            
            result = f"Match Results ({season}):\n\n"
            result += "Note: Individual match data not available in current dataset.\n"
            result += "Available data includes season-long player statistics aggregated by team.\n"
            
            return [types.TextContent(type="text", text=result)]
            
        except Exception as e:
            return [types.TextContent(type="text", text=f"Match results failed: {str(e)}")]
    
    async def _analyze_player_performance(self, args: Dict[str, Any]) -> List[types.TextContent]:
        """HTTP wrapper for player performance analysis"""
        if "season" not in args:
            return [types.TextContent(type="text", text="Error: Season parameter is required. Please specify a season (e.g., '2025', '2024', '2023')")]
        player_name = args.get("player_name")
        season = args["season"]
        
        return await self._get_player_stats({"season": season, "player_name": player_name, "limit": 1})
    
    async def _analyze_team_performance(self, args: Dict[str, Any]) -> List[types.TextContent]:
        """HTTP wrapper for team performance analysis"""
        if "season" not in args:
            return [types.TextContent(type="text", text="Error: Season parameter is required. Please specify a season (e.g., '2025', '2024', '2023')")]
        team_name = args.get("team_name")
        season = args["season"]
        
        return await self._get_team_stats({"season": season, "team_name": team_name})
    
    async def _find_correlations(self, args: Dict[str, Any]) -> List[types.TextContent]:
        """HTTP wrapper for correlation analysis"""
        try:
            if "season" not in args:
                return [types.TextContent(type="text", text="Error: Season parameter is required. Please specify a season (e.g., '2025', '2024', '2023')")]
            analysis_type = args.get("analysis_type", "player_performance")
            season = args["season"]
            
            query = f"""
            SELECT 
                CORR(goals, expected_goals) as goals_xg_correlation,
                CORR(assists, expected_assists) as assists_xa_correlation,
                COUNT(*) as sample_size
            FROM `{self.project_id}.nwsl_fbref.player_stats_all_years`
            WHERE season = {int(season)} AND minutes_played > 450
            """
            
            df = self.bigquery_client.query(query).to_dataframe()
            
            result = f"Statistical Correlations ({season}):\n\n"
            row = df.iloc[0]
            result += f"• Goals vs xG correlation: {row['goals_xg_correlation']:.3f}\n"
            result += f"• Assists vs xA correlation: {row['assists_xa_correlation']:.3f}\n"
            result += f"• Sample size: {row['sample_size']} players\n"
            
            return [types.TextContent(type="text", text=result)]
            
        except Exception as e:
            return [types.TextContent(type="text", text=f"Correlation analysis failed: {str(e)}")]
    
    async def _compare_teams(self, args: Dict[str, Any]) -> List[types.TextContent]:
        """HTTP wrapper for team comparison"""
        if "season" not in args:
            return [types.TextContent(type="text", text="Error: Season parameter is required. Please specify a season (e.g., '2025', '2024', '2023')")]
        team1 = args.get("team1")
        team2 = args.get("team2")
        season = args["season"]
        
        team1_stats = await self._get_team_stats({"season": season, "team_name": team1})
        team2_stats = await self._get_team_stats({"season": season, "team_name": team2})
        
        result = f"Team Comparison ({season}):\n\n"
        result += f"{team1}:\n{team1_stats[0].text}\n"
        result += f"{team2}:\n{team2_stats[0].text}\n"
        
        return [types.TextContent(type="text", text=result)]
    
    async def _get_nwsl_players(self, args: Dict[str, Any]) -> List[types.TextContent]:
        """HTTP wrapper for NWSL player roster"""
        try:
            player_name = args.get("player_name")
            position = args.get("position")
            nationality = args.get("nationality")
            team_name = args.get("team_name")
            limit = args.get("limit", 50)
            
            query = f"""
            SELECT DISTINCT player_name, team, position, nationality
            FROM `{self.project_id}.nwsl_fbref.player_stats_all_years`
            WHERE 1=1
            """
            
            if player_name:
                query += f" AND LOWER(player_name) LIKE '%{player_name.lower()}%'"
            if position:
                query += f" AND position = '{position}'"
            if nationality:
                query += f" AND nationality = '{nationality}'"
            if team_name:
                normalized_team = self._normalize_team_name(team_name)
                query += f" AND team = '{normalized_team}'"
            
            query += f" ORDER BY player_name LIMIT {limit}"
            
            df = self.bigquery_client.query(query).to_dataframe()
            
            result = "NWSL Player Roster:\n\n"
            for _, player in df.iterrows():
                result += f"• {player['player_name']} ({player['team']}) - {player['position']}, {player['nationality']}\n"
            
            return [types.TextContent(type="text", text=result)]
            
        except Exception as e:
            return [types.TextContent(type="text", text=f"Player roster failed: {str(e)}")]
    
    async def _get_nwsl_teams(self, args: Dict[str, Any]) -> List[types.TextContent]:
        """HTTP wrapper for NWSL team information"""
        try:
            query = f"""
            SELECT DISTINCT team, COUNT(*) as squad_size
            FROM `{self.project_id}.nwsl_fbref.player_stats_all_years`
            GROUP BY team
            ORDER BY team
            """
            
            df = self.bigquery_client.query(query).to_dataframe()
            
            result = "NWSL Teams:\n\n"
            for _, team in df.iterrows():
                result += f"• {team['team']} ({team['squad_size']} players across all seasons)\n"
            
            return [types.TextContent(type="text", text=result)]
            
        except Exception as e:
            return [types.TextContent(type="text", text=f"Team info failed: {str(e)}")]
    
    async def _get_nwsl_games(self, args: Dict[str, Any]) -> List[types.TextContent]:
        """HTTP wrapper for NWSL games data"""
        try:
            if "season" not in args:
                return [types.TextContent(type="text", text="Error: Season parameter is required. Please specify a season (e.g., '2025', '2024', '2023')")]
            season = args["season"]
            team_name = args.get("team_name")
            limit = args.get("limit", 20)
            
            result = f"NWSL Games ({season}):\n\n"
            result += "Note: Individual game data not available in current dataset.\n"
            result += "Available data includes season-long player statistics.\n"
            
            return [types.TextContent(type="text", text=result)]
            
        except Exception as e:
            return [types.TextContent(type="text", text=f"Games data failed: {str(e)}")]
    
    async def _get_team_roster(self, args: Dict[str, Any]) -> List[types.TextContent]:
        """HTTP wrapper for team roster analysis with current roster intelligence"""
        try:
            if "season" not in args:
                return [types.TextContent(type="text", text="Error: Season parameter is required. Please specify a season (e.g., '2025', '2024', '2023')")]
            if "team" not in args:
                return [types.TextContent(type="text", text="Error: Team parameter is required. Please specify a team name (e.g., 'Courage', 'Current', 'Spirit')")]
            
            season = args["season"]
            team = args["team"]
            min_minutes = args.get("min_minutes", 200)  # Lower default for mid-season analysis
            sort_by = args.get("sort_by", "total_contributions")
            current_only = args.get("current_only", True)  # New parameter
            
            # Normalize team name
            normalized_team = self._normalize_team_name(team)
            
            # Map sort_by to actual column names
            sort_mapping = {
                "total_contributions": "(PERF_Gls + PERF_Ast)",
                "goals": "PERF_Gls",
                "assists": "PERF_Ast", 
                "expected_goals": "EXP_xG",
                "minutes_played": "PT_Min"
            }
            sort_column = sort_mapping.get(sort_by, "(PERF_Gls + PERF_Ast)")
            
            query = f"""
            SELECT 
                Player as player_name,
                Pos as position,
                PT_Min as minutes_played,
                PERF_Gls as goals,
                PERF_Ast as assists,
                EXP_xG as expected_goals,
                EXP_xAG as expected_assists,
                P90_Gls as goals_per_90,
                P90_Ast as assists_per_90,
                (PERF_Gls + PERF_Ast) as total_contributions,
                ROUND(PERF_Gls / NULLIF(EXP_xG, 0), 2) as goal_conversion_rate,
                ROUND(EXP_xG + EXP_xAG, 2) as total_expected_contributions
            FROM `{self.project_id}.nwsl_fbref.player_stats_all_years`
            WHERE Squad = '{normalized_team}' 
                AND season = {int(season)} 
                AND PT_Min >= {min_minutes}
            ORDER BY {sort_column} DESC
            """
            
            df = self.bigquery_client.query(query).to_dataframe()
            
            if df.empty:
                return [types.TextContent(type="text", text=f"No players found for {team} in {season} with minimum {min_minutes} minutes played.")]
            
            result = f"{team} Roster Analysis ({season}):\n"
            result += f"Players with {min_minutes}+ minutes (sorted by {sort_by}):\n\n"
            
            for _, player in df.iterrows():
                conversion = player['goal_conversion_rate'] if pd.notna(player['goal_conversion_rate']) else 0.0
                result += f"• {player['player_name']} ({player['position']}): "
                result += f"{player['goals']}G + {player['assists']}A = {player['total_contributions']} contributions, "
                result += f"{player['expected_goals']:.1f}xG + {player['expected_assists']:.1f}xA, "
                result += f"{player['minutes_played']:.0f} mins, {conversion:.2f} conversion rate\n"
            
            result += f"\nTeam Totals: {df['goals'].sum()}G + {df['assists'].sum()}A, "
            result += f"{df['expected_goals'].sum():.1f}xG + {df['expected_assists'].sum():.1f}xA"
            
            return [types.TextContent(type="text", text=result)]
            
        except Exception as e:
            return [types.TextContent(type="text", text=f"Team roster analysis failed: {str(e)}")]
    
    async def _roster_intelligence(self, args: Dict[str, Any]) -> List[types.TextContent]:
        """Advanced roster analysis with intelligent insights"""
        try:
            if "season" not in args or "team" not in args or "analysis_type" not in args:
                return [types.TextContent(type="text", text="Error: season, team, and analysis_type parameters are required")]
            
            season = args["season"]
            team = args["team"]
            analysis_type = args["analysis_type"]
            position_focus = args.get("position_focus")
            normalized_team = self._normalize_team_name(team)
            
            if analysis_type == "current_form":
                # Get players with recent activity, weighted by recency
                query = f"""
                SELECT 
                    Player as player_name,
                    Pos as position,
                    PT_Min as minutes_played,
                    PERF_Gls as goals,
                    PERF_Ast as assists,
                    EXP_xG as expected_goals,
                    P90_Gls as goals_per_90,
                    P90_Ast as assists_per_90,
                    ROUND(PERF_Gls / NULLIF(EXP_xG, 0), 3) as conversion_rate,
                    CASE 
                        WHEN PT_Min >= 900 THEN 'Regular Starter'
                        WHEN PT_Min >= 450 THEN 'Squad Player'
                        WHEN PT_Min >= 180 THEN 'Rotation Option'
                        ELSE 'Limited Minutes'
                    END as playing_time_status
                FROM `{self.project_id}.nwsl_fbref.player_stats_all_years`
                WHERE Squad = '{normalized_team}' AND season = {int(season)}
                ORDER BY PT_Min DESC
                """
                
                df = self.bigquery_client.query(query).to_dataframe()
                
                result = f"{team} Current Form Analysis ({season}):\n\n"
                result += "**PLAYING TIME BREAKDOWN:**\n"
                
                for status in ['Regular Starter', 'Squad Player', 'Rotation Option', 'Limited Minutes']:
                    players = df[df['playing_time_status'] == status]
                    if not players.empty:
                        result += f"\n{status} ({len(players)} players):\n"
                        for _, player in players.iterrows():
                            conv_rate = player['conversion_rate'] if pd.notna(player['conversion_rate']) else 0.0
                            result += f"• {player['player_name']} ({player['position']}): {player['minutes_played']:.0f} mins, "
                            result += f"{player['goals']}G+{player['assists']}A, {conv_rate:.2f} conversion\n"
                
                # Add team summary
                total_goals = df['goals'].sum()
                total_xg = df['expected_goals'].sum()
                team_conversion = total_goals / total_xg if total_xg > 0 else 0
                
                result += f"\n**TEAM SUMMARY:**\n"
                result += f"Total Goals: {total_goals}, Expected: {total_xg:.1f}, Conversion: {team_conversion:.2f}\n"
                result += f"Squad Size: {len(df)} players with minutes\n"
                
            elif analysis_type == "best_xi":
                # Suggest optimal starting XI based on contributions and form
                query = f"""
                WITH position_rankings AS (
                    SELECT 
                        Player,
                        Pos,
                        PT_Min,
                        PERF_Gls + PERF_Ast as contributions,
                        EXP_xG + EXP_xAG as expected_contributions,
                        CASE 
                            WHEN Pos LIKE '%GK%' THEN 'GK'
                            WHEN Pos LIKE '%DF%' OR Pos LIKE '%CB%' OR Pos LIKE '%LB%' OR Pos LIKE '%RB%' THEN 'DF'  
                            WHEN Pos LIKE '%MF%' OR Pos LIKE '%CM%' OR Pos LIKE '%DM%' OR Pos LIKE '%AM%' THEN 'MF'
                            WHEN Pos LIKE '%FW%' OR Pos LIKE '%ST%' OR Pos LIKE '%LW%' OR Pos LIKE '%RW%' THEN 'FW'
                            ELSE 'UTIL'
                        END as position_group,
                        ROW_NUMBER() OVER (
                            PARTITION BY CASE 
                                WHEN Pos LIKE '%GK%' THEN 'GK'
                                WHEN Pos LIKE '%DF%' OR Pos LIKE '%CB%' OR Pos LIKE '%LB%' OR Pos LIKE '%RB%' THEN 'DF'  
                                WHEN Pos LIKE '%MF%' OR Pos LIKE '%CM%' OR Pos LIKE '%DM%' OR Pos LIKE '%AM%' THEN 'MF'
                                WHEN Pos LIKE '%FW%' OR Pos LIKE '%ST%' OR Pos LIKE '%LW%' OR Pos LIKE '%RW%' THEN 'FW'
                                ELSE 'UTIL'
                            END 
                            ORDER BY (PERF_Gls + PERF_Ast + EXP_xG + EXP_xAG) DESC, PT_Min DESC
                        ) as position_rank
                    FROM `{self.project_id}.nwsl_fbref.player_stats_all_years`
                    WHERE Squad = '{normalized_team}' AND season = {int(season)} AND PT_Min >= 180
                )
                SELECT * FROM position_rankings 
                WHERE (position_group = 'GK' AND position_rank <= 1)
                   OR (position_group = 'DF' AND position_rank <= 4) 
                   OR (position_group = 'MF' AND position_rank <= 4)
                   OR (position_group = 'FW' AND position_rank <= 3)
                ORDER BY position_group, position_rank
                """
                
                df = self.bigquery_client.query(query).to_dataframe()
                
                result = f"{team} Optimal Starting XI ({season}):\n\n"
                
                for pos_group in ['GK', 'DF', 'MF', 'FW']:
                    players = df[df['position_group'] == pos_group]
                    if not players.empty:
                        result += f"**{pos_group}:**\n"
                        for _, player in players.iterrows():
                            result += f"• {player['Player']} ({player['Pos']}): {player['contributions']:.0f} contributions, "
                            result += f"{player['expected_contributions']:.1f} expected, {player['PT_Min']:.0f} mins\n"
                        result += "\n"
                
            elif analysis_type == "underperformers":
                # Find players significantly underperforming expectations
                query = f"""
                SELECT 
                    Player,
                    Pos,
                    PT_Min,
                    PERF_Gls as goals,
                    EXP_xG as expected_goals,
                    PERF_Gls - EXP_xG as goal_difference,
                    ROUND((PERF_Gls - EXP_xG) / NULLIF(EXP_xG, 0) * 100, 1) as underperformance_pct
                FROM `{self.project_id}.nwsl_fbref.player_stats_all_years`
                WHERE Squad = '{normalized_team}' 
                    AND season = {int(season)} 
                    AND PT_Min >= 300
                    AND EXP_xG >= 1.0
                    AND (PERF_Gls - EXP_xG) < -1.0
                ORDER BY (PERF_Gls - EXP_xG) ASC
                """
                
                df = self.bigquery_client.query(query).to_dataframe()
                
                result = f"{team} Underperforming Players ({season}):\n\n"
                if df.empty:
                    result += "No significant underperformers found (good sign!).\n"
                else:
                    result += "Players significantly below expected goals:\n"
                    for _, player in df.iterrows():
                        result += f"• {player['Player']} ({player['Pos']}): {player['goals']} goals from {player['expected_goals']:.1f} xG "
                        result += f"({player['goal_difference']:.1f} difference, {player['underperformance_pct']:.0f}% below expectation)\n"
            
            return [types.TextContent(type="text", text=result)]
            
        except Exception as e:
            return [types.TextContent(type="text", text=f"Roster intelligence analysis failed: {str(e)}")]
    
    async def _ingest_current_roster(self, args: Dict[str, Any]) -> List[types.TextContent]:
        """Ingest current roster data from FBref team pages"""
        try:
            if "team" not in args or "fbref_url" not in args:
                return [types.TextContent(type="text", text="Error: team and fbref_url parameters are required")]
            
            team = args["team"]
            fbref_url = args["fbref_url"]
            update_db = args.get("update_database", False)
            
            # Import requests and BeautifulSoup for web scraping
            try:
                import requests
                from bs4 import BeautifulSoup
                import re
            except ImportError:
                return [types.TextContent(type="text", text="Error: Web scraping dependencies not available. Need requests and beautifulsoup4.")]
            
            # Fetch the FBref page
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            try:
                response = requests.get(fbref_url, headers=headers, timeout=10)
                response.raise_for_status()
            except requests.RequestException as e:
                return [types.TextContent(type="text", text=f"Error fetching FBref page: {str(e)}")]
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find the standard stats table (usually the main roster table)
            stats_table = soup.find('table', {'id': 'stats_standard'}) or soup.find('table', class_='stats_table')
            
            if not stats_table:
                return [types.TextContent(type="text", text=f"Error: Could not find player stats table on {fbref_url}. Check URL format.")]
            
            current_roster = []
            
            # Parse table rows
            tbody = stats_table.find('tbody')
            if tbody:
                for row in tbody.find_all('tr'):
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 4:  # Ensure we have enough columns
                        try:
                            # Extract player data (adjust indices based on FBref table structure)
                            player_name = cells[0].get_text(strip=True) if len(cells) > 0 else ""
                            nation = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                            position = cells[2].get_text(strip=True) if len(cells) > 2 else ""
                            age = cells[3].get_text(strip=True) if len(cells) > 3 else ""
                            
                            # Get minutes played if available (usually around column 5-7)
                            minutes = ""
                            for i in range(4, min(len(cells), 10)):
                                cell_text = cells[i].get_text(strip=True)
                                if cell_text.isdigit() and int(cell_text) > 50:  # Likely minutes
                                    minutes = cell_text
                                    break
                            
                            if player_name and player_name != "Player":  # Skip header rows
                                current_roster.append({
                                    'player_name': player_name,
                                    'nation': nation,
                                    'position': position,
                                    'age': age,
                                    'minutes_2025': minutes or "0",
                                    'active_status': 'Current'
                                })
                        except Exception as e:
                            continue  # Skip malformed rows
            
            if not current_roster:
                return [types.TextContent(type="text", text=f"Error: No player data found in table. URL may be incorrect or page structure changed.")]
            
            # Format results
            result = f"Current {team} Roster (Ingested from FBref):\n\n"
            result += f"Found {len(current_roster)} active players:\n\n"
            
            # Group by position
            positions = {}
            for player in current_roster:
                pos = player['position'] if player['position'] else 'Unknown'
                if pos not in positions:
                    positions[pos] = []
                positions[pos].append(player)
            
            for pos, players in positions.items():
                result += f"**{pos}:**\n"
                for player in players:
                    age_text = f", Age {player['age']}" if player['age'] else ""
                    minutes_text = f", {player['minutes_2025']} mins" if player['minutes_2025'] != "0" else ""
                    result += f"• {player['player_name']} ({player['nation']}{age_text}{minutes_text})\n"
                result += "\n"
            
            if update_db:
                # TODO: Implement database update logic here
                result += f"Note: Database update requested but not yet implemented. "
                result += f"Currently showing scraped data for validation.\n"
            
            result += f"\nSource: {fbref_url}\n"
            result += f"Scraped: {len(current_roster)} players\n"
            result += f"This roster reflects current 2025 squad composition."
            
            return [types.TextContent(type="text", text=result)]
            
        except Exception as e:
            return [types.TextContent(type="text", text=f"Roster ingestion failed: {str(e)}")]
    
    def _register_resources(self):
        """Register resources (datasets, schemas)"""
        
        @self.server.list_resources()
        async def handle_list_resources() -> List[types.Resource]:
            return [
                types.Resource(
                    uri="bigquery://nwsl_fbref/player_stats_all_years",
                    name="NWSL Player Statistics (Live + Historical)",
                    description="Live 2025 season player statistics plus complete historical data (2021-2024). Updated regularly with current season performance including xG, progressive play, and advanced metrics. Last updated: After most recent match week.",
                    mimeType="application/json"
                ),
                types.Resource(
                    uri="bigquery://nwsl_fbref/team_season_analytics", 
                    name="Team Season Analytics (Current + Historical)",
                    description="Real-time team performance analytics for 2025 season plus historical comparisons. Updated after each match week with efficiency metrics, rankings, and tactical insights.",
                    mimeType="application/json"
                )
            ]
    
    def _register_prompts(self):
        """Register research-focused prompts"""
        
        @self.server.list_prompts()
        async def handle_list_prompts() -> List[types.Prompt]:
            return [
                types.Prompt(
                    name="nwsl_research_assistant",
                    title="NWSL Research Assistant",
                    description="AI assistant specialized in NWSL analytics research questions",
                    arguments=[
                        types.PromptArgument(
                            name="research_question",
                            description="Core research question to investigate",
                            required=True
                        )
                    ]
                )
            ]
        
        @self.server.get_prompt()
        async def handle_get_prompt(name: str, arguments: Dict[str, str]) -> types.GetPromptResult:
            if name == "nwsl_research_assistant":
                research_question = arguments.get("research_question", "")
                
                return types.GetPromptResult(
                    description="NWSL Research Assistant for data-driven soccer analytics",
                    messages=[
                        types.PromptMessage(
                            role="system",
                            content=types.TextContent(
                                type="text",
                                text=f"""You are "NWSL Knowledge Engine," a specialized AI assistant for National Women's Soccer League analytics research.

Research Question: {research_question}

CURRENT CONTEXT: It is July 2025. The 2025 NWSL season is currently in progress. You have access to real-time player and team statistics that are updated regularly as games are played.

Core Research Framework:
1. What truly generates goals? (xG analysis, shot quality, creation patterns)
2. How do events translate into points? (win expectancy, situational leverage)
3. What is "replacement level" in soccer? (WAR, value above replacement)
4. How do we separate skill from luck? (statistical stability, sample sizes)
5. How do context and environment modulate performance? (normalization factors)
6. How do defensive and transition contributions work? (prevention metrics)

Available Tools:
- expected_goals_analysis: Analyze xG patterns, efficiency, overperformers
- shot_quality_analysis: Break down shooting by quality, volume, position
- replacement_value_analysis: Calculate WAR and roster construction value
- query_raw_data: Custom SQL analysis of NWSL datasets

When answering:
- Map the question to core research areas
- Use appropriate analytical tools with clear parameters
- Provide both raw numbers and interpretations
- Always specify data recency (e.g., "as of latest data update")
- Focus on objective, evidence-based insights
- Explain statistical significance when relevant
- For 2025 season analysis, note that data represents the season in progress

Data Coverage: 
- CURRENT SEASON (2025): Live player and team statistics updated regularly
- HISTORICAL SEASONS (2021-2024): Complete season data
- Available metrics: xG, progressive play, defensive actions, team performance
- Data sources: FBref professional statistics, updated after each match week"""
                            )
                        )
                    ]
                )
            else:
                raise ValueError(f"Unknown prompt: {name}")

async def main():
    """Run the NWSL Analytics MCP Server"""
    server = NWSLAnalyticsServer()
    
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="nwsl-analytics-research",
                server_version="1.0.0",
                capabilities=server.server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={}
                )
            )
        )

if __name__ == "__main__":
    asyncio.run(main())