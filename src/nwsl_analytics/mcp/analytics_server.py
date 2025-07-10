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
            WHERE season = '{season}'
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
            WHERE season = '{season}'
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
            WHERE season = '{season}'
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
            WHERE season = '{season}' AND minutes_played > 450
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