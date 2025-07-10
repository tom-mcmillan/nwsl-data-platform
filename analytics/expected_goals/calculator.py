#!/usr/bin/env python3
"""
ExpectedGoalsCalculator - NWSL Analytics Tool
Computes xG and analyzes goal generation patterns using BigQuery player data

Research Question: What truly generates goals?
- Which on-ball actions contribute most to goal probability?
- How do player contexts affect xG generation?
"""

import pandas as pd
from google.cloud import bigquery
from typing import Dict, List, Optional, Tuple
import numpy as np
from datetime import datetime

class ExpectedGoalsCalculator:
    """
    Analyzes expected goals data from NWSL player statistics
    Provides insights into goal generation patterns and player efficiency
    """
    
    def __init__(self, project_id: str = "nwsl-data"):
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)
        
    def get_player_xg_analysis(self, player_name: Optional[str] = None, 
                              season: Optional[str] = None,
                              team: Optional[str] = None) -> pd.DataFrame:
        """
        Get comprehensive xG analysis for players
        
        Args:
            player_name: Specific player to analyze (optional)
            season: Season to filter (optional) 
            team: Team to filter (optional)
            
        Returns:
            DataFrame with xG analysis including efficiency metrics
        """
        
        # Build WHERE clause dynamically
        where_conditions = []
        if player_name:
            where_conditions.append(f"Player LIKE '%{player_name}%'")
        if season:
            where_conditions.append(f"season = '{season}'")
        if team:
            # Handle both full names and short names
            if team.lower() in ['north carolina courage', 'nc courage', 'courage']:
                where_conditions.append("Squad = 'Courage'")
            elif team.lower() in ['chicago red stars', 'red stars']:
                where_conditions.append("Squad = 'Red Stars'")
            elif team.lower() in ['houston dash', 'dash']:
                where_conditions.append("Squad = 'Dash'")
            else:
                where_conditions.append(f"Squad = '{team}'")
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        query = f"""
        SELECT 
          Player as player_name,
          Squad as team,
          season,
          Pos as position,
          Age as age,
          
          -- Core Goal Statistics
          PERF_Gls as goals,
          PERF_Ast as assists,
          PERF_G_plus_A as goal_contributions,
          
          -- Expected Statistics
          EXP_xG as expected_goals,
          EXP_npxG as non_penalty_xg,
          EXP_xAG as expected_assists,
          EXP_npxG_plus_xAG as total_non_pen_expected,
          
          -- Playing Time Context
          PT_Min as minutes_played,
          PT_90s as matches_90s,
          
          -- Per 90 Rates
          P90_Gls as goals_per_90,
          P90_Ast as assists_per_90,
          P90_xG as xg_per_90,
          P90_xAG as xag_per_90,
          
          -- Goal Generation Efficiency
          CASE 
            WHEN EXP_xG > 0 THEN PERF_Gls / EXP_xG 
            ELSE NULL 
          END as goal_conversion_rate,
          
          CASE 
            WHEN EXP_xAG > 0 THEN PERF_Ast / EXP_xAG 
            ELSE NULL 
          END as assist_conversion_rate,
          
          -- Goal Generation Above/Below Expected
          PERF_Gls - EXP_xG as goals_vs_expected,
          PERF_Ast - EXP_xAG as assists_vs_expected,
          (PERF_Gls + PERF_Ast) - (EXP_xG + EXP_xAG) as total_vs_expected,
          
          -- Penalty Context
          PERF_PK as penalties_scored,
          PERF_PKatt as penalties_attempted,
          EXP_xG - EXP_npxG as penalty_xg,
          
          -- Data Quality
          '{datetime.now().isoformat()}' as analysis_timestamp
          
        FROM `{self.project_id}.nwsl_fbref.player_stats_all_years`
        {where_clause}
        ORDER BY EXP_xG DESC, goals DESC
        """
        
        return self.client.query(query).to_dataframe()
    
    def analyze_goal_generation_patterns(self, season: str = "2024") -> Dict:
        """
        Analyze league-wide goal generation patterns
        
        Research Focus: What truly generates goals at the league level?
        """
        
        query = f"""
        WITH league_analysis AS (
          SELECT 
            COUNT(*) as total_players,
            SUM(PERF_Gls) as total_goals,
            SUM(EXP_xG) as total_expected_goals,
            SUM(PT_Min) as total_minutes,
            
            -- Goal Generation Rates
            SUM(PERF_Gls) / SUM(PT_Min) * 90 as league_goals_per_90,
            SUM(EXP_xG) / SUM(PT_Min) * 90 as league_xg_per_90,
            
            -- Conversion Analysis
            SUM(PERF_Gls) / NULLIF(SUM(EXP_xG), 0) as league_conversion_rate,
            
            -- Distribution Analysis
            STDDEV(P90_Gls) as goals_per_90_std,
            STDDEV(P90_xG) as xg_per_90_std,
            
            -- Top Performers
            MAX(P90_Gls) as max_goals_per_90,
            MAX(P90_xG) as max_xg_per_90
            
          FROM `{self.project_id}.nwsl_fbref.player_stats_all_years`
          WHERE season = '{season}' AND PT_Min >= 450  -- Min 5 matches worth
        ),
        
        position_analysis AS (
          SELECT 
            CASE 
              WHEN Pos LIKE '%FW%' THEN 'Forward'
              WHEN Pos LIKE '%MF%' THEN 'Midfielder'  
              WHEN Pos LIKE '%DF%' THEN 'Defender'
              WHEN Pos LIKE '%GK%' THEN 'Goalkeeper'
              ELSE 'Other'
            END as position_group,
            
            COUNT(*) as players_count,
            AVG(P90_Gls) as avg_goals_per_90,
            AVG(P90_xG) as avg_xg_per_90,
            AVG(PERF_Gls / NULLIF(EXP_xG, 0)) as avg_conversion_rate,
            SUM(PERF_Gls) as total_goals,
            SUM(EXP_xG) as total_xg
            
          FROM `{self.project_id}.nwsl_fbref.player_stats_all_years`
          WHERE season = '{season}' AND PT_Min >= 450
          GROUP BY position_group
        )
        
        SELECT 'league' as analysis_type, 
               CAST(total_players AS STRING) as metric_value,
               'total_players' as metric_name
        FROM league_analysis
        
        UNION ALL
        
        SELECT 'league' as analysis_type,
               CAST(ROUND(league_goals_per_90, 3) AS STRING) as metric_value, 
               'goals_per_90' as metric_name
        FROM league_analysis
        
        UNION ALL
        
        SELECT 'league' as analysis_type,
               CAST(ROUND(league_conversion_rate, 3) AS STRING) as metric_value,
               'conversion_rate' as metric_name  
        FROM league_analysis
        
        UNION ALL
        
        SELECT 'position' as analysis_type,
               CONCAT(position_group, ': ', ROUND(avg_goals_per_90, 3)) as metric_value,
               'avg_goals_per_90_by_position' as metric_name
        FROM position_analysis
        ORDER BY analysis_type, metric_name
        """
        
        df = self.client.query(query).to_dataframe()
        
        # Convert to structured dict
        result = {
            'season': season,
            'analysis_timestamp': datetime.now().isoformat(),
            'league_metrics': {},
            'position_metrics': {}
        }
        
        for _, row in df.iterrows():
            if row['analysis_type'] == 'league':
                result['league_metrics'][row['metric_name']] = row['metric_value']
            else:
                if 'position_breakdown' not in result['position_metrics']:
                    result['position_metrics']['position_breakdown'] = []
                result['position_metrics']['position_breakdown'].append(row['metric_value'])
        
        return result
    
    def find_xg_overperformers(self, season: str = "2024", min_minutes: int = 900) -> pd.DataFrame:
        """
        Find players who significantly over/under-perform their xG
        
        Research Focus: How do we separate skill from luck?
        """
        
        query = f"""
        SELECT 
          Player as player_name,
          Squad as team,
          Pos as position,
          PT_Min as minutes_played,
          PT_90s as matches_90s,
          
          PERF_Gls as actual_goals,
          EXP_xG as expected_goals,
          PERF_Gls - EXP_xG as goals_vs_expected,
          
          CASE 
            WHEN EXP_xG > 0 THEN PERF_Gls / EXP_xG 
            ELSE NULL 
          END as conversion_rate,
          
          -- Statistical significance (rough estimate)
          CASE 
            WHEN EXP_xG >= 3 AND ABS(PERF_Gls - EXP_xG) >= 2 THEN 'Significant'
            WHEN EXP_xG >= 1.5 AND ABS(PERF_Gls - EXP_xG) >= 1 THEN 'Moderate'
            ELSE 'Minimal'
          END as significance_level,
          
          -- Context
          P90_Gls as goals_per_90,
          P90_xG as xg_per_90,
          
          -- Classification
          CASE 
            WHEN PERF_Gls - EXP_xG >= 2 THEN 'Major Overperformer'
            WHEN PERF_Gls - EXP_xG >= 1 THEN 'Overperformer'
            WHEN PERF_Gls - EXP_xG <= -2 THEN 'Major Underperformer'
            WHEN PERF_Gls - EXP_xG <= -1 THEN 'Underperformer'
            ELSE 'Expected'
          END as performance_category
          
        FROM `{self.project_id}.nwsl_fbref.player_stats_all_years`
        WHERE season = '{season}' 
          AND PT_Min >= {min_minutes}
          AND EXP_xG > 0.5  -- Minimum threshold for meaningful analysis
        ORDER BY goals_vs_expected DESC
        """
        
        return self.client.query(query).to_dataframe()
    
    def calculate_team_xg_efficiency(self, season: str = "2024") -> pd.DataFrame:
        """
        Calculate team-level xG efficiency and goal generation patterns
        """
        
        query = f"""
        SELECT 
          Squad as team_name,
          COUNT(*) as squad_size,
          
          -- Team Totals
          SUM(PERF_Gls) as total_goals,
          SUM(EXP_xG) as total_xg,
          SUM(PERF_Ast) as total_assists,
          SUM(EXP_xAG) as total_xag,
          
          -- Team Efficiency
          SUM(PERF_Gls) / NULLIF(SUM(EXP_xG), 0) as team_conversion_rate,
          SUM(PERF_Ast) / NULLIF(SUM(EXP_xAG), 0) as team_assist_efficiency,
          
          -- Goal Generation vs Expected
          SUM(PERF_Gls) - SUM(EXP_xG) as goals_vs_expected,
          SUM(PERF_Ast) - SUM(EXP_xAG) as assists_vs_expected,
          
          -- Per 90 Team Rates (weighted by minutes)
          SUM(P90_Gls * PT_90s) / NULLIF(SUM(PT_90s), 0) as team_goals_per_90,
          SUM(P90_xG * PT_90s) / NULLIF(SUM(PT_90s), 0) as team_xg_per_90,
          
          -- Squad Depth Analysis
          COUNT(CASE WHEN PERF_Gls >= 5 THEN 1 END) as goalscorers_5plus,
          COUNT(CASE WHEN EXP_xG >= 3 THEN 1 END) as high_xg_players,
          
          -- Playing Time Distribution
          SUM(PT_Min) as total_team_minutes,
          AVG(PT_Min) as avg_minutes_per_player,
          STDDEV(PT_Min) as minutes_distribution_std
          
        FROM `{self.project_id}.nwsl_fbref.player_stats_all_years`
        WHERE season = '{season}' AND Squad IS NOT NULL
        GROUP BY Squad
        ORDER BY total_xg DESC
        """
        
        return self.client.query(query).to_dataframe()

def main():
    """Test the ExpectedGoalsCalculator"""
    print("🎯 Testing NWSL Expected Goals Calculator")
    print("=" * 50)
    
    calc = ExpectedGoalsCalculator()
    
    # Test 1: League-wide goal generation patterns
    print("📊 Analyzing 2024 league-wide goal generation...")
    patterns = calc.analyze_goal_generation_patterns("2024")
    
    print("League Metrics:")
    for metric, value in patterns['league_metrics'].items():
        print(f"  {metric}: {value}")
    
    print(f"\nPosition Breakdown:")
    for breakdown in patterns.get('position_metrics', {}).get('position_breakdown', []):
        print(f"  {breakdown}")
    
    # Test 2: Top xG performers
    print(f"\n🏆 Top xG performers in 2024:")
    top_performers = calc.get_player_xg_analysis(season="2024").head(10)
    for _, player in top_performers.iterrows():
        print(f"  {player['player_name']} ({player['team']}): {player['expected_goals']:.2f} xG, {player['goals']} goals")
    
    # Test 3: Over/underperformers
    print(f"\n📈 Biggest xG overperformers in 2024:")
    overperformers = calc.find_xg_overperformers("2024").head(5)
    for _, player in overperformers.iterrows():
        print(f"  {player['player_name']}: +{player['goals_vs_expected']:.2f} vs expected ({player['performance_category']})")
    
    print(f"\n✅ ExpectedGoalsCalculator test complete!")

if __name__ == "__main__":
    main()