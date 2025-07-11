#!/usr/bin/env python3
"""
ShotQualityProfiler - NWSL Analytics Tool
Breaks down shot data and goal creation patterns using BigQuery player data

Research Question: What truly generates goals?
- Which on-ball actions contribute most to goal probability?
- How do different shot contexts affect conversion rates?
"""

import pandas as pd
from google.cloud import bigquery
from typing import Dict, List, Optional, Tuple
import numpy as np
from datetime import datetime

class ShotQualityProfiler:
    """
    Analyzes shot quality and goal creation patterns from NWSL player statistics
    Provides insights into shooting efficiency and goal generation contexts
    """
    
    def __init__(self, project_id: str = "nwsl-data"):
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)
        
    def analyze_shooting_profiles(self, season: str, min_minutes: int = 450) -> pd.DataFrame:
        """
        Analyze player shooting profiles and shot quality metrics
        
        Args:
            season: Season to analyze
            min_minutes: Minimum minutes played threshold
            
        Returns:
            DataFrame with comprehensive shooting analysis
        """
        
        query = f"""
        WITH shooting_analysis AS (
          SELECT 
            Player as player_name,
            Squad as team,
            Pos as position,
            PT_Min as minutes_played,
            PT_90s as matches_90s,
            
            -- Core Shooting Stats
            PERF_Gls as goals,
            EXP_xG as expected_goals,
            EXP_npxG as non_penalty_xg,
            
            -- Shot Volume & Quality (derived metrics)
            CASE 
              WHEN P90_xG > 0 THEN P90_xG * 90 / PT_Min * PT_Min 
              ELSE EXP_xG 
            END as total_shots_estimated,
            
            -- Shooting Efficiency Metrics  
            CASE 
              WHEN EXP_xG > 0 THEN PERF_Gls / EXP_xG 
              ELSE NULL 
            END as shot_conversion_rate,
            
            -- Shot Quality (xG per shot estimate)
            CASE 
              WHEN P90_xG > 0 AND PT_Min >= 90 THEN 
                EXP_xG / (P90_xG * PT_Min / 90) 
              ELSE NULL 
            END as avg_shot_quality,
            
            -- Rate Statistics
            P90_Gls as goals_per_90,
            P90_xG as xg_per_90,
            
            -- Performance vs Expected
            PERF_Gls - EXP_xG as goals_vs_expected,
            
            -- Penalty Context
            PERF_PK as penalties_scored,
            PERF_PKatt as penalties_attempted,
            EXP_xG - EXP_npxG as penalty_xg_value,
            
            -- Shot Creation Context (assist relationship)
            PERF_Ast as assists_provided,
            EXP_xAG as expected_assists,
            
            -- Classification
            CASE 
              WHEN P90_xG >= 0.5 THEN 'High Volume Shooter'
              WHEN P90_xG >= 0.25 THEN 'Medium Volume Shooter'
              WHEN P90_xG > 0 THEN 'Low Volume Shooter'
              ELSE 'Non-Shooter'
            END as shooter_type,
            
            CASE 
              WHEN EXP_xG > 0 AND PERF_Gls / EXP_xG >= 1.5 THEN 'Clinical Finisher'
              WHEN EXP_xG > 0 AND PERF_Gls / EXP_xG >= 1.1 THEN 'Above Average Finisher'
              WHEN EXP_xG > 0 AND PERF_Gls / EXP_xG >= 0.9 THEN 'Average Finisher'
              WHEN EXP_xG > 0 THEN 'Below Average Finisher'
              ELSE 'No Shot Data'
            END as finishing_quality
            
          FROM `{self.project_id}.nwsl_fbref.player_stats_all_years`
          WHERE season = {int(season)} AND PT_Min >= {min_minutes}
        )
        
        SELECT *,
          -- Shot Quality Percentiles
          PERCENT_RANK() OVER (ORDER BY avg_shot_quality) as shot_quality_percentile,
          PERCENT_RANK() OVER (ORDER BY xg_per_90) as shot_volume_percentile,
          PERCENT_RANK() OVER (ORDER BY shot_conversion_rate) as finishing_percentile
          
        FROM shooting_analysis
        WHERE EXP_xG > 0  -- Only players with shooting data
        ORDER BY xg_per_90 DESC, shot_conversion_rate DESC
        """
        
        return self.client.query(query).to_dataframe()
    
    def analyze_positional_shooting_patterns(self, season: str) -> Dict:
        """
        Analyze how shooting patterns vary by position
        
        Research Focus: Which positions contribute most to goal probability?
        """
        
        query = f"""
        WITH position_shooting AS (
          SELECT 
            CASE 
              WHEN Pos LIKE '%FW%' THEN 'Forward'
              WHEN Pos LIKE '%MF%' AND Pos LIKE '%FW%' THEN 'Attacking Midfielder'
              WHEN Pos LIKE '%MF%' THEN 'Midfielder'
              WHEN Pos LIKE '%DF%' AND Pos LIKE '%MF%' THEN 'Defensive Midfielder'  
              WHEN Pos LIKE '%DF%' THEN 'Defender'
              WHEN Pos LIKE '%GK%' THEN 'Goalkeeper'
              ELSE 'Other'
            END as position_group,
            
            COUNT(*) as player_count,
            
            -- Shooting Volume
            AVG(P90_xG) as avg_xg_per_90,
            SUM(EXP_xG) as total_xg,
            SUM(PERF_Gls) as total_goals,
            
            -- Shooting Quality  
            AVG(PERF_Gls / NULLIF(EXP_xG, 0)) as avg_conversion_rate,
            STDDEV(PERF_Gls / NULLIF(EXP_xG, 0)) as conversion_rate_std,
            
            -- Goal Generation Efficiency
            SUM(PERF_Gls) / NULLIF(SUM(EXP_xG), 0) as position_conversion_rate,
            SUM(PERF_Gls - EXP_xG) as total_goals_vs_expected,
            
            -- Context
            AVG(PT_Min) as avg_minutes,
            SUM(PT_Min) as total_minutes,
            
            -- Distribution Analysis
            MAX(P90_xG) as max_xg_per_90,
            MIN(P90_xG) as min_xg_per_90,
            
            -- Top Performers Count
            COUNT(CASE WHEN P90_xG >= 0.5 THEN 1 END) as high_volume_shooters,
            COUNT(CASE WHEN PERF_Gls / NULLIF(EXP_xG, 0) >= 1.2 THEN 1 END) as clinical_finishers
            
          FROM `{self.project_id}.nwsl_fbref.player_stats_all_years`
          WHERE season = {int(season)} 
            AND PT_Min >= 450
            AND EXP_xG > 0
          GROUP BY position_group
        )
        
        SELECT *,
          -- Position Contribution to League
          total_xg / SUM(total_xg) OVER () as xg_share_of_league,
          total_goals / SUM(total_goals) OVER () as goals_share_of_league,
          
          -- Efficiency Rankings
          RANK() OVER (ORDER BY position_conversion_rate DESC) as conversion_rank,
          RANK() OVER (ORDER BY avg_xg_per_90 DESC) as volume_rank
          
        FROM position_shooting
        ORDER BY total_xg DESC
        """
        
        df = self.client.query(query).to_dataframe()
        
        return {
            'season': season,
            'analysis_timestamp': datetime.now().isoformat(),
            'position_data': df.to_dict('records'),
            'summary': {
                'total_positions_analyzed': len(df),
                'highest_volume_position': df.loc[df['avg_xg_per_90'].idxmax(), 'position_group'],
                'most_clinical_position': df.loc[df['position_conversion_rate'].idxmax(), 'position_group'],
                'total_league_xg': df['total_xg'].sum(),
                'total_league_goals': df['total_goals'].sum()
            }
        }
    
    def find_shot_quality_leaders(self, season: str, min_shots: float = 2.0) -> pd.DataFrame:
        """
        Find players with the highest quality shot generation
        
        Research Focus: How do we separate skill from luck in shooting?
        """
        
        query = f"""
        WITH shot_quality_analysis AS (
          SELECT 
            Player as player_name,
            Squad as team,
            Pos as position,
            PT_Min as minutes_played,
            
            -- Shot Quality Metrics
            EXP_xG as total_xg,
            P90_xG as xg_per_90,
            
            -- Estimated shot quality (xG per shot)
            CASE 
              WHEN P90_xG > 0 AND PT_Min >= 90 THEN 
                EXP_xG / (P90_xG * PT_Min / 90)
              ELSE NULL 
            END as estimated_xg_per_shot,
            
            -- Finishing Performance
            PERF_Gls as goals,
            CASE 
              WHEN EXP_xG > 0 THEN PERF_Gls / EXP_xG 
              ELSE NULL 
            END as conversion_rate,
            
            -- Shot Volume Category
            CASE 
              WHEN P90_xG >= 0.6 THEN 'Very High Volume'
              WHEN P90_xG >= 0.4 THEN 'High Volume'
              WHEN P90_xG >= 0.2 THEN 'Medium Volume'
              ELSE 'Low Volume'
            END as volume_category,
            
            -- Quality vs Volume Balance
            P90_xG * CASE 
              WHEN P90_xG > 0 AND PT_Min >= 90 THEN 
                EXP_xG / (P90_xG * PT_Min / 90)
              ELSE 0
            END as quality_volume_score,
            
            -- Performance Classification
            CASE 
              WHEN PERF_Gls - EXP_xG >= 2 THEN 'Major Overperformer'
              WHEN PERF_Gls - EXP_xG >= 1 THEN 'Overperformer'  
              WHEN ABS(PERF_Gls - EXP_xG) < 1 THEN 'Expected'
              WHEN PERF_Gls - EXP_xG <= -2 THEN 'Major Underperformer'
              ELSE 'Underperformer'
            END as performance_vs_expected
            
          FROM `{self.project_id}.nwsl_fbref.player_stats_all_years`
          WHERE season = {int(season)} 
            AND PT_Min >= 450
            AND EXP_xG >= {min_shots}  -- Minimum total xG threshold
        )
        
        SELECT *,
          -- Percentile Rankings
          PERCENT_RANK() OVER (ORDER BY estimated_xg_per_shot) as shot_quality_percentile,
          PERCENT_RANK() OVER (ORDER BY xg_per_90) as shot_volume_percentile,
          PERCENT_RANK() OVER (ORDER BY conversion_rate) as finishing_percentile,
          PERCENT_RANK() OVER (ORDER BY quality_volume_score) as overall_shooting_percentile
          
        FROM shot_quality_analysis
        WHERE estimated_xg_per_shot IS NOT NULL
        ORDER BY quality_volume_score DESC, estimated_xg_per_shot DESC
        """
        
        return self.client.query(query).to_dataframe()
    
    def analyze_team_shooting_styles(self, season: str) -> pd.DataFrame:
        """
        Analyze team-level shooting styles and patterns
        """
        
        query = f"""
        WITH team_shooting AS (
          SELECT 
            Squad as team_name,
            
            -- Team Shooting Volume
            COUNT(*) as squad_shooters,
            SUM(EXP_xG) as team_total_xg,
            SUM(PERF_Gls) as team_total_goals,
            AVG(P90_xG) as avg_player_xg_per_90,
            
            -- Team Shooting Distribution
            STDDEV(P90_xG) as xg_distribution_std,
            MAX(P90_xG) as top_shooter_xg_per_90,
            
            -- Team Shooting Quality
            SUM(PERF_Gls) / NULLIF(SUM(EXP_xG), 0) as team_conversion_rate,
            
            -- Squad Depth in Shooting
            COUNT(CASE WHEN P90_xG >= 0.3 THEN 1 END) as regular_shooters,
            COUNT(CASE WHEN P90_xG >= 0.6 THEN 1 END) as high_volume_shooters,
            COUNT(CASE WHEN PERF_Gls >= 5 THEN 1 END) as prolific_goalscorers,
            
            -- Minutes Distribution
            SUM(PT_Min) as total_team_minutes,
            
            -- Shooting Style Indicators
            SUM(EXP_xG) / SUM(PT_Min) * 90 as team_xg_per_90_weighted,
            SUM(PERF_Gls) / SUM(PT_Min) * 90 as team_goals_per_90_weighted
            
          FROM `{self.project_id}.nwsl_fbref.player_stats_all_years`
          WHERE season = {int(season)} 
            AND Squad IS NOT NULL
            AND PT_Min >= 90  -- At least 1 match worth
          GROUP BY Squad
        )
        
        SELECT *,
          -- Team Style Classification
          CASE 
            WHEN regular_shooters >= 8 THEN 'Distributed Attack'
            WHEN high_volume_shooters >= 3 THEN 'Multiple Threat Attack'
            WHEN top_shooter_xg_per_90 >= 1.0 THEN 'Star-Driven Attack'
            ELSE 'Conservative Attack'
          END as attacking_style,
          
          CASE 
            WHEN team_conversion_rate >= 1.2 THEN 'Clinical'
            WHEN team_conversion_rate >= 1.0 THEN 'Efficient'
            WHEN team_conversion_rate >= 0.9 THEN 'Average'
            ELSE 'Wasteful'
          END as finishing_quality,
          
          -- League Rankings
          RANK() OVER (ORDER BY team_total_xg DESC) as xg_rank,
          RANK() OVER (ORDER BY team_total_goals DESC) as goals_rank,
          RANK() OVER (ORDER BY team_conversion_rate DESC) as efficiency_rank
          
        FROM team_shooting
        ORDER BY team_total_xg DESC
        """
        
        return self.client.query(query).to_dataframe()

def main():
    """Test the ShotQualityProfiler"""
    print("🎯 Testing NWSL Shot Quality Profiler")
    print("=" * 50)
    
    profiler = ShotQualityProfiler()
    
    # Test 1: Top shooting profiles
    print("📊 Analyzing top shooting profiles in 2024...")
    profiles = profiler.analyze_shooting_profiles("2024").head(10)
    
    for _, player in profiles.iterrows():
        print(f"  {player['player_name']} ({player['team']}): {player['xg_per_90']:.2f} xG/90, "
              f"{player['shot_conversion_rate']:.2f} conversion ({player['finishing_quality']})")
    
    # Test 2: Positional shooting patterns
    print(f"\n🏆 Positional shooting patterns:")
    patterns = profiler.analyze_positional_shooting_patterns("2024")
    
    for pos_data in patterns['position_data']:
        print(f"  {pos_data['position_group']}: {pos_data['avg_xg_per_90']:.2f} xG/90, "
              f"{pos_data['position_conversion_rate']:.2f} conversion rate")
    
    # Test 3: Shot quality leaders
    print(f"\n📈 Shot quality leaders:")
    leaders = profiler.find_shot_quality_leaders("2024").head(5)
    
    for _, player in leaders.iterrows():
        print(f"  {player['player_name']}: {player['estimated_xg_per_shot']:.3f} xG/shot, "
              f"{player['volume_category']} ({player['performance_vs_expected']})")
    
    print(f"\n✅ ShotQualityProfiler test complete!")

if __name__ == "__main__":
    main()