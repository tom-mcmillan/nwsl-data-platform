#!/usr/bin/env python3
"""
ReplacementValueEstimator - NWSL Analytics Tool
Calculates player value above replacement level using BigQuery player data

Research Question: What is "replacement level" in soccer?
- How do you define a "replacement" player by position?
- How many "expected points above replacement" does a championship roster need?
"""

import pandas as pd
from google.cloud import bigquery
from typing import Dict, List, Optional, Tuple
import numpy as np
from datetime import datetime

class ReplacementValueEstimator:
    """
    Estimates player value above replacement level for NWSL players
    Provides WAR-style metrics adapted for soccer using available statistics
    """
    
    def __init__(self, project_id: str = "nwsl-data"):
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)
        
        # Define replacement level thresholds by position
        self.replacement_percentiles = {
            'forward': 0.25,      # Bottom 25% of forwards with significant minutes
            'midfielder': 0.30,    # Bottom 30% of midfielders  
            'defender': 0.35,     # Bottom 35% of defenders
            'goalkeeper': 0.20    # Bottom 20% of goalkeepers (smaller sample)
        }
        
    def calculate_replacement_baselines(self, season: str, min_minutes: int = 450) -> Dict:
        """
        Calculate replacement level baselines for each position
        
        Args:
            season: Season to analyze
            min_minutes: Minimum minutes to be considered for replacement level
            
        Returns:
            Dict with replacement level statistics by position
        """
        
        query = f"""
        WITH position_stats AS (
          SELECT 
            CASE 
              WHEN Pos LIKE '%FW%' THEN 'forward'
              WHEN Pos LIKE '%MF%' THEN 'midfielder'
              WHEN Pos LIKE '%DF%' THEN 'defender'
              WHEN Pos LIKE '%GK%' THEN 'goalkeeper'
              ELSE 'other'
            END as position_group,
            
            Player as player_name,
            Squad as team,
            PT_Min as minutes_played,
            PT_90s as matches_90s,
            
            -- Offensive Contributions
            P90_Gls as goals_per_90,
            P90_Ast as assists_per_90,
            P90_xG as xg_per_90,
            P90_xAG as xag_per_90,
            PERF_G_plus_A as total_goal_contributions,
            EXP_npxG_plus_xAG as total_expected_contributions,
            
            -- Advanced Metrics
            PROG_PrgC as progressive_carries,
            PROG_PrgP as progressive_passes,
            PROG_PrgR as progressive_receptions,
            
            -- Discipline (negative value)
            PERF_CrdY as yellow_cards,
            PERF_CrdR as red_cards,
            
            -- Per 90 progressive actions
            PROG_PrgC / NULLIF(PT_90s, 0) as progressive_carries_per_90,
            PROG_PrgP / NULLIF(PT_90s, 0) as progressive_passes_per_90,
            
            -- Overall contribution score (basic)
            (PERF_G_plus_A * 0.8 + EXP_npxG_plus_xAG * 0.2) / NULLIF(PT_90s, 0) as contribution_per_90
            
          FROM `{self.project_id}.nwsl_fbref.player_stats_all_years`
          WHERE season = {int(season)} 
            AND PT_Min >= {min_minutes}
            AND Pos NOT LIKE '%GK%' OR Pos LIKE '%GK%'  -- Include all positions
        ),
        
        replacement_levels AS (
          SELECT 
            position_group,
            COUNT(*) as total_players,
            
            -- Core Replacement Metrics
            PERCENTILE_CONT(goals_per_90, {self.replacement_percentiles['forward']}) OVER (PARTITION BY position_group) as replacement_goals_per_90,
            PERCENTILE_CONT(assists_per_90, {self.replacement_percentiles['midfielder']}) OVER (PARTITION BY position_group) as replacement_assists_per_90,
            PERCENTILE_CONT(xg_per_90, {self.replacement_percentiles['forward']}) OVER (PARTITION BY position_group) as replacement_xg_per_90,
            PERCENTILE_CONT(xag_per_90, {self.replacement_percentiles['midfielder']}) OVER (PARTITION BY position_group) as replacement_xag_per_90,
            PERCENTILE_CONT(contribution_per_90, 0.30) OVER (PARTITION BY position_group) as replacement_contribution_per_90,
            
            -- Progressive Play Baselines
            PERCENTILE_CONT(progressive_carries_per_90, 0.30) OVER (PARTITION BY position_group) as replacement_progressive_carries_per_90,
            PERCENTILE_CONT(progressive_passes_per_90, 0.30) OVER (PARTITION BY position_group) as replacement_progressive_passes_per_90,
            
            -- Basic Stats for Context
            AVG(goals_per_90) as avg_goals_per_90,
            AVG(assists_per_90) as avg_assists_per_90,
            AVG(xg_per_90) as avg_xg_per_90,
            AVG(contribution_per_90) as avg_contribution_per_90
            
          FROM position_stats
          WHERE position_group != 'other'
          GROUP BY position_group
        )
        
        SELECT DISTINCT
          position_group,
          total_players,
          replacement_goals_per_90,
          replacement_assists_per_90, 
          replacement_xg_per_90,
          replacement_xag_per_90,
          replacement_contribution_per_90,
          replacement_progressive_carries_per_90,
          replacement_progressive_passes_per_90,
          avg_goals_per_90,
          avg_assists_per_90,
          avg_xg_per_90,
          avg_contribution_per_90
        FROM replacement_levels
        ORDER BY position_group
        """
        
        df = self.client.query(query).to_dataframe()
        
        # Convert to dict for easier access
        baselines = {}
        for _, row in df.iterrows():
            position = row['position_group']
            baselines[position] = {
                'total_players': row['total_players'],
                'replacement_goals_per_90': row['replacement_goals_per_90'],
                'replacement_assists_per_90': row['replacement_assists_per_90'],
                'replacement_xg_per_90': row['replacement_xg_per_90'],
                'replacement_xag_per_90': row['replacement_xag_per_90'],
                'replacement_contribution_per_90': row['replacement_contribution_per_90'],
                'replacement_progressive_carries_per_90': row['replacement_progressive_carries_per_90'],
                'replacement_progressive_passes_per_90': row['replacement_progressive_passes_per_90'],
                'league_avg_goals_per_90': row['avg_goals_per_90'],
                'league_avg_assists_per_90': row['avg_assists_per_90'],
                'league_avg_xg_per_90': row['avg_xg_per_90'],
                'league_avg_contribution_per_90': row['avg_contribution_per_90']
            }
        
        return {
            'season': season,
            'analysis_timestamp': datetime.now().isoformat(),
            'replacement_baselines': baselines,
            'methodology': {
                'min_minutes': min_minutes,
                'replacement_percentiles': self.replacement_percentiles,
                'description': 'Replacement level calculated as bottom percentile of players with significant minutes'
            }
        }
    
    def calculate_player_war_estimates(self, season: str, min_minutes: int = 450) -> pd.DataFrame:
        """
        Calculate WAR (Wins Above Replacement) estimates for players
        
        Note: This is a simplified soccer WAR based on available offensive statistics
        """
        
        # First get replacement baselines
        baselines = self.calculate_replacement_baselines(season, min_minutes)['replacement_baselines']
        
        query = f"""
        WITH player_value AS (
          SELECT 
            Player as player_name,
            Squad as team,
            CASE 
              WHEN Pos LIKE '%FW%' THEN 'forward'
              WHEN Pos LIKE '%MF%' THEN 'midfielder'
              WHEN Pos LIKE '%DF%' THEN 'defender'
              WHEN Pos LIKE '%GK%' THEN 'goalkeeper'
              ELSE 'other'
            END as position_group,
            Pos as detailed_position,
            
            PT_Min as minutes_played,
            PT_90s as matches_90s,
            
            -- Core Performance Metrics
            P90_Gls as goals_per_90,
            P90_Ast as assists_per_90,
            P90_xG as xg_per_90,
            P90_xAG as xag_per_90,
            
            -- Total Contributions
            PERF_G_plus_A as total_goal_contributions,
            EXP_npxG_plus_xAG as total_expected_contributions,
            
            -- Progressive Actions
            PROG_PrgC / NULLIF(PT_90s, 0) as progressive_carries_per_90,
            PROG_PrgP / NULLIF(PT_90s, 0) as progressive_passes_per_90,
            PROG_PrgR / NULLIF(PT_90s, 0) as progressive_receptions_per_90,
            
            -- Discipline (penalty)
            (PERF_CrdY * 0.1 + PERF_CrdR * 0.5) / NULLIF(PT_90s, 0) as discipline_penalty_per_90,
            
            -- Overall contribution rate
            (PERF_G_plus_A * 0.7 + EXP_npxG_plus_xAG * 0.3) / NULLIF(PT_90s, 0) as weighted_contribution_per_90
            
          FROM `{self.project_id}.nwsl_fbref.player_stats_all_years`
          WHERE season = {int(season)} 
            AND PT_Min >= {min_minutes}
            AND Pos IS NOT NULL
        )
        
        SELECT *
        FROM player_value
        WHERE position_group != 'other'
        ORDER BY weighted_contribution_per_90 DESC
        """
        
        df = self.client.query(query).to_dataframe()
        
        # Calculate value above replacement for each player
        def calculate_var(row):
            position = row['position_group']
            if position not in baselines:
                return 0
            
            baseline = baselines[position]
            
            # Goals above replacement
            goals_ar = (row['goals_per_90'] - baseline['replacement_goals_per_90']) * row['matches_90s']
            
            # Assists above replacement  
            assists_ar = (row['assists_per_90'] - baseline['replacement_assists_per_90']) * row['matches_90s']
            
            # xG above replacement
            xg_ar = (row['xg_per_90'] - baseline['replacement_xg_per_90']) * row['matches_90s']
            
            # Progressive actions above replacement
            prog_carries_ar = max(0, (row['progressive_carries_per_90'] or 0) - baseline['replacement_progressive_carries_per_90']) * row['matches_90s'] * 0.01
            prog_passes_ar = max(0, (row['progressive_passes_per_90'] or 0) - baseline['replacement_progressive_passes_per_90']) * row['matches_90s'] * 0.01
            
            # Discipline penalty
            discipline_penalty = (row['discipline_penalty_per_90'] or 0) * row['matches_90s']
            
            # Weighted value above replacement (rough estimate)
            var = (goals_ar * 1.0 + assists_ar * 0.7 + xg_ar * 0.3 + 
                   prog_carries_ar + prog_passes_ar - discipline_penalty)
            
            return var
        
        df['value_above_replacement'] = df.apply(calculate_var, axis=1)
        
        # Convert to "wins" estimate (very rough - assuming 10 goal contributions = ~1 win)
        df['estimated_wins_above_replacement'] = df['value_above_replacement'] / 10
        
        # Add percentile rankings
        df['var_percentile'] = df['value_above_replacement'].rank(pct=True)
        df['war_percentile'] = df['estimated_wins_above_replacement'].rank(pct=True)
        
        # Add value categories
        df['value_tier'] = pd.cut(df['var_percentile'], 
                                 bins=[0, 0.25, 0.5, 0.75, 0.9, 1.0],
                                 labels=['Below Replacement', 'Replacement Level', 'Average', 'Above Average', 'Elite'])
        
        return df.sort_values('value_above_replacement', ascending=False)
    
    def analyze_team_roster_construction(self, season: str) -> pd.DataFrame:
        """
        Analyze team roster construction using replacement value concepts
        
        Research Focus: How many "plus-minus" does a championship roster need?
        """
        
        war_data = self.calculate_player_war_estimates(season)
        
        team_analysis = war_data.groupby('team').agg({
            'value_above_replacement': ['sum', 'mean', 'std', 'count'],
            'estimated_wins_above_replacement': ['sum', 'mean'],
            'weighted_contribution_per_90': ['mean', 'max'],
            'minutes_played': 'sum',
            'matches_90s': 'sum'
        }).round(3)
        
        # Flatten column names
        team_analysis.columns = ['_'.join(col).strip() for col in team_analysis.columns.values]
        team_analysis = team_analysis.reset_index()
        
        # Add analysis columns
        team_analysis['total_war'] = team_analysis['estimated_wins_above_replacement_sum']
        team_analysis['avg_war'] = team_analysis['estimated_wins_above_replacement_mean'] 
        team_analysis['squad_depth'] = team_analysis['value_above_replacement_count']
        team_analysis['star_power'] = team_analysis['weighted_contribution_per_90_max']
        team_analysis['squad_consistency'] = team_analysis['value_above_replacement_mean'] / (team_analysis['value_above_replacement_std'] + 0.1)
        
        # Roster construction style
        def classify_roster_style(row):
            if row['star_power'] >= 1.5 and row['total_war'] >= 5:
                return 'Star-Driven Championship'
            elif row['squad_consistency'] >= 2.0 and row['total_war'] >= 3:
                return 'Balanced Excellence'
            elif row['total_war'] >= 4:
                return 'High Value Roster'
            elif row['total_war'] >= 1:
                return 'Above Average Roster'
            else:
                return 'Below Average Roster'
        
        team_analysis['roster_style'] = team_analysis.apply(classify_roster_style, axis=1)
        
        # Add rankings
        team_analysis['war_rank'] = team_analysis['total_war'].rank(ascending=False, method='dense')
        team_analysis['depth_rank'] = team_analysis['squad_consistency'].rank(ascending=False, method='dense')
        team_analysis['star_rank'] = team_analysis['star_power'].rank(ascending=False, method='dense')
        
        return team_analysis.sort_values('total_war', ascending=False)
    
    def find_undervalued_players(self, season: str, min_war: float = 0.5) -> pd.DataFrame:
        """
        Find players providing high value above replacement
        
        Research Focus: Which players provide championship-level value?
        """
        
        war_data = self.calculate_player_war_estimates(season)
        
        # Focus on players with positive WAR
        undervalued = war_data[war_data['estimated_wins_above_replacement'] >= min_war].copy()
        
        # Add context ratios
        undervalued['goals_above_replacement'] = undervalued.apply(
            lambda row: (row['goals_per_90'] - 0.1) * row['matches_90s'], axis=1  # Rough replacement = 0.1 goals/90
        )
        
        undervalued['assists_above_replacement'] = undervalued.apply(
            lambda row: (row['assists_per_90'] - 0.05) * row['matches_90s'], axis=1  # Rough replacement = 0.05 assists/90
        )
        
        # Value efficiency (WAR per 90 minutes)
        undervalued['war_per_90'] = undervalued['estimated_wins_above_replacement'] / undervalued['matches_90s']
        
        return undervalued[['player_name', 'team', 'position_group', 'detailed_position',
                          'minutes_played', 'matches_90s', 'value_above_replacement',
                          'estimated_wins_above_replacement', 'war_per_90', 'value_tier',
                          'goals_per_90', 'assists_per_90', 'weighted_contribution_per_90',
                          'goals_above_replacement', 'assists_above_replacement']].sort_values('estimated_wins_above_replacement', ascending=False)

def main():
    """Test the ReplacementValueEstimator"""
    print("⭐ Testing NWSL Replacement Value Estimator")
    print("=" * 50)
    
    estimator = ReplacementValueEstimator()
    
    # Test 1: Calculate replacement baselines
    print("📊 Calculating replacement level baselines for 2024...")
    baselines = estimator.calculate_replacement_baselines("2024")
    
    for position, stats in baselines['replacement_baselines'].items():
        print(f"  {position.title()}: {stats['replacement_contribution_per_90']:.3f} contributions/90 "
              f"(from {stats['total_players']} players)")
    
    # Test 2: Top WAR players
    print(f"\n🏆 Top WAR performers in 2024:")
    war_leaders = estimator.calculate_player_war_estimates("2024").head(10)
    
    for _, player in war_leaders.iterrows():
        print(f"  {player['player_name']} ({player['team']}): {player['estimated_wins_above_replacement']:.2f} WAR, "
              f"{player['value_tier']}")
    
    # Test 3: Team roster analysis
    print(f"\n🏆 Team roster construction analysis:")
    team_analysis = estimator.analyze_team_roster_construction("2024").head(5)
    
    for _, team in team_analysis.iterrows():
        print(f"  {team['team']}: {team['total_war']:.1f} total WAR, "
              f"{team['roster_style']}")
    
    print(f"\n✅ ReplacementValueEstimator test complete!")

if __name__ == "__main__":
    main()