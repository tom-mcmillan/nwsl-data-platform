#!/usr/bin/env python3
"""
Create comprehensive team-level analytics from player statistics data
Generates team performance metrics, tactical analysis, and aggregated insights
"""

import pandas as pd
from google.cloud import bigquery
from datetime import datetime

def create_team_season_analytics():
    """Create comprehensive team season analytics from player data"""
    print("🏈 Creating NWSL Team Season Analytics")
    print("=" * 50)
    
    client = bigquery.Client(project="nwsl-data")
    
    # Get all available seasons
    seasons_query = """
    SELECT DISTINCT season 
    FROM `nwsl-data.nwsl_fbref.player_stats_2021`
    UNION DISTINCT
    SELECT DISTINCT season 
    FROM `nwsl-data.nwsl_fbref.player_stats_2022`
    UNION DISTINCT
    SELECT DISTINCT season 
    FROM `nwsl-data.nwsl_fbref.player_stats_2023`
    UNION DISTINCT
    SELECT DISTINCT season 
    FROM `nwsl-data.nwsl_fbref.player_stats_2024`
    UNION DISTINCT
    SELECT DISTINCT season 
    FROM `nwsl-data.nwsl_fbref.player_stats_2025`
    ORDER BY season
    """
    
    seasons_df = client.query(seasons_query).to_dataframe()
    seasons = seasons_df['season'].tolist()
    
    print(f"📅 Processing seasons: {seasons}")
    
    all_team_data = []
    
    for season in seasons:
        print(f"\n📊 Processing season {season}...")
        
        # Comprehensive team analytics query
        team_query = f"""
        SELECT 
          '{season}' as season,
          Squad as team_name,
          COUNT(*) as squad_size,
          
          -- Goal Statistics
          SUM(PERF_Gls) as total_goals,
          SUM(PERF_Ast) as total_assists,
          SUM(PERF_G_plus_A) as total_goal_contributions,
          SUM(PERF_PK) as total_penalties,
          SUM(PERF_PKatt) as total_penalty_attempts,
          
          -- Expected Statistics  
          SUM(EXP_xG) as total_xg,
          SUM(EXP_npxG) as total_non_pen_xg,
          SUM(EXP_xAG) as total_xag,
          SUM(EXP_npxG_plus_xAG) as total_npxg_plus_xag,
          
          -- Performance vs Expected
          SUM(PERF_Gls) - SUM(EXP_xG) as goals_vs_xg_diff,
          SUM(PERF_Ast) - SUM(EXP_xAG) as assists_vs_xag_diff,
          
          -- Playing Time
          SUM(PT_Min) as total_minutes,
          SUM(PT_Starts) as total_starts,
          SUM(PT_90s) as total_90s,
          AVG(PT_Min) as avg_minutes_per_player,
          
          -- Progressive Play
          SUM(PROG_PrgC) as total_progressive_carries,
          SUM(PROG_PrgP) as total_progressive_passes,
          SUM(PROG_PrgR) as total_progressive_receptions,
          
          -- Discipline
          SUM(PERF_CrdY) as total_yellow_cards,
          SUM(PERF_CrdR) as total_red_cards,
          
          -- Per 90 Averages (weighted by minutes)
          SUM(P90_Gls * PT_90s) / NULLIF(SUM(PT_90s), 0) as team_goals_per_90,
          SUM(P90_Ast * PT_90s) / NULLIF(SUM(PT_90s), 0) as team_assists_per_90,
          SUM(P90_xG * PT_90s) / NULLIF(SUM(PT_90s), 0) as team_xg_per_90,
          SUM(P90_xAG * PT_90s) / NULLIF(SUM(PT_90s), 0) as team_xag_per_90,
          
          -- Team Depth Analysis
          COUNT(CASE WHEN PT_Min >= 900 THEN 1 END) as regular_players,
          COUNT(CASE WHEN PT_Min >= 1800 THEN 1 END) as key_players,
          COUNT(CASE WHEN PERF_Gls >= 5 THEN 1 END) as goal_scorers,
          COUNT(CASE WHEN PERF_Ast >= 3 THEN 1 END) as assist_providers,
          
          -- Position Analysis
          COUNT(CASE WHEN Pos LIKE '%GK%' THEN 1 END) as goalkeepers,
          COUNT(CASE WHEN Pos LIKE '%DF%' THEN 1 END) as defenders,
          COUNT(CASE WHEN Pos LIKE '%MF%' THEN 1 END) as midfielders,
          COUNT(CASE WHEN Pos LIKE '%FW%' THEN 1 END) as forwards,
          
          -- Age Analysis
          AVG(CAST(SUBSTR(Age, 1, 2) AS INT64)) as avg_age,
          MIN(CAST(SUBSTR(Age, 1, 2) AS INT64)) as youngest_player,
          MAX(CAST(SUBSTR(Age, 1, 2) AS INT64)) as oldest_player,
          
          -- International Players
          COUNT(CASE WHEN Nation != 'us USA' THEN 1 END) as international_players,
          COUNT(DISTINCT Nation) as nationalities_count,
          
          -- Efficiency Metrics
          SUM(PERF_Gls) / NULLIF(SUM(EXP_xG), 0) as goal_conversion_rate,
          SUM(PERF_Ast) / NULLIF(SUM(EXP_xAG), 0) as assist_conversion_rate,
          
          -- Data quality
          '{datetime.now().isoformat()}' as created_at
          
        FROM `nwsl-data.nwsl_fbref.player_stats_{season}`
        WHERE Squad IS NOT NULL
        GROUP BY Squad
        ORDER BY total_goals DESC
        """
        
        season_df = client.query(team_query).to_dataframe()
        all_team_data.append(season_df)
        
        print(f"✅ Processed {len(season_df)} teams for {season}")
    
    # Combine all seasons
    combined_df = pd.concat(all_team_data, ignore_index=True)
    
    print(f"\n📊 Total team records: {len(combined_df)}")
    
    # Save to CSV
    csv_path = "data/processed/team_season_analytics.csv"
    combined_df.to_csv(csv_path, index=False)
    print(f"💾 Saved to: {csv_path}")
    
    # Upload to BigQuery
    table_id = "nwsl_fbref.team_season_analytics"
    
    print(f"📤 Uploading to BigQuery: {table_id}")
    
    try:
        combined_df.to_gbq(
            destination_table=table_id,
            project_id="nwsl-data",
            if_exists='replace',
            chunksize=1000,
            progress_bar=False
        )
        
        print(f"✅ Successfully uploaded {len(combined_df)} team season records")
        return True
        
    except Exception as e:
        print(f"❌ Upload failed: {e}")
        return False

def create_team_comparison_views():
    """Create BigQuery views for team comparisons and rankings"""
    print("\n🏆 Creating Team Comparison Views")
    print("=" * 40)
    
    client = bigquery.Client(project="nwsl-data")
    
    # Team Performance Rankings View
    rankings_view = """
    CREATE OR REPLACE VIEW `nwsl-data.nwsl_fbref.team_performance_rankings` AS
    WITH team_rankings AS (
      SELECT 
        season,
        team_name,
        total_goals,
        total_xg,
        goals_vs_xg_diff,
        team_goals_per_90,
        team_xg_per_90,
        goal_conversion_rate,
        
        -- Rankings
        RANK() OVER (PARTITION BY season ORDER BY total_goals DESC) as goals_rank,
        RANK() OVER (PARTITION BY season ORDER BY total_xg DESC) as xg_rank,
        RANK() OVER (PARTITION BY season ORDER BY goals_vs_xg_diff DESC) as efficiency_rank,
        RANK() OVER (PARTITION BY season ORDER BY team_goals_per_90 DESC) as goals_per_90_rank,
        RANK() OVER (PARTITION BY season ORDER BY goal_conversion_rate DESC) as conversion_rank
        
      FROM `nwsl-data.nwsl_fbref.team_season_analytics`
    )
    SELECT * FROM team_rankings
    ORDER BY season DESC, goals_rank ASC
    """
    
    try:
        client.query(rankings_view).result()
        print("✅ Created team_performance_rankings view")
    except Exception as e:
        print(f"❌ Failed to create rankings view: {e}")
    
    # Multi-year team evolution view  
    evolution_view = """
    CREATE OR REPLACE VIEW `nwsl-data.nwsl_fbref.team_evolution` AS
    SELECT 
      team_name,
      season,
      total_goals,
      total_xg,
      team_goals_per_90,
      squad_size,
      avg_age,
      international_players,
      
      -- Year-over-year changes
      LAG(total_goals) OVER (PARTITION BY team_name ORDER BY season) as prev_year_goals,
      total_goals - LAG(total_goals) OVER (PARTITION BY team_name ORDER BY season) as goals_change,
      total_xg - LAG(total_xg) OVER (PARTITION BY team_name ORDER BY season) as xg_change,
      squad_size - LAG(squad_size) OVER (PARTITION BY team_name ORDER BY season) as squad_size_change,
      avg_age - LAG(avg_age) OVER (PARTITION BY team_name ORDER BY season) as age_change
      
    FROM `nwsl-data.nwsl_fbref.team_season_analytics`
    WHERE team_name IN (
      SELECT team_name 
      FROM `nwsl-data.nwsl_fbref.team_season_analytics`
      GROUP BY team_name 
      HAVING COUNT(DISTINCT season) >= 2
    )
    ORDER BY team_name, season
    """
    
    try:
        client.query(evolution_view).result()
        print("✅ Created team_evolution view")
    except Exception as e:
        print(f"❌ Failed to create evolution view: {e}")

def main():
    """Main execution function"""
    success = create_team_season_analytics()
    
    if success:
        create_team_comparison_views()
        
        print("\n🎉 Team Analytics Creation Complete!")
        print("📊 New BigQuery resources available:")
        print("   - nwsl_fbref.team_season_analytics (table)")
        print("   - nwsl_fbref.team_performance_rankings (view)")
        print("   - nwsl_fbref.team_evolution (view)")
        print("\nThese provide comprehensive team-level insights from player data!")
    else:
        print("\n❌ Team analytics creation failed")

if __name__ == "__main__":
    main()