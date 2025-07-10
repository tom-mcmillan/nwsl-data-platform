#!/usr/bin/env python3
"""
Basic NWSL Data Ingestion - What Actually Works
Focus on the data we confirmed is available: players, teams, and games
"""

import sys
import os
import logging
import time
from pathlib import Path
from typing import Optional, Dict, Any, List
import pandas as pd
import json

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Import itscalledsoccer
try:
    from itscalledsoccer.client import AmericanSoccerAnalysis
except ImportError:
    print("❌ itscalledsoccer not installed. Run: pip install itscalledsoccer")
    sys.exit(1)

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def ingest_available_nwsl_data():
    """Ingest the NWSL data that we confirmed works"""
    logger.info("⚽ Ingesting confirmed working NWSL data...")
    
    asa = AmericanSoccerAnalysis()
    results = {}
    
    # 1. Players roster (1,016 records confirmed)
    logger.info("\\n👥 Getting NWSL players roster...")
    try:
        players = asa.get_players(leagues=['nwsl'])
        if players is not None and len(players) > 0:
            logger.info(f"✅ Got {len(players)} players")
            
            # Clean column names
            players_clean = clean_dataframe(players)
            
            # Save to CSV for inspection
            players_clean.to_csv('nwsl_players_complete.csv', index=False)
            logger.info("💾 Saved to nwsl_players_complete.csv")
            
            # Show column info
            logger.info(f"📊 Player columns: {list(players_clean.columns)}")
            
            # Show sample data
            sample_player = players_clean.iloc[0]
            logger.info(f"👤 Sample: {sample_player.get('player_name', 'Unknown')} - {sample_player.get('primary_general_position', 'Unknown')} ({sample_player.get('nationality', 'Unknown')})")
            
            results['players'] = {
                'records': len(players_clean),
                'columns': list(players_clean.columns),
                'data': players_clean
            }
            
    except Exception as e:
        logger.error(f"❌ Players failed: {e}")
    
    # 2. Teams info (17 records confirmed)
    logger.info("\\n🏆 Getting NWSL teams...")
    try:
        teams = asa.get_teams(leagues=['nwsl'])
        if teams is not None and len(teams) > 0:
            logger.info(f"✅ Got {len(teams)} teams")
            
            teams_clean = clean_dataframe(teams)
            teams_clean.to_csv('nwsl_teams_complete.csv', index=False)
            logger.info("💾 Saved to nwsl_teams_complete.csv")
            
            logger.info(f"📊 Team columns: {list(teams_clean.columns)}")
            logger.info(f"🏆 Teams: {', '.join(teams_clean['team_name'].tolist())}")
            
            results['teams'] = {
                'records': len(teams_clean),
                'columns': list(teams_clean.columns),
                'data': teams_clean
            }
            
    except Exception as e:
        logger.error(f"❌ Teams failed: {e}")
    
    # 3. Games for multiple seasons
    seasons = ['2024', '2023', '2022', '2021']
    for season in seasons:
        logger.info(f"\\n📅 Getting NWSL games for {season}...")
        try:
            games = asa.get_games(leagues=['nwsl'], seasons=[season])
            if games is not None and len(games) > 0:
                logger.info(f"✅ Got {len(games)} games for {season}")
                
                games_clean = clean_dataframe(games)
                games_clean['season'] = season
                
                filename = f'nwsl_games_{season}.csv'
                games_clean.to_csv(filename, index=False)
                logger.info(f"💾 Saved to {filename}")
                
                if season == '2024':  # Show details for latest season
                    logger.info(f"📊 Game columns: {list(games_clean.columns)}")
                    recent_game = games_clean.iloc[0]
                    logger.info(f"⚽ Sample game: {recent_game.get('home_team_id', 'Unknown')} vs {recent_game.get('away_team_id', 'Unknown')} ({recent_game.get('date_time_utc', 'Unknown date')})")
                
                results[f'games_{season}'] = {
                    'records': len(games_clean),
                    'columns': list(games_clean.columns),
                    'data': games_clean
                }
                
        except Exception as e:
            logger.error(f"❌ Games {season} failed: {e}")
        
        time.sleep(0.5)  # Be respectful to API
    
    # 4. Try other data types to see what might work
    logger.info("\\n🔍 Testing other data types...")
    
    other_tests = [
        ('player_salaries', lambda: asa.get_player_salaries(leagues=['nwsl'])),
        ('team_salaries', lambda: asa.get_team_salaries(leagues=['nwsl'])),
        ('managers', lambda: asa.get_managers(leagues=['nwsl'])),
        ('referees', lambda: asa.get_referees(leagues=['nwsl'])),
        ('stadia', lambda: asa.get_stadia(leagues=['nwsl']))
    ]
    
    for test_name, test_func in other_tests:
        try:
            logger.info(f"   🔍 Testing {test_name}...")
            data = test_func()
            if data is not None and len(data) > 0:
                logger.info(f"   ✅ {test_name}: {len(data)} records")
                
                data_clean = clean_dataframe(data)
                filename = f'nwsl_{test_name}.csv'
                data_clean.to_csv(filename, index=False)
                
                results[test_name] = {
                    'records': len(data_clean),
                    'columns': list(data_clean.columns),
                    'data': data_clean
                }
            else:
                logger.info(f"   ❌ {test_name}: No data")
                
        except Exception as e:
            logger.info(f"   💥 {test_name}: {e}")
    
    return results

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Clean DataFrame column names"""
    df = df.copy()
    
    # Clean column names for BigQuery
    df.columns = [
        col.replace(' ', '_')
           .replace('-', '_')
           .replace('.', '_')
           .replace('/', '_')
           .replace('%', '_pct')
           .replace('(', '')
           .replace(')', '')
           .replace('+', '_plus')
           .replace('#', 'num')
           .lower()
        for col in df.columns
    ]
    
    # Add ingestion metadata
    df['ingestion_date'] = pd.Timestamp.now()
    df['data_source'] = 'ASA_itscalledsoccer'
    
    return df

def create_field_mapping():
    """Create a mapping of available fields to desired player stats"""
    logger.info("\\n🎯 Creating field mapping for player statistics...")
    
    # Load the player data we just saved
    try:
        players_df = pd.read_csv('nwsl_players_complete.csv')
        
        available_fields = list(players_df.columns)
        logger.info(f"📊 Available player fields: {available_fields}")
        
        # Your desired fields from the original request
        desired_fields = [
            'team', 'player_name', 'position', 'games_played', 'games_started',
            'minutes_played', 'goals', 'accurate_pass_percentage', 'assists',
            'total_scoring_attempts', 'on_target_scoring_attempts', 'total_attacking_assists',
            'tackles', 'fouls_committed', 'fouls_suffered', 'total_offside',
            'yellow_cards', 'red_cards', 'accurate_passes', 'total_passes',
            'crosses', 'assists_avg_over_90_mins', 'long_balls', 'successful_short_passes',
            'turnovers', 'goals_avg_over_90_mins', 'penalty_kick_goals',
            'penalty_kick_taken', 'penalty_kick_percentage', 'accurate_shooting_percentage',
            'successful_dribble', 'dribble_percentage', 'goals_and_assists',
            'tackles_percentage', 'interceptions', 'headed_duel',
            'gk_saves', 'gk_long_ball_percentage', 'gk_total_clearance'
        ]
        
        # Map available to desired
        field_mapping = {}
        
        for desired in desired_fields:
            matches = []
            for available in available_fields:
                if any(part in available.lower() for part in desired.lower().split('_')):
                    matches.append(available)
            
            if matches:
                field_mapping[desired] = matches[0]  # Take first match
                logger.info(f"✅ {desired} → {matches[0]}")
            else:
                logger.info(f"❌ {desired} → NOT FOUND")
        
        logger.info(f"\\n📊 MAPPING SUMMARY: {len(field_mapping)}/{len(desired_fields)} fields available")
        
        # Save mapping to JSON
        with open('nwsl_field_mapping.json', 'w') as f:
            json.dump(field_mapping, f, indent=2)
        logger.info("💾 Saved field mapping to nwsl_field_mapping.json")
        
        return field_mapping
        
    except Exception as e:
        logger.error(f"❌ Field mapping failed: {e}")
        return {}

def main():
    """Main function"""
    logger.info("🚀 Starting basic NWSL data ingestion...")
    
    # Ingest available data
    results = ingest_available_nwsl_data()
    
    # Create field mapping
    field_mapping = create_field_mapping()
    
    # Summary
    logger.info(f"""
    
{'='*60}
📊 NWSL DATA INGESTION SUMMARY
{'='*60}

✅ Successfully retrieved:
""")
    
    total_records = 0
    for data_type, info in results.items():
        if 'records' in info:
            records = info['records']
            total_records += records
            logger.info(f"   - {data_type}: {records:,} records")
    
    logger.info(f"""
📈 Total records: {total_records:,}
💾 Data saved to CSV files for inspection
🎯 Field mapping created

🔧 WHAT WE HAVE:
   ✅ Complete player roster (names, positions, basic info)
   ✅ Team information (all 17 NWSL teams)
   ✅ Match data (games, scores, attendance)
   ✅ Historical data across multiple seasons

❌ WHAT'S MISSING:
   ❌ Detailed player performance stats (goals, assists, minutes)
   ❌ Individual match-by-match player statistics
   ❌ Advanced metrics (xG, passing accuracy, tackles)

💡 NEXT STEPS:
   1. Use this data as foundation in BigQuery
   2. Consider supplementing with web scraping for missing stats
   3. Explore other data sources for detailed player performance
   4. Build analytics on available team/game data

🎯 RECOMMENDATION:
   The available data provides excellent foundation for:
   - Team analysis and comparisons
   - Season trends and historical analysis
   - Player roster management and demographics
   - Match result analysis and predictions
    """)

if __name__ == "__main__":
    main()