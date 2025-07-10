#!/usr/bin/env python3
"""
Test itscalledsoccer library for NWSL player data
This is the proper way to access American Soccer Analysis data
"""

import sys
import logging
import pandas as pd
from pathlib import Path

# Import itscalledsoccer
try:
    from itscalledsoccer.client import AmericanSoccerAnalysis
except ImportError:
    print("❌ itscalledsoccer not installed. Run: pip install itscalledsoccer")
    sys.exit(1)

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_asa_player_data():
    """Test American Soccer Analysis API for NWSL player data"""
    logger.info("🔍 Testing itscalledsoccer library for NWSL data...")
    
    # Create ASA client
    asa = AmericanSoccerAnalysis()
    
    # Test different data types
    data_tests = [
        ('players', 'get_players'),
        ('teams', 'get_teams'), 
        ('games', 'get_games'),
        ('player_stats', 'get_player_stats'),
        ('team_stats', 'get_team_stats'),
        ('player_salaries', 'get_player_salaries'),
        ('player_goals_added', 'get_player_goals_added'),
        ('goalkeeper_stats', 'get_goalkeeper_stats'),
        ('team_salaries', 'get_team_salaries')
    ]
    
    results = {}
    
    for data_type, method_name in data_tests:
        logger.info(f"\n📊 Testing {data_type}...")
        
        try:
            method = getattr(asa, method_name)
            
            # Try with NWSL league and recent seasons
            logger.info(f"   🔍 Calling {method_name} for NWSL...")
            
            if data_type in ['games', 'team_stats', 'player_stats', 'player_goals_added', 'goalkeeper_stats']:
                # These methods support season filtering
                df = method(leagues=['nwsl'], seasons=[2024, 2023])
            else:
                # These methods get all data
                df = method(leagues=['nwsl'])
            
            if df is not None and len(df) > 0:
                logger.info(f"   ✅ {data_type}: {len(df)} records")
                results[data_type] = {
                    'records': len(df),
                    'columns': list(df.columns),
                    'sample_data': df.head(3)
                }
                
                # Show key columns
                logger.info(f"   📊 Columns ({len(df.columns)}): {list(df.columns)[:8]}...")
                
                # Check for desired player stats fields
                if 'player' in data_type:
                    desired_fields = [
                        'player_name', 'team', 'position', 'games_played', 'games_started',
                        'minutes_played', 'goals', 'assists', 'accurate_pass_percentage',
                        'total_scoring_attempts', 'on_target_scoring_attempts', 'tackles',
                        'yellow_cards', 'red_cards', 'fouls_committed', 'fouls_suffered'
                    ]
                    
                    found_fields = []
                    for field in desired_fields:
                        # Check exact match or similar fields
                        matching_cols = [col for col in df.columns if field.lower() in col.lower() or any(part in col.lower() for part in field.lower().split('_'))]
                        if matching_cols:
                            found_fields.extend(matching_cols)
                    
                    if found_fields:
                        logger.info(f"   🎯 Relevant fields: {found_fields[:8]}...")
                
                # Save sample data
                filename = f"asa_{data_type}_sample.csv"
                df.head(10).to_csv(filename, index=False)
                logger.info(f"   💾 Saved sample to {filename}")
                
                # Show sample row
                if not df.empty:
                    sample = df.iloc[0]
                    if 'player_name' in df.columns:
                        player_name = sample.get('player_name', 'Unknown')
                        team = sample.get('team_name', sample.get('team', 'Unknown'))
                        logger.info(f"   👤 Sample: {player_name} ({team})")
                    elif 'team_name' in df.columns:
                        team_name = sample.get('team_name', 'Unknown')
                        logger.info(f"   🏆 Sample: {team_name}")
                    
            else:
                logger.info(f"   ❌ {data_type}: No data returned")
                results[data_type] = {'records': 0}
                
        except Exception as e:
            logger.info(f"   💥 {data_type} failed: {str(e)[:100]}...")
            results[data_type] = {'error': str(e)}
    
    # Summary
    logger.info(f"\n{'='*60}")
    logger.info("📊 SUMMARY - NWSL Data Available via itscalledsoccer")
    logger.info(f"{'='*60}")
    
    successful_types = []
    for data_type, result in results.items():
        if isinstance(result, dict) and result.get('records', 0) > 0:
            successful_types.append(data_type)
            logger.info(f"✅ {data_type}: {result['records']} records")
        else:
            logger.info(f"❌ {data_type}: Not available")
    
    if successful_types:
        logger.info(f"\n🎉 SUCCESS! Available data types: {successful_types}")
        logger.info("✅ You can proceed with itscalledsoccer for NWSL player data!")
        
        # Show what player fields are available
        player_data_types = [dt for dt in successful_types if 'player' in dt]
        if player_data_types:
            logger.info(f"\n👥 Player data available: {player_data_types}")
            for dt in player_data_types:
                if dt in results and 'columns' in results[dt]:
                    logger.info(f"   📊 {dt} columns: {results[dt]['columns'][:10]}...")
    else:
        logger.info("\n❌ No NWSL data found via itscalledsoccer")
    
    return results

def show_detailed_player_stats():
    """Show detailed breakdown of available player statistics"""
    logger.info(f"\n{'='*60}")
    logger.info("🔍 DETAILED PLAYER STATISTICS ANALYSIS")
    logger.info(f"{'='*60}")
    
    asa = AmericanSoccerAnalysis()
    
    try:
        # Get player stats for detailed analysis
        logger.info("📊 Getting detailed player stats...")
        player_stats = asa.get_player_stats(leagues=['nwsl'], seasons=[2024])
        
        if player_stats is not None and len(player_stats) > 0:
            logger.info(f"✅ Found {len(player_stats)} player stat records")
            
            # Show all columns
            logger.info(f"\n📋 ALL AVAILABLE COLUMNS ({len(player_stats.columns)}):")
            for i, col in enumerate(player_stats.columns, 1):
                logger.info(f"   {i:2d}. {col}")
            
            # Map to desired fields
            logger.info(f"\n🎯 MAPPING TO DESIRED FIELDS:")
            desired_mapping = {
                'team': ['team_name', 'team'],
                'player_name': ['player_name', 'name'],
                'position': ['position', 'primary_position'],
                'games_played': ['games_played', 'appearances'],
                'games_started': ['games_started', 'starts'],
                'minutes_played': ['minutes_played', 'minutes'],
                'goals': ['goals', 'total_goals'],
                'assists': ['assists', 'total_assists'],
                'accurate_pass_percentage': ['pass_completion', 'passing_accuracy'],
                'total_scoring_attempts': ['shots', 'total_shots'],
                'on_target_scoring_attempts': ['shots_on_target', 'on_target'],
                'tackles': ['tackles', 'defensive_actions'],
                'yellow_cards': ['yellow_cards', 'yellows'],
                'red_cards': ['red_cards', 'reds'],
                'fouls_committed': ['fouls_committed', 'fouls'],
                'fouls_suffered': ['fouls_suffered', 'fouls_drawn']
            }
            
            available_fields = {}
            for desired_field, possible_names in desired_mapping.items():
                found = [col for col in player_stats.columns if any(name.lower() in col.lower() for name in possible_names)]
                if found:
                    available_fields[desired_field] = found[0]  # Take first match
                    logger.info(f"   ✅ {desired_field}: {found[0]}")
                else:
                    logger.info(f"   ❌ {desired_field}: Not found")
            
            logger.info(f"\n📊 AVAILABLE: {len(available_fields)}/{len(desired_mapping)} desired fields")
            
            # Show sample data with available fields
            if available_fields:
                logger.info(f"\n👤 SAMPLE PLAYER DATA:")
                sample_cols = list(available_fields.values())[:8]  # First 8 available fields
                sample_data = player_stats[sample_cols].head(3)
                logger.info(f"\n{sample_data.to_string()}")
        
    except Exception as e:
        logger.error(f"❌ Error getting detailed player stats: {e}")

if __name__ == "__main__":
    # Test all data types
    results = test_asa_player_data()
    
    # Show detailed player stats if available
    if any('player' in dt for dt in results.keys() if results[dt].get('records', 0) > 0):
        show_detailed_player_stats()
    
    # Final recommendation
    player_types = [dt for dt, result in results.items() if 'player' in dt and result.get('records', 0) > 0]
    
    if player_types:
        print(f"\n🎯 RECOMMENDATION: Use itscalledsoccer library!")
        print(f"   Available player data: {player_types}")
        print(f"   This library provides direct access to NWSL player statistics.")
    else:
        print(f"\n⚠️  RECOMMENDATION: Player data not found in itscalledsoccer")
        print(f"   Available data: {[dt for dt, result in results.items() if result.get('records', 0) > 0]}")