#!/usr/bin/env python3
"""
Test NWSL Data Availability with soccerdata
Test script to check if soccerdata can access NWSL player statistics
"""

import sys
import logging
from pathlib import Path
import pandas as pd

# Import soccerdata
try:
    import soccerdata as sd
except ImportError:
    print("❌ soccerdata not installed. Run: pip install soccerdata")
    sys.exit(1)

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_nwsl_data_sources():
    """Test different data sources for NWSL availability"""
    
    # Possible NWSL league identifiers
    nwsl_codes = [
        'USA-NWSL',
        'NWSL', 
        'USA-National Women\'s Soccer League',
        'United States-NWSL',
        'US-NWSL'
    ]
    
    # Data sources to test
    sources = {
        'ESPN': sd.ESPN,
        'FBref': sd.FBref, 
        'FotMob': sd.FotMob
    }
    
    season = '2024'
    results = {}
    
    logger.info("🔍 Testing NWSL data availability...")
    logger.info(f"📅 Testing season: {season}")
    
    for source_name, source_class in sources.items():
        logger.info(f"\n📊 Testing {source_name}...")
        results[source_name] = {}
        
        for league_code in nwsl_codes:
            logger.info(f"   🔍 Trying league code: '{league_code}'")
            
            try:
                # Create scraper instance
                scraper = source_class(league_code, season)
                
                # Test 1: Read schedule
                try:
                    schedule = scraper.read_schedule()
                    matches_count = len(schedule) if not schedule.empty else 0
                    logger.info(f"      📅 Schedule: {matches_count} matches")
                    
                    if matches_count > 0:
                        results[source_name][league_code] = {'schedule': matches_count}
                        
                        # Test 2: Team stats
                        try:
                            team_stats = scraper.read_team_season_stats()
                            team_count = len(team_stats) if not team_stats.empty else 0
                            logger.info(f"      🏆 Team stats: {team_count} teams")
                            results[source_name][league_code]['teams'] = team_count
                            
                        except Exception as e:
                            logger.info(f"      ❌ Team stats failed: {str(e)[:50]}...")
                        
                        # Test 3: Player stats (general)
                        try:
                            player_stats = scraper.read_player_season_stats()
                            player_count = len(player_stats) if not player_stats.empty else 0
                            logger.info(f"      👥 Player stats: {player_count} players")
                            results[source_name][league_code]['players'] = player_count
                            
                            if player_count > 0:
                                # Show sample player data
                                sample = player_stats.iloc[0]
                                player_name = sample.get('player', sample.get('name', 'Unknown'))
                                team = sample.get('team', 'Unknown')
                                logger.info(f"         Sample: {player_name} ({team})")
                                logger.info(f"         Columns: {list(player_stats.columns)[:8]}...")
                                
                                # Save sample to file
                                filename = f"sample_{source_name.lower()}_{league_code.replace('-', '_')}_players.csv"
                                player_stats.head(5).to_csv(filename, index=False)
                                logger.info(f"         💾 Saved sample to {filename}")
                                
                        except Exception as e:
                            logger.info(f"      ❌ Player stats failed: {str(e)[:50]}...")
                        
                        # Test 4: Different stat types
                        stat_types = ['standard', 'shooting', 'passing', 'defense']
                        for stat_type in stat_types:
                            try:
                                stats = scraper.read_player_season_stats(stat_type=stat_type)
                                count = len(stats) if not stats.empty else 0
                                if count > 0:
                                    logger.info(f"      🎯 {stat_type} stats: {count} records")
                                    results[source_name][league_code][f'{stat_type}_stats'] = count
                            except Exception as e:
                                logger.info(f"      ⚠️ {stat_type} failed: {str(e)[:30]}...")
                        
                        # If we found working data, break to next source
                        break
                        
                except Exception as e:
                    logger.info(f"      ❌ Schedule failed: {str(e)[:50]}...")
                    
            except Exception as e:
                logger.info(f"   💥 {source_name}({league_code}) initialization failed: {str(e)[:50]}...")
    
    # Summary
    logger.info(f"\n{'='*60}")
    logger.info("📊 SUMMARY")
    logger.info(f"{'='*60}")
    
    working_sources = []
    for source, data in results.items():
        if data:
            working_sources.append(source)
            for league_code, stats in data.items():
                logger.info(f"✅ {source} ({league_code}):")
                for stat_name, count in stats.items():
                    logger.info(f"   - {stat_name}: {count}")
    
    if working_sources:
        logger.info(f"\n🎉 SUCCESS! Working sources: {working_sources}")
        logger.info("✅ NWSL player data is available through soccerdata!")
    else:
        logger.info("\n❌ No working sources found for NWSL data")
        logger.info("⚠️  NWSL may not be supported by current soccerdata sources")
    
    return results

def test_specific_source():
    """Test with known working parameters"""
    logger.info("\n🧪 Testing with specific known parameters...")
    
    # Try FBref with standard soccer league format
    try:
        logger.info("Testing FBref with 'United States' format...")
        fbref = sd.FBref('United States', '2024')
        
        # List available leagues/competitions
        logger.info("Attempting to read schedule...")
        schedule = fbref.read_schedule()
        logger.info(f"Schedule result: {len(schedule) if not schedule.empty else 0} matches")
        
    except Exception as e:
        logger.info(f"FBref test failed: {e}")
    
    # Try ESPN
    try:
        logger.info("\nTesting ESPN with 'NWSL' format...")
        espn = sd.ESPN('NWSL', '2024')
        schedule = espn.read_schedule()
        logger.info(f"ESPN schedule: {len(schedule) if not schedule.empty else 0} matches")
        
    except Exception as e:
        logger.info(f"ESPN test failed: {e}")

if __name__ == "__main__":
    # Test all sources systematically
    results = test_nwsl_data_sources()
    
    # Test specific configurations
    test_specific_source()
    
    # Print final recommendation
    if any(results.values()):
        print("\n🎯 RECOMMENDATION: soccerdata CAN access NWSL data!")
        print("   You can proceed with BigQuery ingestion using the working source.")
    else:
        print("\n⚠️  RECOMMENDATION: NWSL may not be available in soccerdata")
        print("   Consider alternative approaches or contact soccerdata maintainers.")