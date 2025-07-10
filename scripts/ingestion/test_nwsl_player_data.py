#!/usr/bin/env python3
"""
Test NWSL Player Data from FBref API
Check what player statistics are available using the correct NWSL league ID
"""

import os
import requests
import json
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_nwsl_player_data():
    """Test NWSL player data with known league ID"""
    
    api_key = os.getenv('FBREF_API_KEY', 'KvcVSKb_a49kmsKc6nnFAPfyaLPwLqiKm4VBxA2fvmY')
    nwsl_league_id = "182"  # National Women's Soccer League
    
    headers = {
        "X-API-Key": api_key,
        "Content-Type": "application/json",
        "User-Agent": "NWSL-Analytics/1.0"
    }
    
    try:
        # 1. Get available seasons
        logger.info("📅 Getting NWSL seasons...")
        response = requests.get("https://fbrapi.com/league-seasons", headers=headers, params={"league_id": nwsl_league_id})
        
        if response.status_code != 200:
            logger.error(f"❌ Seasons request failed: {response.status_code}")
            return
        
        seasons = response.json().get("data", [])
        logger.info(f"📊 Found {len(seasons)} NWSL seasons")
        
        for season in seasons:
            logger.info(f"   - {season.get('season_id')} {season.get('competition_name')}")
        
        # 2. Test player season stats for 2024
        logger.info("\n👥 Getting player season stats for 2024...")
        response = requests.get("https://fbrapi.com/player-season-stats", headers=headers, params={
            "league_id": nwsl_league_id,
            "season_id": "2024"
        })
        
        if response.status_code != 200:
            logger.error(f"❌ Player stats failed: {response.status_code}")
            logger.error(f"Response: {response.text}")
            return
        
        player_data = response.json().get("data", [])
        logger.info(f"📈 Found {len(player_data)} player records for 2024")
        
        if not player_data:
            logger.warning("⚠️ No player data found")
            return
        
        # Show sample player
        sample_player = player_data[0]
        logger.info(f"\n👤 Sample player: {sample_player.get('player_name', 'Unknown')}")
        logger.info(f"   Team: {sample_player.get('team', 'Unknown')}")
        logger.info(f"   Position: {sample_player.get('position', 'Unknown')}")
        
        logger.info("\n📊 All available fields:")
        all_fields = sorted(sample_player.keys())
        for i, field in enumerate(all_fields):
            logger.info(f"   {i+1:2d}. {field}: {sample_player.get(field)}")
        
        # Check specific fields we want
        desired_fields = [
            'player_name', 'team', 'position', 'games_played', 'games_started',
            'minutes_played', 'goals', 'assists', 'accurate_pass_percentage',
            'total_scoring_attempts', 'on_target_scoring_attempts', 'tackles',
            'yellow_cards', 'red_cards', 'fouls_committed', 'fouls_suffered',
            'crosses', 'long_balls', 'turnovers', 'penalty_kick_goals',
            'successful_dribble', 'interceptions'
        ]
        
        logger.info(f"\n🎯 Checking for desired fields:")
        available = []
        missing = []
        
        for field in desired_fields:
            if field in sample_player:
                available.append(field)
                logger.info(f"   ✅ {field}: {sample_player.get(field)}")
            else:
                missing.append(field)
                logger.info(f"   ❌ {field}: NOT FOUND")
        
        logger.info(f"\n📊 Summary:")
        logger.info(f"   ✅ Available: {len(available)}/{len(desired_fields)} fields")
        logger.info(f"   ❌ Missing: {len(missing)} fields")
        
        # Save full sample to file
        with open('nwsl_player_sample.json', 'w') as f:
            json.dump(player_data[:3], f, indent=2)
        logger.info(f"\n💾 Saved 3 player samples to nwsl_player_sample.json")
        
        # 3. Test player match stats
        logger.info("\n🏆 Testing player match stats for 2024...")
        response = requests.get("https://fbrapi.com/all-players-match-stats", headers=headers, params={
            "league_id": nwsl_league_id,
            "season_id": "2024"
        })
        
        if response.status_code == 200:
            match_data = response.json().get("data", [])
            logger.info(f"🎯 Found {len(match_data)} player match records")
            
            if match_data:
                sample_match = match_data[0]
                logger.info(f"\n⚽ Sample match record fields:")
                for field in sorted(sample_match.keys())[:15]:  # Show first 15
                    logger.info(f"   - {field}: {sample_match.get(field)}")
        else:
            logger.warning(f"⚠️ Match stats failed: {response.status_code}")
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")

if __name__ == "__main__":
    test_nwsl_player_data()