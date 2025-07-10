#!/usr/bin/env python3
"""
Test FBref Player Data
Check what player statistics are available from FBref API
"""

import os
import sys
import logging
from pathlib import Path
import requests
import json

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_fbref_api():
    """Test FBref API for player data without BigQuery"""
    
    # Get API key from environment
    api_key = os.getenv('FBREF_API_KEY', 'KvcVSKb_a49kmsKc6nnFAPfyaLPwLqiKm4VBxA2fvmY')
    
    base_url = "https://fbrapi.com"
    headers = {
        "X-API-Key": api_key,
        "Content-Type": "application/json",
        "User-Agent": "NWSL-Analytics/1.0"
    }
    
    try:
        # 1. Test connection
        logger.info("🔌 Testing FBref API connection...")
        response = requests.get(f"{base_url}/countries", headers=headers)
        if response.status_code != 200:
            logger.error(f"❌ API connection failed: {response.status_code}")
            return
        
        logger.info("✅ API connection successful")
        
        # 2. Find NWSL league ID
        logger.info("🔍 Finding NWSL league...")
        
        # Get USA country code
        countries = response.json().get("data", [])
        usa_code = None
        for country in countries:
            if country.get("country", "").lower() in ["usa", "united states"]:
                usa_code = country.get("country_code")
                break
        
        if not usa_code:
            logger.error("❌ Could not find USA country code")
            return
        
        # Get leagues for USA
        response = requests.get(f"{base_url}/leagues", headers=headers, params={"country_code": usa_code})
        league_data = response.json().get("data", [])
        
        nwsl_league_id = None
        for league_type in league_data:
            if league_type.get("league_type") == "domestic_leagues":
                for league in league_type.get("leagues", []):
                    if "nwsl" in league.get("competition_name", "").lower():
                        nwsl_league_id = league.get("league_id")
                        break
        
        if not nwsl_league_id:
            logger.error("❌ Could not find NWSL league ID")
            return
        
        logger.info(f"✅ Found NWSL league ID: {nwsl_league_id}")
        
        # 3. Get seasons
        logger.info("📅 Getting available seasons...")
        response = requests.get(f"{base_url}/league-seasons", headers=headers, params={"league_id": nwsl_league_id})
        seasons = response.json().get("data", [])
        
        logger.info(f"📊 Found {len(seasons)} seasons")
        for season in seasons[:3]:  # Show first 3
            logger.info(f"   - {season.get('season_id')} {season.get('competition_name')}")
        
        # 4. Test player season stats for 2024
        logger.info("👥 Testing player season stats for 2024...")
        response = requests.get(f"{base_url}/player-season-stats", headers=headers, params={
            "league_id": nwsl_league_id,
            "season_id": "2024"
        })
        
        if response.status_code != 200:
            logger.error(f"❌ Player stats request failed: {response.status_code}")
            logger.error(f"Response: {response.text}")
            return
        
        player_data = response.json().get("data", [])
        logger.info(f"📈 Found {len(player_data)} player records for 2024")
        
        if player_data:
            # Show sample player data
            sample_player = player_data[0]
            logger.info("👤 Sample player data fields:")
            for key in sorted(sample_player.keys())[:10]:  # Show first 10 fields
                logger.info(f"   - {key}: {sample_player.get(key)}")
            
            # Check for specific fields we want
            desired_fields = [
                'player_name', 'team', 'position', 'games_played', 'games_started',
                'minutes_played', 'goals', 'assists', 'accurate_pass_percentage',
                'total_scoring_attempts', 'on_target_scoring_attempts', 'tackles',
                'yellow_cards', 'red_cards'
            ]
            
            available_fields = []
            missing_fields = []
            
            for field in desired_fields:
                if field in sample_player:
                    available_fields.append(field)
                else:
                    missing_fields.append(field)
            
            logger.info(f"\n📊 Field Analysis:")
            logger.info(f"✅ Available fields ({len(available_fields)}): {', '.join(available_fields)}")
            logger.info(f"❌ Missing fields ({len(missing_fields)}): {', '.join(missing_fields)}")
            
            # Save sample data to file for inspection
            with open('sample_player_data.json', 'w') as f:
                json.dump(player_data[:5], f, indent=2)
            logger.info("💾 Saved sample data to sample_player_data.json")
        
        # 5. Test player match stats
        logger.info("\n🏆 Testing player match stats for 2024...")
        response = requests.get(f"{base_url}/all-players-match-stats", headers=headers, params={
            "league_id": nwsl_league_id,
            "season_id": "2024"
        })
        
        if response.status_code == 200:
            match_stats = response.json().get("data", [])
            logger.info(f"🎯 Found {len(match_stats)} player match records for 2024")
            
            if match_stats:
                sample_match = match_stats[0]
                logger.info("⚽ Sample match data fields:")
                for key in sorted(sample_match.keys())[:10]:
                    logger.info(f"   - {key}: {sample_match.get(key)}")
        else:
            logger.warning(f"⚠️ Player match stats request failed: {response.status_code}")
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")

if __name__ == "__main__":
    test_fbref_api()