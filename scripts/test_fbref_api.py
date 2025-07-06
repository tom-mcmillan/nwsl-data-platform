#!/usr/bin/env python3
"""
Test script for FBref API client
"""

import sys
import logging
from pathlib import Path

# Add src to path so we can import our modules
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from nwsl_analytics.config.settings import settings
from nwsl_analytics.data.ingestion.fbref_client import FBrefAPIClient

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Test FBref API client"""
    logger.info("🧪 Testing FBref API client...")
    
    # Create client without API key first (to test endpoints)
    client = FBrefAPIClient(
        project_id=settings.gcp_project_id,
        dataset_id=settings.bigquery_dataset_id,
        api_key=None  # Start without API key
    )
    
    # Test connection
    logger.info("🔌 Testing API connection...")
    if client.test_connection():
        logger.info("✅ API connection successful")
    else:
        logger.error("❌ API connection failed")
        return
    
    # Find NWSL league ID
    logger.info("🔍 Finding NWSL league ID...")
    league_id = client.find_nwsl_league_id()
    if league_id:
        logger.info(f"✅ Found NWSL league ID: {league_id}")
    else:
        logger.error("❌ Could not find NWSL league ID")
        return
    
    # Get available seasons
    logger.info("📅 Getting available seasons...")
    seasons = client.get_league_seasons()
    if seasons:
        logger.info(f"✅ Found {len(seasons)} seasons:")
        for season in seasons[:5]:  # Show first 5
            logger.info(f"  - {season.get('name', 'Unknown')} (ID: {season.get('id', 'Unknown')})")
    else:
        logger.error("❌ No seasons found")
        return
    
    # Test data retrieval (this might fail without API key, but we can try)
    if seasons:
        latest_season = seasons[0]
        season_id = latest_season.get('id')
        season_name = latest_season.get('name')
        
        logger.info(f"📊 Testing data retrieval for season: {season_name}")
        
        # Test team stats
        logger.info("🏆 Testing team season stats...")
        team_stats = client.get_team_season_stats(season_id)
        if not team_stats.empty:
            logger.info(f"✅ Team stats: {len(team_stats)} rows, {len(team_stats.columns)} columns")
            logger.info(f"  Columns: {list(team_stats.columns)[:10]}...")  # Show first 10 columns
        else:
            logger.warning("⚠️ No team stats found (may need API key)")
        
        # Test player stats  
        logger.info("👥 Testing player season stats...")
        player_stats = client.get_player_season_stats(season_id)
        if not player_stats.empty:
            logger.info(f"✅ Player stats: {len(player_stats)} rows, {len(player_stats.columns)} columns")
            logger.info(f"  Columns: {list(player_stats.columns)[:10]}...")  # Show first 10 columns
        else:
            logger.warning("⚠️ No player stats found (may need API key)")
        
        # Test match data
        logger.info("⚽ Testing match data...")
        match_data = client.get_match_stats(season_id)
        if not match_data.empty:
            logger.info(f"✅ Match data: {len(match_data)} rows, {len(match_data.columns)} columns")
            logger.info(f"  Columns: {list(match_data.columns)[:10]}...")  # Show first 10 columns
        else:
            logger.warning("⚠️ No match data found (may need API key)")
    
    logger.info("🎉 FBref API client test completed!")
    logger.info("📝 Note: Full data access requires an API key from https://fbrapi.com")
    logger.info("🔑 Set API key in environment variable or pass to client constructor")

if __name__ == "__main__":
    main()