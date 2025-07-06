#!/usr/bin/env python3
"""
FBref NWSL Data Ingestion Script
Downloads professional NWSL statistics from FBref API to BigQuery
"""

import sys
import os
import time
import logging
from pathlib import Path

# Add src to path so we can import our modules
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from nwsl_analytics.config.settings import settings
from nwsl_analytics.data.ingestion.fbref_client import FBrefAPIClient

# Setup logging
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Main FBref ingestion function"""
    logger.info("⚽ Starting NWSL FBref data ingestion...")
    logger.info(f"📊 Project: {settings.gcp_project_id}")
    logger.info(f"💾 Dataset: {settings.bigquery_dataset_id}")
    
    # Get FBref API key from environment
    fbref_api_key = os.getenv('FBREF_API_KEY')
    
    if not fbref_api_key:
        logger.error("❌ FBREF_API_KEY not found in environment variables!")
        logger.info("📝 To get an API key:")
        logger.info("   1. Visit https://fbrapi.com")
        logger.info("   2. Sign up for an account")
        logger.info("   3. Get your API key")
        logger.info("   4. Set environment variable: export FBREF_API_KEY='your-key-here'")
        return
    
    # Create FBref client
    client = FBrefAPIClient(
        project_id=settings.gcp_project_id,
        dataset_id=settings.bigquery_dataset_id,
        api_key=fbref_api_key
    )
    
    # Test connection
    logger.info("🔌 Testing FBref API connection...")
    if not client.test_connection():
        logger.error("❌ Failed to connect to FBref API")
        return
    
    logger.info("✅ FBref API connection successful")
    
    # Find NWSL league ID
    logger.info("🔍 Finding NWSL league in FBref...")
    if not client.find_nwsl_league_id():
        logger.error("❌ Could not find NWSL league in FBref API")
        return
    
    logger.info(f"✅ Found NWSL league ID: {client.nwsl_league_id}")
    
    # Get available seasons
    logger.info("📅 Fetching available NWSL seasons...")
    seasons = client.get_league_seasons()
    
    if not seasons:
        logger.error("❌ No seasons found for NWSL")
        return
    
    logger.info(f"📊 Found {len(seasons)} NWSL seasons in FBref")
    
    # Show seasons
    for i, season in enumerate(seasons):
        logger.info(f"   {i+1}. {season.get('name', 'Unknown')} (ID: {season.get('id')})")
    
    total_stats = {'tables_created': 0, 'total_rows': 0, 'seasons_processed': 0}
    
    # Process each season
    for i, season_data in enumerate(seasons):
        season_id = season_data.get('id')
        season_name = season_data.get('name', 'Unknown')
        
        # Filter for NWSL seasons 2013-2025
        try:
            # Extract year from season name (could be "2024" or "2024 NWSL" etc)
            season_year = None
            for part in season_name.split():
                if part.isdigit() and len(part) == 4:
                    season_year = int(part)
                    break
            
            if not season_year or season_year < 2013 or season_year > 2025:
                logger.info(f"⏭️ Skipping season {season_name} (outside 2013-2025 range)")
                continue
        except:
            logger.warning(f"⚠️ Could not parse year from season name: {season_name}")
            pass
        
        logger.info(f"\n{'='*60}")
        logger.info(f"📅 Processing season {i+1}/{len(seasons)}: {season_name}")
        logger.info(f"{'='*60}")
        
        try:
            # Add delay to respect rate limits (adjust based on your API tier)
            if i > 0:
                logger.info("⏳ Waiting 3 seconds for rate limit...")
                time.sleep(3)
            
            # Ingest season data
            season_stats = client.ingest_season_data(season_id)
            
            total_stats['tables_created'] += season_stats['tables_created']
            total_stats['total_rows'] += season_stats['total_rows']
            total_stats['seasons_processed'] += 1
            
            logger.info(f"✅ Season {season_name} complete!")
            logger.info(f"   Tables: {season_stats['tables_created']}")
            logger.info(f"   Rows: {season_stats['total_rows']:,}")
            
        except Exception as e:
            logger.error(f"❌ Failed to process season {season_name}: {e}")
            # Continue with next season
    
    # Summary
    logger.info(f"""
    
{'='*60}
🎉 FBref NWSL data ingestion complete!
{'='*60}

📊 Summary:
   - Seasons processed: {total_stats['seasons_processed']}
   - Tables created: {total_stats['tables_created']}
   - Total rows: {total_stats['total_rows']:,}
   - Dataset: {settings.gcp_project_id}.{settings.bigquery_dataset_id}

🔍 Query your data:
   
   # List all tables
   bq ls {settings.gcp_project_id}:{settings.bigquery_dataset_id}
   
   # Team season stats
   bq query "SELECT * FROM \`{settings.gcp_project_id}.{settings.bigquery_dataset_id}.nwsl_team_season_stats_*\` LIMIT 5"
   
   # Player season stats (with xG!)
   bq query "SELECT player_name, goals, xg, assists, xa FROM \`{settings.gcp_project_id}.{settings.bigquery_dataset_id}.nwsl_player_season_stats_*\` WHERE season_id = '2024' ORDER BY xg DESC LIMIT 10"
   
   # Match data
   bq query "SELECT home_team, away_team, home_score, away_score, home_xg, away_xg FROM \`{settings.gcp_project_id}.{settings.bigquery_dataset_id}.nwsl_matches_*\` LIMIT 10"

💡 Next steps:
   1. Update MCP server to query from BigQuery instead of API
   2. Build analytics dashboards
   3. Create advanced sabermetrics tools
    """)

if __name__ == "__main__":
    main()