#!/usr/bin/env python3
"""
NWSL Data Ingestion Script
Professional data pipeline from American Soccer Analysis to BigQuery
"""

import sys
import logging
from pathlib import Path

# Add src to path so we can import our modules
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from nwsl_analytics.config.settings import settings
from nwsl_analytics.data.ingestion.asa_client import ASAClient
from nwsl_analytics.data.ingestion.fbref_client import FBrefAPIClient

# Setup logging
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Main ingestion function"""
    logger.info("🚀 Starting NWSL data ingestion...")
    logger.info(f"📊 Project: {settings.gcp_project_id}")
    logger.info(f"📅 Seasons: {settings.seasons_list}")
    
    # Create clients
    asa_client = ASAClient(settings.gcp_project_id, settings.bigquery_dataset_id)
    
    # Get FBref API key from environment or settings
    import os
    fbref_api_key = os.getenv('FBREF_API_KEY')
    
    total_stats = {'tables_created': 0, 'total_rows': 0}
    
    # All NWSL seasons from inception to current
    all_seasons = [str(year) for year in range(2013, 2026)]  # 2013-2025
    
    # 1. Ingest from American Soccer Analysis
    logger.info("📚 Phase 1: Ingesting from American Soccer Analysis...")
    for season in all_seasons:
        try:
            logger.info(f"🏃‍♀️ Processing ASA season {season}...")
            season_stats = asa_client.ingest_season_data(season)
            
            total_stats['tables_created'] += season_stats['tables_created']
            total_stats['total_rows'] += season_stats['total_rows']
            
            logger.info(f"✅ ASA Season {season} complete: {season_stats['total_rows']} rows")
            
        except Exception as e:
            logger.error(f"❌ Failed to process ASA season {season}: {e}")
    
    # 2. Ingest from FBref API (if we have access)
    logger.info("\n📚 Phase 2: Attempting FBref data ingestion...")
    
    fbref_client = FBrefAPIClient(
        project_id=settings.gcp_project_id,
        dataset_id=settings.bigquery_dataset_id,
        api_key=fbref_api_key
    )
    
    # Test connection first
    if not fbref_client.test_connection():
        logger.warning("⚠️ FBref API connection failed. Skipping FBref data ingestion.")
        logger.info("💡 To enable FBref data: Get API key from https://fbrapi.com and set FBREF_API_KEY env variable")
    else:
        # Find NWSL league
        if fbref_client.find_nwsl_league_id():
            # Get available seasons
            fbref_seasons = fbref_client.get_league_seasons()
            logger.info(f"📅 Found {len(fbref_seasons)} FBref seasons for NWSL")
            
            # Ingest each season
            for season_data in fbref_seasons:
                season_id = season_data.get('id')
                season_name = season_data.get('name', 'Unknown')
                
                # Skip if not in our target range
                try:
                    season_year = int(season_name.split()[0]) if season_name else None
                    if season_year and (season_year < 2013 or season_year > 2025):
                        continue
                except:
                    pass
                
                try:
                    logger.info(f"⚽ Processing FBref season: {season_name} (ID: {season_id})")
                    
                    # Add delay to respect rate limits
                    import time
                    time.sleep(2)  # 2 second delay between seasons
                    
                    season_stats = fbref_client.ingest_season_data(season_id)
                    
                    total_stats['tables_created'] += season_stats['tables_created']
                    total_stats['total_rows'] += season_stats['total_rows']
                    
                    logger.info(f"✅ FBref Season {season_name} complete: {season_stats['total_rows']} rows")
                    
                except Exception as e:
                    logger.error(f"❌ Failed to process FBref season {season_name}: {e}")
                    # Continue with other seasons
        else:
            logger.error("❌ Could not find NWSL league in FBref API")
    
    logger.info(f"""
🎉 NWSL data ingestion complete!
📊 Tables created: {total_stats['tables_created']}
📈 Total rows: {total_stats['total_rows']:,}
💾 Dataset: {settings.gcp_project_id}.{settings.bigquery_dataset_id}

🔍 Check your data:
   bq query "SELECT COUNT(*) as total_games FROM \`{settings.gcp_project_id}.{settings.bigquery_dataset_id}.nwsl_games_2024\`"
   bq query "SELECT COUNT(*) as total_teams FROM \`{settings.gcp_project_id}.{settings.bigquery_dataset_id}.nwsl_teams_all\`"
   
📊 FBref tables (if available):
   bq query "SELECT * FROM \`{settings.gcp_project_id}.{settings.bigquery_dataset_id}.nwsl_team_season_stats_*\` LIMIT 5"
   bq query "SELECT * FROM \`{settings.gcp_project_id}.{settings.bigquery_dataset_id}.nwsl_player_season_stats_*\` LIMIT 5"
    """)

if __name__ == "__main__":
    main()
