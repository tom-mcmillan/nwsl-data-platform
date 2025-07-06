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

# Setup logging
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Main ingestion function"""
    logger.info("🚀 Starting NWSL data ingestion from American Soccer Analysis...")
    logger.info(f"📊 Project: {settings.gcp_project_id}")
    logger.info(f"📅 Seasons: {settings.seasons_list}")
    
    # Create ASA client
    client = ASAClient(settings.gcp_project_id, settings.bigquery_dataset_id)
    
    total_stats = {'tables_created': 0, 'total_rows': 0}
    
    # Start with recent seasons for testing
    test_seasons = ['2022', '2023', '2024']
    
    for season in test_seasons:
        try:
            logger.info(f"🏃‍♀️ Processing NWSL season {season}...")
            season_stats = client.ingest_season_data(season)
            
            total_stats['tables_created'] += season_stats['tables_created']
            total_stats['total_rows'] += season_stats['total_rows']
            
            logger.info(f"✅ Season {season} complete: {season_stats['total_rows']} rows")
            
        except Exception as e:
            logger.error(f"❌ Failed to process season {season}: {e}")
    
    logger.info(f"""
🎉 NWSL data ingestion complete!
📊 Tables created: {total_stats['tables_created']}
📈 Total rows: {total_stats['total_rows']:,}
💾 Dataset: {settings.gcp_project_id}.{settings.bigquery_dataset_id}

🔍 Check your data:
   bq query "SELECT COUNT(*) as total_players FROM \`{settings.gcp_project_id}.{settings.bigquery_dataset_id}.nwsl_player_stats\`"
    """)

if __name__ == "__main__":
    main()
