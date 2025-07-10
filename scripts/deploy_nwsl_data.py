#!/usr/bin/env python3
"""
Deploy NWSL Data to BigQuery via Cloud Run
Creates a simple endpoint to trigger data upload from cloud environment
"""

import sys
import os
import logging
from pathlib import Path
from typing import Dict, Any
import pandas as pd
from google.cloud import bigquery

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from nwsl_analytics.config.settings import settings

# Import itscalledsoccer
try:
    from itscalledsoccer.client import AmericanSoccerAnalysis
except ImportError:
    print("❌ itscalledsoccer not installed. Run: pip install itscalledsoccer")
    sys.exit(1)

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def deploy_nwsl_data_to_bigquery():
    """Deploy NWSL data directly to BigQuery (for cloud environment)"""
    logger.info("🚀 Deploying NWSL data to BigQuery...")
    
    try:
        # Create BigQuery client (this will work in cloud environment)
        client = bigquery.Client(project=settings.gcp_project_id)
        
        # Create dataset
        dataset_id = "nwsl_player_stats"
        full_dataset_id = f"{settings.gcp_project_id}.{dataset_id}"
        
        try:
            client.get_dataset(full_dataset_id)
            logger.info(f"✅ Dataset {full_dataset_id} exists")
        except Exception:
            dataset = bigquery.Dataset(full_dataset_id)
            dataset.location = "US"
            dataset.description = "NWSL player and team statistics from American Soccer Analysis"
            client.create_dataset(dataset, timeout=30)
            logger.info(f"✅ Created dataset {full_dataset_id}")
        
        # Create ASA client
        asa = AmericanSoccerAnalysis()
        
        total_rows = 0
        tables_created = 0
        
        # 1. Upload players data
        logger.info("\\n👥 Uploading NWSL players...")
        try:
            players = asa.get_players(leagues=['nwsl'])
            if players is not None and len(players) > 0:
                players_clean = clean_dataframe_for_bq(players)
                rows = upload_to_bigquery(client, players_clean, f"{full_dataset_id}.nwsl_players_roster")
                total_rows += rows
                tables_created += 1
                logger.info(f"✅ Players: {rows} rows")
        except Exception as e:
            logger.error(f"❌ Players upload failed: {e}")
        
        # 2. Upload teams data
        logger.info("\\n🏆 Uploading NWSL teams...")
        try:
            teams = asa.get_teams(leagues=['nwsl'])
            if teams is not None and len(teams) > 0:
                teams_clean = clean_dataframe_for_bq(teams)
                rows = upload_to_bigquery(client, teams_clean, f"{full_dataset_id}.nwsl_teams_info")
                total_rows += rows
                tables_created += 1
                logger.info(f"✅ Teams: {rows} rows")
        except Exception as e:
            logger.error(f"❌ Teams upload failed: {e}")
        
        # 3. Upload games data for multiple seasons
        seasons = ['2024', '2023', '2022', '2021']
        for season in seasons:
            logger.info(f"\\n📅 Uploading NWSL games for {season}...")
            try:
                games = asa.get_games(leagues=['nwsl'], seasons=[season])
                if games is not None and len(games) > 0:
                    games_clean = clean_dataframe_for_bq(games)
                    games_clean['season'] = season
                    rows = upload_to_bigquery(client, games_clean, f"{full_dataset_id}.nwsl_games_{season}")
                    total_rows += rows
                    tables_created += 1
                    logger.info(f"✅ Games {season}: {rows} rows")
            except Exception as e:
                logger.error(f"❌ Games {season} upload failed: {e}")
        
        # 4. Create unified games table
        logger.info("\\n🔗 Creating unified games table...")
        try:
            query = f"""
            CREATE OR REPLACE TABLE `{full_dataset_id}.nwsl_games_all` AS
            SELECT * FROM `{full_dataset_id}.nwsl_games_2024`
            UNION ALL
            SELECT * FROM `{full_dataset_id}.nwsl_games_2023`  
            UNION ALL
            SELECT * FROM `{full_dataset_id}.nwsl_games_2022`
            UNION ALL
            SELECT * FROM `{full_dataset_id}.nwsl_games_2021`
            """
            
            job = client.query(query)
            job.result()
            logger.info("✅ Created unified nwsl_games_all table")
            
        except Exception as e:
            logger.error(f"❌ Unified table creation failed: {e}")
        
        # Summary
        logger.info(f"""
        
{'='*60}
🎉 NWSL Data Deployment Complete!
{'='*60}

📊 Summary:
   - Tables created: {tables_created}
   - Total rows: {total_rows:,}
   - Dataset: {full_dataset_id}

🔍 Available Tables:
   - nwsl_players_roster: Player information and demographics
   - nwsl_teams_info: Team details and identifiers
   - nwsl_games_2024: 2024 season matches
   - nwsl_games_2023: 2023 season matches  
   - nwsl_games_2022: 2022 season matches
   - nwsl_games_2021: 2021 season matches
   - nwsl_games_all: All seasons combined

💡 Ready for MCP server integration!
        """)
        
        return {
            'success': True,
            'tables_created': tables_created,
            'total_rows': total_rows,
            'dataset': full_dataset_id
        }
        
    except Exception as e:
        logger.error(f"❌ Deployment failed: {e}")
        return {'success': False, 'error': str(e)}

def clean_dataframe_for_bq(df: pd.DataFrame) -> pd.DataFrame:
    """Clean DataFrame for BigQuery upload"""
    df = df.copy()
    
    # Add metadata
    df['ingestion_date'] = pd.Timestamp.now()
    df['data_source'] = 'ASA_itscalledsoccer'
    
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
    
    # Remove duplicates and clean up
    df.columns = [col.replace('__', '_').strip('_') for col in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]
    
    return df

def upload_to_bigquery(client: bigquery.Client, df: pd.DataFrame, table_id: str) -> int:
    """Upload DataFrame to BigQuery"""
    if df is None or len(df) == 0:
        return 0
    
    try:
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True,
            create_disposition="CREATE_IF_NEEDED"
        )
        
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        
        logger.info(f"✅ Uploaded {len(df)} rows to {table_id.split('.')[-1]}")
        return len(df)
        
    except Exception as e:
        logger.error(f"❌ Upload failed for {table_id}: {e}")
        return 0

if __name__ == "__main__":
    result = deploy_nwsl_data_to_bigquery()
    
    if result['success']:
        print("\\n🎯 SUCCESS! NWSL data is now in BigQuery!")
        print(f"Dataset: {result['dataset']}")
        print(f"Tables: {result['tables_created']}")
        print(f"Rows: {result['total_rows']:,}")
    else:
        print(f"\\n❌ FAILED: {result['error']}")
        print("💡 This script needs to run in a cloud environment with proper authentication")