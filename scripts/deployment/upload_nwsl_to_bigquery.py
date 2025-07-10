#!/usr/bin/env python3
"""
Upload NWSL CSV data to BigQuery
Takes the CSV files we successfully created and uploads them to BigQuery
"""

import sys
import os
import logging
from pathlib import Path
import pandas as pd
from google.cloud import bigquery
import glob

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from nwsl_analytics.config.settings import settings

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def upload_csv_files_to_bigquery():
    """Upload all NWSL CSV files to BigQuery"""
    
    # Check if we can run this with gcloud authentication
    try:
        # Use gcloud authentication instead of service account file
        client = bigquery.Client(project=settings.gcp_project_id)
        
        # Test connection
        query = "SELECT 1 as test"
        client.query(query).result()
        logger.info("✅ BigQuery connection successful")
        
    except Exception as e:
        logger.error(f"❌ BigQuery authentication failed: {e}")
        logger.info("💡 Run this script from Cloud Shell or with proper gcloud auth")
        return
    
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
    
    # Find all NWSL CSV files
    csv_files = glob.glob("nwsl_*.csv")
    logger.info(f"📊 Found {len(csv_files)} NWSL CSV files: {csv_files}")
    
    total_rows = 0
    tables_created = 0
    
    for csv_file in csv_files:
        logger.info(f"\\n📁 Processing {csv_file}...")
        
        try:
            # Read CSV
            df = pd.read_csv(csv_file)
            logger.info(f"   📊 {len(df)} rows, {len(df.columns)} columns")
            
            # Create table name from filename
            table_name = csv_file.replace('.csv', '').replace('-', '_')
            table_id = f"{full_dataset_id}.{table_name}"
            
            # Configure load job
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",  # Replace if exists
                autodetect=True,
                create_disposition="CREATE_IF_NEEDED"
            )
            
            # Upload to BigQuery
            job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()  # Wait for completion
            
            logger.info(f"   ✅ Uploaded to {table_name}")
            total_rows += len(df)
            tables_created += 1
            
        except Exception as e:
            logger.error(f"   ❌ Failed to upload {csv_file}: {e}")
    
    # Summary
    logger.info(f"""
    
{'='*60}
🎉 NWSL Data Upload Complete!
{'='*60}

📊 Summary:
   - Tables created: {tables_created}
   - Total rows uploaded: {total_rows:,}
   - Dataset: {full_dataset_id}

🔍 Query your data:

# Player roster
bq query --use_legacy_sql=false "
SELECT player_name, primary_general_position, nationality, season_name 
FROM \\`{full_dataset_id}.nwsl_players_complete\\` 
WHERE primary_general_position = 'ST' 
LIMIT 10"

# Team information  
bq query --use_legacy_sql=false "
SELECT team_name, team_abbreviation 
FROM \\`{full_dataset_id}.nwsl_teams_complete\\` 
ORDER BY team_name"

# Recent matches
bq query --use_legacy_sql=false "
SELECT date_time_utc, home_team_id, away_team_id, home_score, away_score, attendance
FROM \\`{full_dataset_id}.nwsl_games_2024\\` 
ORDER BY date_time_utc DESC 
LIMIT 10"

# Goals scored by season
bq query --use_legacy_sql=false "
SELECT season, 
       COUNT(*) as total_games,
       SUM(home_score + away_score) as total_goals,
       ROUND(AVG(home_score + away_score), 2) as avg_goals_per_game
FROM (
  SELECT season, home_score, away_score FROM \\`{full_dataset_id}.nwsl_games_2024\\`
  UNION ALL
  SELECT season, home_score, away_score FROM \\`{full_dataset_id}.nwsl_games_2023\\`
  UNION ALL  
  SELECT season, home_score, away_score FROM \\`{full_dataset_id}.nwsl_games_2022\\`
  UNION ALL
  SELECT season, home_score, away_score FROM \\`{full_dataset_id}.nwsl_games_2021\\`
)
GROUP BY season
ORDER BY season DESC"

💡 Next Steps:
1. Update your MCP server to include these new tables
2. Add player and team analysis tools
3. Create NWSL-specific analytics dashboards
4. Consider supplementing with additional data sources for detailed player stats
    """)

def create_unified_games_table():
    """Create a unified games table across all seasons"""
    logger.info("\\n🔗 Creating unified games table...")
    
    try:
        client = bigquery.Client(project=settings.gcp_project_id)
        dataset_id = f"{settings.gcp_project_id}.nwsl_player_stats"
        
        # Create unified games view
        query = f"""
        CREATE OR REPLACE TABLE `{dataset_id}.nwsl_games_all` AS
        SELECT * FROM `{dataset_id}.nwsl_games_2024`
        UNION ALL
        SELECT * FROM `{dataset_id}.nwsl_games_2023`  
        UNION ALL
        SELECT * FROM `{dataset_id}.nwsl_games_2022`
        UNION ALL
        SELECT * FROM `{dataset_id}.nwsl_games_2021`
        """
        
        job = client.query(query)
        job.result()
        
        logger.info("✅ Created unified nwsl_games_all table")
        
    except Exception as e:
        logger.error(f"❌ Failed to create unified table: {e}")

if __name__ == "__main__":
    # Upload CSV files
    upload_csv_files_to_bigquery()
    
    # Create unified tables
    create_unified_games_table()
    
    print("\\n🎯 SUCCESS! NWSL data is now in BigQuery and ready for analytics!")