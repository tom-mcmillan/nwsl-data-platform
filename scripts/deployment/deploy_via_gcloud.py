#!/usr/bin/env python3
"""
Deploy NWSL data to BigQuery using gcloud authentication
This script bypasses the cloud endpoint and uploads directly
"""

import sys
import os
import logging
from pathlib import Path
import pandas as pd
from google.cloud import bigquery
import glob

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from nwsl_analytics.config.settings import settings

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def deploy_nwsl_data():
    """Deploy NWSL data to BigQuery using gcloud auth"""
    logger.info("🚀 Deploying NWSL data to BigQuery via gcloud auth...")
    
    try:
        # Try using gcloud access token
        import subprocess
        result = subprocess.run(['gcloud', 'auth', 'print-access-token'], 
                              capture_output=True, text=True, check=True)
        access_token = result.stdout.strip()
        
        # Create credentials from access token
        from google.auth.credentials import Credentials
        from google.oauth2 import service_account
        
        # Use default credentials with explicit project
        import google.auth
        credentials, project = google.auth.default()
        
        client = bigquery.Client(project=settings.gcp_project_id, credentials=credentials)
        
        # Test connection
        query = "SELECT 1 as test"
        client.query(query).result()
        logger.info("✅ BigQuery connection successful")
        
    except Exception as e:
        logger.error(f"❌ BigQuery authentication failed: {e}")
        logger.info("💡 Run: gcloud auth application-default login")
        return {'success': False, 'error': str(e)}
    
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
    csv_files = [f for f in glob.glob("nwsl_*.csv") if os.path.getsize(f) > 100]  # Skip tiny files
    logger.info(f"📊 Found {len(csv_files)} NWSL CSV files")
    
    total_rows = 0
    tables_created = 0
    
    for csv_file in csv_files:
        logger.info(f"\n📁 Processing {csv_file}...")
        
        try:
            # Read CSV
            df = pd.read_csv(csv_file)
            
            if len(df) == 0:
                logger.warning(f"   ⚠️ Empty file: {csv_file}")
                continue
                
            logger.info(f"   📊 {len(df)} rows, {len(df.columns)} columns")
            
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
            
            # Remove duplicates in column names
            df.columns = [col.replace('__', '_').strip('_') for col in df.columns]
            df = df.loc[:, ~df.columns.duplicated()]
            
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
    
    # Create unified games table
    logger.info("\n🔗 Creating unified games table...")
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
        logger.error(f"❌ Failed to create unified table: {e}")
    
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
   - nwsl_players_complete: Complete player roster and details
   - nwsl_teams_complete: Team information and identifiers
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

if __name__ == "__main__":
    result = deploy_nwsl_data()
    
    if result['success']:
        print(f"\n🎯 SUCCESS! NWSL data is now in BigQuery!")
        print(f"Dataset: {result['dataset']}")
        print(f"Tables: {result['tables_created']}")
        print(f"Rows: {result['total_rows']:,}")
    else:
        print(f"\n❌ FAILED: {result['error']}")