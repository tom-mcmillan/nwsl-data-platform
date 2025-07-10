#!/usr/bin/env python3
"""
Deploy NWSL data to BigQuery using bq command line tool
This avoids Python authentication issues
"""

import sys
import os
import logging
import subprocess
import glob
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from nwsl_analytics.config.settings import settings

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_bq_command(command, description):
    """Run a bq command and return success/failure"""
    logger.info(f"   📋 {description}")
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True, check=True)
        return True, result.stdout
    except subprocess.CalledProcessError as e:
        logger.error(f"   ❌ Command failed: {e.stderr}")
        return False, e.stderr

def deploy_nwsl_data():
    """Deploy NWSL data to BigQuery using bq CLI"""
    logger.info("🚀 Deploying NWSL data to BigQuery via bq CLI...")
    
    # Test bq connectivity
    success, output = run_bq_command(f"bq ls --project_id={settings.gcp_project_id}", "Testing bq connectivity")
    if not success:
        logger.error("❌ bq command failed. Ensure gcloud is authenticated")
        return {'success': False, 'error': 'bq authentication failed'}
    
    logger.info("✅ bq command working")
    
    # Create dataset
    dataset_id = "nwsl_player_stats"
    full_dataset_id = f"{settings.gcp_project_id}:{dataset_id}"
    
    success, output = run_bq_command(
        f"bq mk --dataset --description='NWSL player and team statistics from American Soccer Analysis' {full_dataset_id}",
        f"Creating dataset {dataset_id}"
    )
    
    if "already exists" in output or success:
        logger.info(f"✅ Dataset {dataset_id} ready")
    else:
        logger.error(f"❌ Failed to create dataset: {output}")
        return {'success': False, 'error': f'Dataset creation failed: {output}'}
    
    # Find all NWSL CSV files
    csv_files = [f for f in glob.glob("nwsl_*.csv") if os.path.getsize(f) > 100]
    logger.info(f"📊 Found {len(csv_files)} NWSL CSV files")
    
    total_rows = 0
    tables_created = 0
    
    for csv_file in csv_files:
        logger.info(f"\n📁 Processing {csv_file}...")
        
        # Count rows
        try:
            with open(csv_file, 'r') as f:
                row_count = sum(1 for line in f) - 1  # -1 for header
            logger.info(f"   📊 {row_count} rows")
        except Exception as e:
            logger.error(f"   ❌ Failed to count rows: {e}")
            continue
        
        # Create table name from filename
        table_name = csv_file.replace('.csv', '').replace('-', '_')
        table_id = f"{full_dataset_id}.{table_name}"
        
        # Upload CSV to BigQuery
        bq_command = f"""bq load --replace --autodetect --source_format=CSV {table_id} {csv_file}"""
        
        success, output = run_bq_command(bq_command, f"Uploading {csv_file} to {table_name}")
        
        if success:
            logger.info(f"   ✅ Uploaded to {table_name}")
            total_rows += row_count
            tables_created += 1
        else:
            logger.error(f"   ❌ Failed to upload {csv_file}")
    
    # Create unified games table
    logger.info("\n🔗 Creating unified games table...")
    
    unified_query = f"""
    CREATE OR REPLACE TABLE `{settings.gcp_project_id}.{dataset_id}.nwsl_games_all` AS
    SELECT * FROM `{settings.gcp_project_id}.{dataset_id}.nwsl_games_2024`
    UNION ALL
    SELECT * FROM `{settings.gcp_project_id}.{dataset_id}.nwsl_games_2023`  
    UNION ALL
    SELECT * FROM `{settings.gcp_project_id}.{dataset_id}.nwsl_games_2022`
    UNION ALL
    SELECT * FROM `{settings.gcp_project_id}.{dataset_id}.nwsl_games_2021`
    """
    
    success, output = run_bq_command(
        f'bq query --use_legacy_sql=false "{unified_query}"',
        "Creating unified games table"
    )
    
    if success:
        logger.info("✅ Created unified nwsl_games_all table")
    
    # Summary
    logger.info(f"""
    
{'='*60}
🎉 NWSL Data Deployment Complete!
{'='*60}

📊 Summary:
   - Tables created: {tables_created}
   - Total rows: {total_rows:,}
   - Dataset: {settings.gcp_project_id}.{dataset_id}

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
        'dataset': f"{settings.gcp_project_id}.{dataset_id}"
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