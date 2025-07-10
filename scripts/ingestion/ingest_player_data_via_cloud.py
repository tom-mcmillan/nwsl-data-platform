#!/usr/bin/env python3
"""
Ingest FBref player data via cloud environment
This script uses the FBref API to get player statistics and uploads to BigQuery
"""

import os
import sys
import logging
from pathlib import Path
import subprocess

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from nwsl_analytics.config.settings import settings

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def ingest_player_data():
    """Run FBref ingestion using bq command line and local API calls"""
    logger.info("🚀 Starting NWSL player data ingestion...")
    
    # Step 1: Create a simplified script that doesn't need BigQuery client
    script_content = '''
#!/usr/bin/env python3
import os
import requests
import json
import time
import pandas as pd

def get_fbref_data():
    """Get player data from FBref API"""
    api_key = os.getenv('FBREF_API_KEY', 'KvcVSKb_a49kmsKc6nnFAPfyaLPwLqiKm4VBxA2fvmY')
    base_url = "https://fbrapi.com"
    
    headers = {
        "X-API-Key": api_key,
        "Content-Type": "application/json",
        "User-Agent": "NWSL-Analytics/1.0"
    }
    
    def rate_limit():
        time.sleep(6)  # FBref rate limit
    
    # Find NWSL league ID
    print("🔍 Finding NWSL league...")
    
    # Get countries
    rate_limit()
    response = requests.get(f"{base_url}/countries", headers=headers)
    if response.status_code == 401:
        print("❌ API key required for FBref")
        return
    
    response.raise_for_status()
    countries = response.json().get("data", [])
    
    usa_code = None
    for country in countries:
        if country.get("country", "").lower() in ["usa", "united states"]:
            usa_code = country.get("country_code")
            break
    
    if not usa_code:
        print("❌ Could not find USA")
        return
    
    # Get leagues
    rate_limit()
    response = requests.get(f"{base_url}/leagues", headers=headers, params={"country_code": usa_code})
    response.raise_for_status()
    
    league_data = response.json().get("data", [])
    nwsl_league_id = None
    
    for league_type in league_data:
        if league_type.get("league_type") == "domestic_leagues":
            for league in league_type.get("leagues", []):
                if "nwsl" in league.get("competition_name", "").lower():
                    nwsl_league_id = league.get("league_id")
                    break
    
    if not nwsl_league_id:
        print("❌ Could not find NWSL league")
        return
    
    print(f"✅ Found NWSL league: {nwsl_league_id}")
    
    # Get seasons
    rate_limit()
    response = requests.get(f"{base_url}/league-seasons", headers=headers, params={"league_id": nwsl_league_id})
    response.raise_for_status()
    
    seasons = response.json().get("data", [])
    recent_seasons = [s for s in seasons if s.get("season_id") and s.get("season_id").isdigit() and int(s.get("season_id")) >= 2020]
    
    print(f"📅 Found {len(recent_seasons)} recent seasons")
    
    # Get player data for recent seasons
    for season in recent_seasons:
        season_id = season.get("season_id")
        print(f"\\n📊 Getting player data for {season_id}...")
        
        # Player season stats
        rate_limit()
        response = requests.get(f"{base_url}/player-season-stats", headers=headers, 
                              params={"league_id": nwsl_league_id, "season_id": season_id})
        
        if response.status_code == 200:
            data = response.json().get("data", [])
            if data:
                df = pd.DataFrame(data)
                df['season_id'] = season_id
                df['ingestion_date'] = pd.Timestamp.now()
                
                # Clean column names
                df.columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_').replace('/', '_').replace('%', '_pct').replace('(', '').replace(')', '').replace('+', '_plus').lower() for col in df.columns]
                
                # Save to CSV
                filename = f"fbref_player_stats_{season_id}.csv"
                df.to_csv(filename, index=False)
                print(f"✅ Saved {len(df)} player records to {filename}")
        
        # Player match stats  
        rate_limit()
        response = requests.get(f"{base_url}/all-players-match-stats", headers=headers,
                              params={"league_id": nwsl_league_id, "season_id": season_id})
        
        if response.status_code == 200:
            data = response.json().get("data", [])
            if data:
                df = pd.DataFrame(data)
                df['season_id'] = season_id
                df['ingestion_date'] = pd.Timestamp.now()
                
                # Clean column names
                df.columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_').replace('/', '_').replace('%', '_pct').replace('(', '').replace(')', '').replace('+', '_plus').lower() for col in df.columns]
                
                # Save to CSV
                filename = f"fbref_player_match_stats_{season_id}.csv"
                df.to_csv(filename, index=False)
                print(f"✅ Saved {len(df)} player match records to {filename}")

if __name__ == "__main__":
    get_fbref_data()
'''
    
    # Write the script
    script_path = "fetch_fbref_player_data.py"
    with open(script_path, 'w') as f:
        f.write(script_content)
    
    logger.info("📝 Created FBref data fetching script")
    
    # Step 2: Run the script
    try:
        result = subprocess.run(['python', script_path], capture_output=True, text=True, check=True)
        logger.info("✅ FBref data fetching completed")
        print(result.stdout)
        if result.stderr:
            print("Warnings:", result.stderr)
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ FBref data fetching failed: {e}")
        print("STDOUT:", e.stdout)
        print("STDERR:", e.stderr)
        return
    
    # Step 3: Upload CSVs to BigQuery
    import glob
    csv_files = glob.glob("fbref_*.csv")
    
    if not csv_files:
        logger.error("❌ No CSV files found")
        return
    
    logger.info(f"📊 Found {len(csv_files)} CSV files to upload")
    
    for csv_file in csv_files:
        logger.info(f"📁 Uploading {csv_file}...")
        
        # Determine table name
        if "player_stats_" in csv_file:
            season = csv_file.split("_")[-1].replace(".csv", "")
            table_name = f"nwsl_player_season_stats_{season}"
        elif "player_match_stats_" in csv_file:
            season = csv_file.split("_")[-1].replace(".csv", "")
            table_name = f"nwsl_player_match_stats_{season}"
        else:
            continue
        
        # Upload using bq
        bq_command = f'bq load --replace --autodetect --source_format=CSV nwsl-data:nwsl_fbref.{table_name} {csv_file}'
        
        try:
            result = subprocess.run(bq_command, shell=True, capture_output=True, text=True, check=True)
            logger.info(f"✅ Uploaded {csv_file} to {table_name}")
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Failed to upload {csv_file}: {e.stderr}")
    
    # Clean up CSV files
    for csv_file in csv_files:
        os.remove(csv_file)
    
    # Clean up script
    os.remove(script_path)
    
    logger.info("🎉 NWSL player data ingestion complete!")

if __name__ == "__main__":
    ingest_player_data()