#!/usr/bin/env python3
"""
American Soccer Analysis (ASA) Player Data Ingestion
ASA has comprehensive NWSL player statistics including the fields you requested
"""

import sys
import os
import logging
import time
from pathlib import Path
from typing import Optional, Dict, Any, List
import pandas as pd
import requests
from google.cloud import bigquery

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from nwsl_analytics.config.settings import settings

# Setup logging
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ASAPlayerDataIngester:
    """Ingest NWSL player data from American Soccer Analysis API"""
    
    def __init__(self, project_id: str, dataset_id: str = "nwsl_player_stats"):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = bigquery.Client(project=project_id)
        
        # ASA API base URL
        self.base_url = "https://app.americansocceranalysis.com/api/v1"
        
        # Headers for requests
        self.headers = {
            "User-Agent": "NWSL-Analytics/1.0"
        }
        
        # Available seasons
        self.seasons = [2024, 2023, 2022, 2021, 2020, 2019, 2018, 2017, 2016]
        
    def test_asa_availability(self) -> Dict[str, Any]:
        """Test ASA API for NWSL player data availability"""
        logger.info("🔍 Testing American Soccer Analysis API for NWSL data...")
        
        results = {
            'api_working': False,
            'available_endpoints': [],
            'sample_data': {}
        }
        
        # Test endpoints that likely have player data
        endpoints_to_test = [
            'nwsl/players',
            'nwsl/player-stats', 
            'nwsl/player-season-stats',
            'nwsl/player-goals-added',
            'nwsl/players/stats'
        ]
        
        for endpoint in endpoints_to_test:
            url = f"{self.base_url}/{endpoint}"
            logger.info(f"   🔍 Testing: {endpoint}")
            
            try:
                # Try with 2024 season parameter
                response = requests.get(url, headers=self.headers, params={'season': 2024}, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if isinstance(data, list) and len(data) > 0:
                        logger.info(f"   ✅ {endpoint}: {len(data)} records found")
                        results['available_endpoints'].append(endpoint)
                        results['api_working'] = True
                        
                        # Save sample data
                        sample = data[0]
                        results['sample_data'][endpoint] = sample
                        
                        # Show sample fields
                        if isinstance(sample, dict):
                            fields = list(sample.keys())
                            logger.info(f"      📊 Sample fields: {fields[:10]}...")
                            
                            # Check for key player stats we want
                            desired_fields = [
                                'player_name', 'team', 'position', 'games_played', 
                                'goals', 'assists', 'minutes_played'
                            ]
                            
                            found_fields = [f for f in desired_fields if f in fields or any(d in f.lower() for d in f.lower().split('_'))]
                            if found_fields:
                                logger.info(f"      🎯 Relevant fields found: {found_fields}")
                    
                    elif isinstance(data, dict):
                        logger.info(f"   ✅ {endpoint}: Got response object")
                        results['available_endpoints'].append(endpoint)
                        results['sample_data'][endpoint] = data
                        
                else:
                    logger.info(f"   ❌ {endpoint}: HTTP {response.status_code}")
                    
            except Exception as e:
                logger.info(f"   💥 {endpoint}: {str(e)[:50]}...")
                
            # Small delay between requests
            time.sleep(0.5)
        
        return results
    
    def get_player_data(self, season: int) -> pd.DataFrame:
        """Get comprehensive player data for a season"""
        logger.info(f"📊 Getting ASA player data for {season}...")
        
        all_data = []
        
        # Try different endpoints that might have player data
        endpoints = [
            f"{self.base_url}/nwsl/players",
            f"{self.base_url}/nwsl/player-stats", 
            f"{self.base_url}/nwsl/player-season-stats"
        ]
        
        for endpoint in endpoints:
            try:
                logger.info(f"   🔗 Trying {endpoint.split('/')[-1]}...")
                
                response = requests.get(
                    endpoint, 
                    headers=self.headers, 
                    params={'season': season},
                    timeout=30
                )
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if isinstance(data, list) and len(data) > 0:
                        df = pd.DataFrame(data)
                        logger.info(f"   ✅ Got {len(df)} records from {endpoint.split('/')[-1]}")
                        
                        # Add metadata
                        df['season'] = season
                        df['data_source'] = 'ASA'
                        df['endpoint'] = endpoint.split('/')[-1]
                        df['ingestion_date'] = pd.Timestamp.now()
                        
                        all_data.append(df)
                        
                        # Show sample data
                        if not df.empty:
                            sample = df.iloc[0]
                            player_name = sample.get('player_name', sample.get('name', 'Unknown'))
                            team = sample.get('team', sample.get('team_name', 'Unknown'))
                            logger.info(f"      👤 Sample: {player_name} ({team})")
                        
                        break  # Found working endpoint
                        
            except Exception as e:
                logger.warning(f"   ⚠️ {endpoint}: {e}")
        
        # Combine all data
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            return combined_df
        else:
            return pd.DataFrame()
    
    def create_dataset_if_not_exists(self):
        """Create BigQuery dataset if it doesn't exist"""
        dataset_id = f"{self.project_id}.{self.dataset_id}"
        
        try:
            self.client.get_dataset(dataset_id)
            logger.info(f"✅ Dataset {dataset_id} already exists")
        except Exception:
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = "US"
            dataset.description = "NWSL player statistics from American Soccer Analysis"
            
            dataset = self.client.create_dataset(dataset, timeout=30)
            logger.info(f"✅ Created dataset {dataset_id}")
    
    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean DataFrame for BigQuery"""
        if df.empty:
            return df
            
        # Clean column names
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
        
        # Remove duplicate columns
        df = df.loc[:, ~df.columns.duplicated()]
        
        return df
    
    def upload_to_bigquery(self, df: pd.DataFrame, table_name: str) -> int:
        """Upload DataFrame to BigQuery"""
        if df.empty:
            return 0
            
        try:
            table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
            
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
                autodetect=True,
                create_disposition="CREATE_IF_NEEDED"
            )
            
            job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()
            
            logger.info(f"✅ Uploaded {len(df)} rows to {table_name}")
            return len(df)
            
        except Exception as e:
            logger.error(f"❌ Upload failed for {table_name}: {e}")
            return 0

def test_without_bigquery():
    """Test ASA API without requiring BigQuery authentication"""
    logger.info("🧪 Testing ASA API (no BigQuery required)...")
    
    # Create test instance without BigQuery
    class TestASA:
        def __init__(self):
            self.base_url = "https://app.americansocceranalysis.com/api/v1"
            self.headers = {"User-Agent": "NWSL-Analytics/1.0"}
        
        def test_api(self):
            endpoints = [
                'nwsl/players',
                'nwsl/player-stats',
                'nwsl/teams',
                'nwsl/games'
            ]
            
            for endpoint in endpoints:
                url = f"{self.base_url}/{endpoint}"
                logger.info(f"🔍 Testing: {url}")
                
                try:
                    response = requests.get(url, headers=self.headers, params={'season': 2024}, timeout=10)
                    logger.info(f"   Status: {response.status_code}")
                    
                    if response.status_code == 200:
                        data = response.json()
                        if isinstance(data, list):
                            logger.info(f"   📊 Records: {len(data)}")
                            if data:
                                sample = data[0]
                                logger.info(f"   📝 Sample fields: {list(sample.keys())[:8]}...")
                        else:
                            logger.info(f"   📝 Response type: {type(data)}")
                    
                except Exception as e:
                    logger.info(f"   ❌ Error: {e}")
                
                time.sleep(0.5)
    
    tester = TestASA()
    tester.test_api()

def main():
    """Main function - test first, then ingest if BigQuery auth works"""
    
    # Always test API availability first
    test_without_bigquery()
    
    # Try BigQuery ingestion if authentication works
    try:
        logger.info("\n📊 Attempting BigQuery ingestion...")
        
        ingester = ASAPlayerDataIngester(
            project_id=settings.gcp_project_id,
            dataset_id="nwsl_player_stats"
        )
        
        # Test ASA availability
        availability = ingester.test_asa_availability()
        
        if availability['api_working']:
            logger.info("✅ ASA API is working! Proceeding with data ingestion...")
            
            # Create dataset
            ingester.create_dataset_if_not_exists()
            
            # Ingest data for recent seasons
            total_rows = 0
            for season in [2024, 2023, 2022]:
                logger.info(f"\n📅 Processing season {season}...")
                
                player_data = ingester.get_player_data(season)
                
                if not player_data.empty:
                    cleaned_data = ingester.clean_dataframe(player_data)
                    table_name = f"asa_nwsl_players_{season}"
                    rows = ingester.upload_to_bigquery(cleaned_data, table_name)
                    total_rows += rows
                    
                    # Save local copy
                    filename = f"nwsl_players_{season}.csv"
                    cleaned_data.to_csv(filename, index=False)
                    logger.info(f"💾 Saved to {filename}")
            
            logger.info(f"\n🎉 Ingestion complete! Total rows: {total_rows}")
        else:
            logger.warning("⚠️ ASA API not responding as expected")
            
    except Exception as e:
        logger.error(f"❌ BigQuery ingestion failed: {e}")
        logger.info("💡 API testing completed above - check those results")

if __name__ == "__main__":
    main()