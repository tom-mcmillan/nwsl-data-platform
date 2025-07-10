#!/usr/bin/env python3
"""
NWSL Player Data Ingestion using soccerdata
Comprehensive script to extract NWSL player statistics and load into BigQuery
"""

import sys
import os
import logging
import time
from pathlib import Path
from typing import Optional, Dict, Any, List
import pandas as pd
from google.cloud import bigquery

# Add src to path so we can import our modules
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from nwsl_analytics.config.settings import settings

# Import soccerdata
try:
    import soccerdata as sd
except ImportError:
    print("❌ soccerdata not installed. Run: pip install soccerdata")
    sys.exit(1)

# Setup logging
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NWSLSoccerDataIngester:
    """Ingest NWSL player data using soccerdata library"""
    
    def __init__(self, project_id: str, dataset_id: str = "nwsl_soccerdata"):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = bigquery.Client(project=project_id)
        
        # Possible NWSL league identifiers to try
        self.nwsl_league_codes = [
            'USA-NWSL',
            'NWSL', 
            'USA-National Women\'s Soccer League',
            'United States-NWSL'
        ]
        
        # Seasons to process
        self.seasons = ['2024', '2025', '2023', '2022', '2021', '2020', '2019']
        
        # Data sources to try (in order of preference)
        self.data_sources = ['ESPN', 'FBref', 'FotMob']
        
    def create_dataset_if_not_exists(self):
        """Create BigQuery dataset if it doesn't exist"""
        dataset_id = f"{self.project_id}.{self.dataset_id}"
        
        try:
            self.client.get_dataset(dataset_id)
            logger.info(f"✅ Dataset {dataset_id} already exists")
        except Exception:
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = "US"
            dataset.description = "NWSL player statistics from soccerdata library"
            
            dataset = self.client.create_dataset(dataset, timeout=30)
            logger.info(f"✅ Created dataset {dataset_id}")
    
    def test_nwsl_availability(self) -> Dict[str, Any]:
        """Test which data sources have NWSL data"""
        results = {
            'available_sources': [],
            'working_league_codes': {},
            'data_types': {}
        }
        
        logger.info("🔍 Testing NWSL data availability across sources...")
        
        for source in self.data_sources:
            logger.info(f"\n📊 Testing {source}...")
            
            for league_code in self.nwsl_league_codes:
                logger.info(f"   Trying league code: {league_code}")
                
                try:
                    if source == 'ESPN':
                        scraper = sd.ESPN(league_code, '2024')
                    elif source == 'FBref':
                        scraper = sd.FBref(league_code, '2024')
                    elif source == 'FotMob':
                        scraper = sd.FotMob(league_code, '2024')
                    else:
                        continue
                    
                    # Test basic functionality
                    try:
                        schedule = scraper.read_schedule()
                        if not schedule.empty:
                            logger.info(f"   ✅ {source} works with {league_code} - {len(schedule)} matches found")
                            results['available_sources'].append(source)
                            results['working_league_codes'][source] = league_code
                            
                            # Test different data types
                            data_types = {}
                            
                            # Test team stats
                            try:
                                team_stats = scraper.read_team_season_stats()
                                data_types['team_season_stats'] = len(team_stats) if not team_stats.empty else 0
                                logger.info(f"      📈 Team stats: {data_types['team_season_stats']} records")
                            except Exception as e:
                                logger.info(f"      ❌ Team stats failed: {str(e)[:50]}...")
                            
                            # Test player stats
                            try:
                                player_stats = scraper.read_player_season_stats()
                                data_types['player_season_stats'] = len(player_stats) if not player_stats.empty else 0
                                logger.info(f"      👥 Player stats: {data_types['player_season_stats']} records")
                                
                                if not player_stats.empty:
                                    logger.info(f"      📊 Player columns: {list(player_stats.columns)[:10]}...")
                                    
                            except Exception as e:
                                logger.info(f"      ❌ Player stats failed: {str(e)[:50]}...")
                            
                            # Test different stat types for players
                            stat_types = ['standard', 'shooting', 'passing', 'defense', 'possession', 'misc']
                            for stat_type in stat_types:
                                try:
                                    stats = scraper.read_player_season_stats(stat_type=stat_type)
                                    if not stats.empty:
                                        data_types[f'player_{stat_type}_stats'] = len(stats)
                                        logger.info(f"      🎯 Player {stat_type}: {len(stats)} records")
                                except Exception as e:
                                    logger.info(f"      ⚠️ Player {stat_type} failed: {str(e)[:30]}...")
                            
                            results['data_types'][source] = data_types
                            break  # Found working league code for this source
                            
                    except Exception as e:
                        logger.info(f"      ❌ Schedule failed: {str(e)[:50]}...")
                        
                except Exception as e:
                    logger.info(f"   ❌ {source} failed with {league_code}: {str(e)[:50]}...")
        
        return results
    
    def ingest_player_data(self, source: str, league_code: str, season: str) -> Dict[str, int]:
        """Ingest all available player data for a season"""
        results = {'tables_created': 0, 'total_rows': 0}
        
        logger.info(f"📅 Ingesting {source} player data for {season}...")
        
        try:
            # Create scraper
            if source == 'ESPN':
                scraper = sd.ESPN(league_code, season)
            elif source == 'FBref':
                scraper = sd.FBref(league_code, season)
            elif source == 'FotMob':
                scraper = sd.FotMob(league_code, season)
            else:
                return results
            
            # Different stat types to try
            stat_types = {
                'standard': 'Basic player statistics',
                'shooting': 'Shooting and finishing stats',
                'passing': 'Passing accuracy and creativity',
                'defense': 'Defensive actions and tackles',
                'possession': 'Ball possession and dribbling',
                'misc': 'Miscellaneous stats (cards, fouls, etc.)'
            }
            
            # Ingest each stat type
            for stat_type, description in stat_types.items():
                try:
                    logger.info(f"   📊 Getting {stat_type} stats...")
                    
                    player_stats = scraper.read_player_season_stats(stat_type=stat_type)
                    
                    if not player_stats.empty:
                        # Clean and prepare data
                        player_stats = self._clean_dataframe(player_stats, season, source, stat_type)
                        
                        # Upload to BigQuery
                        table_name = f"nwsl_player_{stat_type}_stats_{season}"
                        rows = self._upload_to_bigquery(player_stats, table_name)
                        
                        if rows > 0:
                            results['tables_created'] += 1
                            results['total_rows'] += rows
                            logger.info(f"   ✅ {stat_type}: {rows} rows → {table_name}")
                        
                except Exception as e:
                    logger.warning(f"   ⚠️ {stat_type} failed: {e}")
            
            # Also get basic player stats without stat_type
            try:
                logger.info(f"   📊 Getting general player stats...")
                general_stats = scraper.read_player_season_stats()
                
                if not general_stats.empty:
                    general_stats = self._clean_dataframe(general_stats, season, source, 'general')
                    table_name = f"nwsl_player_general_stats_{season}"
                    rows = self._upload_to_bigquery(general_stats, table_name)
                    
                    if rows > 0:
                        results['tables_created'] += 1
                        results['total_rows'] += rows
                        logger.info(f"   ✅ general: {rows} rows → {table_name}")
                        
            except Exception as e:
                logger.warning(f"   ⚠️ General stats failed: {e}")
                
        except Exception as e:
            logger.error(f"❌ Failed to ingest {source} data for {season}: {e}")
        
        return results
    
    def _clean_dataframe(self, df: pd.DataFrame, season: str, source: str, stat_type: str) -> pd.DataFrame:
        """Clean DataFrame for BigQuery upload"""
        
        # Add metadata columns
        df = df.copy()
        df['season'] = season
        df['data_source'] = source
        df['stat_type'] = stat_type
        df['ingestion_date'] = pd.Timestamp.now()
        
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
               .replace('±', '_pm')
               .replace('#', 'num')
               .lower()
            for col in df.columns
        ]
        
        # Remove any problematic characters
        df.columns = [col.replace('__', '_').strip('_') for col in df.columns]
        
        # Ensure no duplicate columns
        df = df.loc[:, ~df.columns.duplicated()]
        
        return df
    
    def _upload_to_bigquery(self, df: pd.DataFrame, table_name: str) -> int:
        """Upload DataFrame to BigQuery"""
        
        if df is None or len(df) == 0:
            return 0
        
        try:
            table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
            
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",  # Replace table each time
                autodetect=True,
                create_disposition="CREATE_IF_NEEDED"
            )
            
            job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()  # Wait for completion
            
            logger.info(f"✅ Uploaded {len(df)} rows to {table_name}")
            return len(df)
            
        except Exception as e:
            logger.error(f"❌ Upload failed for {table_name}: {e}")
            return 0

def main():
    """Main ingestion function"""
    logger.info("⚽ Starting NWSL soccerdata ingestion...")
    logger.info(f"📊 Project: {settings.gcp_project_id}")
    
    # Create ingester
    ingester = NWSLSoccerDataIngester(
        project_id=settings.gcp_project_id,
        dataset_id="nwsl_soccerdata"
    )
    
    # Create dataset
    ingester.create_dataset_if_not_exists()
    
    # Test availability
    logger.info("🔍 Testing NWSL data availability...")
    availability = ingester.test_nwsl_availability()
    
    if not availability['available_sources']:
        logger.error("❌ No working data sources found for NWSL")
        return
    
    logger.info(f"✅ Found working sources: {availability['available_sources']}")
    
    # Use the best available source
    best_source = availability['available_sources'][0]
    league_code = availability['working_league_codes'][best_source]
    
    logger.info(f"🏆 Using {best_source} with league code: {league_code}")
    
    # Ingest data for each season
    total_stats = {'tables_created': 0, 'total_rows': 0, 'seasons_processed': 0}
    
    for season in ingester.seasons:
        logger.info(f"\n{'='*60}")
        logger.info(f"📅 Processing season {season}")
        logger.info(f"{'='*60}")
        
        try:
            season_stats = ingester.ingest_player_data(best_source, league_code, season)
            
            total_stats['tables_created'] += season_stats['tables_created']
            total_stats['total_rows'] += season_stats['total_rows']
            total_stats['seasons_processed'] += 1
            
            logger.info(f"✅ Season {season} complete!")
            logger.info(f"   Tables: {season_stats['tables_created']}")
            logger.info(f"   Rows: {season_stats['total_rows']:,}")
            
            # Add delay between seasons to be respectful
            if season != ingester.seasons[-1]:
                logger.info("⏳ Waiting 2 seconds...")
                time.sleep(2)
            
        except Exception as e:
            logger.error(f"❌ Failed to process season {season}: {e}")
    
    # Summary
    logger.info(f"""
    
{'='*60}
🎉 NWSL soccerdata ingestion complete!
{'='*60}

📊 Summary:
   - Seasons processed: {total_stats['seasons_processed']}
   - Tables created: {total_stats['tables_created']}
   - Total rows: {total_stats['total_rows']:,}
   - Dataset: {settings.gcp_project_id}.nwsl_soccerdata
   - Data source: {best_source}

🔍 Query your data:
   
   # List all tables
   bq ls {settings.gcp_project_id}:nwsl_soccerdata
   
   # Player standard stats
   bq query "SELECT * FROM `{settings.gcp_project_id}.nwsl_soccerdata.nwsl_player_standard_stats_2024` LIMIT 5"
   
   # Player shooting stats  
   bq query "SELECT player, team, goals, shots, shots_on_target FROM `{settings.gcp_project_id}.nwsl_soccerdata.nwsl_player_shooting_stats_2024` ORDER BY goals DESC LIMIT 10"

💡 Next steps:
   1. Update MCP server to include soccerdata player tables
   2. Add player analysis tools to analytics suite
   3. Create player comparison and ranking features
    """)

if __name__ == "__main__":
    main()