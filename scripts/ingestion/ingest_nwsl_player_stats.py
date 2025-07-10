#!/usr/bin/env python3
"""
Comprehensive NWSL Player Statistics Ingestion
Uses itscalledsoccer library to get all available player data into BigQuery
"""

import sys
import os
import logging
import time
from pathlib import Path
from typing import Optional, Dict, Any, List
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
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NWSLPlayerStatsIngester:
    """Comprehensive NWSL player statistics ingestion using itscalledsoccer"""
    
    def __init__(self, project_id: str, dataset_id: str = "nwsl_player_data"):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = bigquery.Client(project=project_id)
        
        # Create ASA client
        self.asa = AmericanSoccerAnalysis()
        
        # Seasons to process (as strings - required by ASA API)
        self.seasons = ['2024', '2025', '2023', '2022', '2021', '2020', '2019']
        
    def create_dataset_if_not_exists(self):
        """Create BigQuery dataset if it doesn't exist"""
        dataset_id = f"{self.project_id}.{self.dataset_id}"
        
        try:
            self.client.get_dataset(dataset_id)
            logger.info(f"✅ Dataset {dataset_id} already exists")
        except Exception:
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = "US"
            dataset.description = "Comprehensive NWSL player statistics from American Soccer Analysis"
            
            dataset = self.client.create_dataset(dataset, timeout=30)
            logger.info(f"✅ Created dataset {dataset_id}")
    
    def ingest_all_player_data(self) -> Dict[str, int]:
        """Ingest all available NWSL player data"""
        logger.info("⚽ Starting comprehensive NWSL player data ingestion...")
        
        results = {'tables_created': 0, 'total_rows': 0}
        
        # 1. Player roster info (all seasons combined)
        logger.info("\\n👥 Ingesting player roster information...")
        try:
            players = self.asa.get_players(leagues=['nwsl'])
            if players is not None and len(players) > 0:
                players_clean = self._clean_dataframe(players, 'players', 'all')
                rows = self._upload_to_bigquery(players_clean, 'nwsl_players_roster')
                results['total_rows'] += rows
                results['tables_created'] += 1
                logger.info(f"✅ Player roster: {rows} rows")
        except Exception as e:
            logger.error(f"❌ Player roster failed: {e}")
        
        # 2. Team information
        logger.info("\\n🏆 Ingesting team information...")
        try:
            teams = self.asa.get_teams(leagues=['nwsl'])
            if teams is not None and len(teams) > 0:
                teams_clean = self._clean_dataframe(teams, 'teams', 'all')
                rows = self._upload_to_bigquery(teams_clean, 'nwsl_teams_info')
                results['total_rows'] += rows
                results['tables_created'] += 1
                logger.info(f"✅ Teams: {rows} rows")
        except Exception as e:
            logger.error(f"❌ Teams failed: {e}")
        
        # 3. Player statistics by season
        for season in self.seasons:
            logger.info(f"\\n📅 Processing season {season}...")
            season_results = self._ingest_season_player_data(season)
            results['total_rows'] += season_results['total_rows']
            results['tables_created'] += season_results['tables_created']
        
        # 4. Salary data (not season-specific)
        logger.info("\\n💰 Ingesting salary data...")
        try:
            salaries = self.asa.get_player_salaries(leagues=['nwsl'])
            if salaries is not None and len(salaries) > 0:
                salaries_clean = self._clean_dataframe(salaries, 'salaries', 'all')
                rows = self._upload_to_bigquery(salaries_clean, 'nwsl_player_salaries')
                results['total_rows'] += rows
                results['tables_created'] += 1
                logger.info(f"✅ Player salaries: {rows} rows")
        except Exception as e:
            logger.error(f"❌ Player salaries failed: {e}")
        
        return results
    
    def _ingest_season_player_data(self, season: str) -> Dict[str, int]:
        """Ingest all player data for a specific season"""
        results = {'tables_created': 0, 'total_rows': 0}
        
        # Player data types with their methods
        player_data_types = [
            ('goals_added', 'get_player_goals_added', 'Advanced player performance metrics'),
            ('xgoals', 'get_player_xgoals', 'Expected goals statistics'),
            ('xpass', 'get_player_xpass', 'Passing and creativity statistics'),
            ('goalkeeper_goals_added', 'get_goalkeeper_goals_added', 'Goalkeeper performance metrics'),
            ('goalkeeper_xgoals', 'get_goalkeeper_xgoals', 'Goalkeeper expected goals')
        ]
        
        for data_type, method_name, description in player_data_types:
            logger.info(f"   📊 {description}...")
            
            try:
                method = getattr(self.asa, method_name)
                data = method(leagues=['nwsl'], seasons=[season])
                
                if data is not None and len(data) > 0:
                    data_clean = self._clean_dataframe(data, data_type, season)
                    table_name = f"nwsl_player_{data_type}_{season}"
                    rows = self._upload_to_bigquery(data_clean, table_name)
                    
                    if rows > 0:
                        results['total_rows'] += rows
                        results['tables_created'] += 1
                        logger.info(f"   ✅ {data_type}: {rows} rows → {table_name}")
                else:
                    logger.info(f"   ⚠️ {data_type}: No data for {season}")
                    
            except Exception as e:
                logger.warning(f"   ❌ {data_type} failed: {e}")
            
            # Small delay to be respectful to API
            time.sleep(0.5)
        
        # Also get games data for this season
        try:
            logger.info(f"   📅 Match data...")
            games = self.asa.get_games(leagues=['nwsl'], seasons=[season])
            
            if games is not None and len(games) > 0:
                games_clean = self._clean_dataframe(games, 'games', season)
                table_name = f"nwsl_games_{season}"
                rows = self._upload_to_bigquery(games_clean, table_name)
                
                if rows > 0:
                    results['total_rows'] += rows
                    results['tables_created'] += 1
                    logger.info(f"   ✅ games: {rows} rows → {table_name}")
                    
        except Exception as e:
            logger.warning(f"   ❌ Games failed for {season}: {e}")
        
        return results
    
    def _clean_dataframe(self, df: pd.DataFrame, data_type: str, season: str) -> pd.DataFrame:
        """Clean DataFrame for BigQuery upload"""
        
        # Add metadata columns
        df = df.copy()
        df['data_type'] = data_type
        df['target_season'] = season
        df['data_source'] = 'ASA_itscalledsoccer'
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
        
        # Remove any problematic characters and ensure no duplicates
        df.columns = [col.replace('__', '_').strip('_') for col in df.columns]
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
    
    def show_available_fields(self):
        """Show what player fields are available in the data"""
        logger.info("\\n🔍 Analyzing available player statistics fields...")
        
        # Test each data type to see what fields are available
        data_types_to_check = [
            ('goals_added', 'get_player_goals_added'),
            ('xgoals', 'get_player_xgoals'), 
            ('xpass', 'get_player_xpass')
        ]
        
        all_fields = {}
        
        for data_type, method_name in data_types_to_check:
            try:
                method = getattr(self.asa, method_name)
                data = method(leagues=['nwsl'], seasons=['2024'])
                
                if data is not None and len(data) > 0:
                    fields = list(data.columns)
                    all_fields[data_type] = fields
                    logger.info(f"📊 {data_type} ({len(fields)} fields): {fields[:8]}...")
                    
            except Exception as e:
                logger.warning(f"⚠️ Could not check {data_type}: {e}")
        
        # Map to your desired fields
        desired_fields = [
            'team', 'player_name', 'position', 'games_played', 'games_started',
            'minutes_played', 'goals', 'accurate_pass_percentage', 'assists',
            'total_scoring_attempts', 'on_target_scoring_attempts', 'total_attacking_assists',
            'tackles', 'fouls_committed', 'fouls_suffered', 'total_offside',
            'yellow_cards', 'red_cards', 'accurate_passes', 'total_passes',
            'crosses', 'assists_avg_over_90_mins', 'long_balls', 'successful_short_passes',
            'turnovers', 'goals_avg_over_90_mins', 'penalty_kick_goals',
            'penalty_kick_taken', 'penalty_kick_percentage', 'accurate_shooting_percentage',
            'successful_dribble', 'dribble_percentage', 'goals_and_assists',
            'tackles_percentage', 'interceptions', 'headed_duel'
        ]
        
        logger.info(f"\\n🎯 FIELD MAPPING ANALYSIS:")
        found_mappings = {}
        
        for desired in desired_fields:
            matches = []
            for data_type, available_fields in all_fields.items():
                matching = [field for field in available_fields if any(part in field.lower() for part in desired.lower().split('_'))]
                if matching:
                    matches.extend([(data_type, field) for field in matching])
            
            if matches:
                found_mappings[desired] = matches
                logger.info(f"✅ {desired}: {matches[:3]}...")  # Show first 3 matches
            else:
                logger.info(f"❌ {desired}: Not found")
        
        logger.info(f"\\n📊 SUMMARY: Found {len(found_mappings)}/{len(desired_fields)} desired fields")
        
        return all_fields, found_mappings

def test_without_bigquery():
    """Test the data availability without BigQuery authentication"""
    logger.info("🧪 Testing NWSL player data availability (no BigQuery needed)...")
    
    # Create a test instance
    class TestNWSL:
        def __init__(self):
            self.asa = AmericanSoccerAnalysis()
        
        def test_data_availability(self):
            tests = [
                ('players', lambda: self.asa.get_players(leagues=['nwsl'])),
                ('teams', lambda: self.asa.get_teams(leagues=['nwsl'])),
                ('games_2024', lambda: self.asa.get_games(leagues=['nwsl'], seasons=['2024'])),
                ('player_goals_added_2024', lambda: self.asa.get_player_goals_added(leagues=['nwsl'], seasons=['2024'])),
                ('player_xgoals_2024', lambda: self.asa.get_player_xgoals(leagues=['nwsl'], seasons=['2024'])),
                ('player_xpass_2024', lambda: self.asa.get_player_xpass(leagues=['nwsl'], seasons=['2024'])),
                ('player_salaries', lambda: self.asa.get_player_salaries(leagues=['nwsl']))
            ]
            
            results = {}
            
            for test_name, test_func in tests:
                logger.info(f"🔍 Testing {test_name}...")
                try:
                    data = test_func()
                    if data is not None and len(data) > 0:
                        logger.info(f"   ✅ {len(data)} records, {len(data.columns)} fields")
                        logger.info(f"   📊 Fields: {list(data.columns)[:6]}...")
                        results[test_name] = {
                            'records': len(data),
                            'fields': len(data.columns),
                            'sample_columns': list(data.columns)[:10]
                        }
                        
                        # Save sample
                        filename = f"sample_{test_name}.csv"
                        data.head(5).to_csv(filename, index=False)
                        logger.info(f"   💾 Sample saved to {filename}")
                    else:
                        logger.info(f"   ❌ No data")
                        results[test_name] = {'records': 0}
                        
                except Exception as e:
                    logger.info(f"   💥 Failed: {e}")
                    results[test_name] = {'error': str(e)}
            
            return results
    
    tester = TestNWSL()
    results = tester.test_data_availability()
    
    # Summary
    successful = [name for name, result in results.items() if result.get('records', 0) > 0]
    logger.info(f"\\n🎉 SUCCESS: {len(successful)} data types available: {successful}")
    
    return results

def main():
    """Main function"""
    
    # Always test first
    test_results = test_without_bigquery()
    
    # Try BigQuery ingestion if auth works
    try:
        logger.info("\\n📊 Attempting BigQuery ingestion...")
        
        ingester = NWSLPlayerStatsIngester(
            project_id=settings.gcp_project_id,
            dataset_id="nwsl_player_data"
        )
        
        # Show available fields
        ingester.show_available_fields()
        
        # Create dataset
        ingester.create_dataset_if_not_exists()
        
        # Ingest all data
        logger.info("\\n⚽ Starting comprehensive data ingestion...")
        results = ingester.ingest_all_player_data()
        
        # Summary
        logger.info(f"""
        
{'='*60}
🎉 NWSL Player Data Ingestion Complete!
{'='*60}

📊 Summary:
   - Tables created: {results['tables_created']}
   - Total rows: {results['total_rows']:,}
   - Dataset: {settings.gcp_project_id}.nwsl_player_data

🔍 Query your data:
   
   # Player roster
   bq query "SELECT player_name, primary_general_position, nationality FROM \\`{settings.gcp_project_id}.nwsl_player_data.nwsl_players_roster\\` LIMIT 10"
   
   # Player performance (goals added)
   bq query "SELECT player_name, team_name, goals_added_above_avg FROM \\`{settings.gcp_project_id}.nwsl_player_data.nwsl_player_goals_added_2024\\` ORDER BY goals_added_above_avg DESC LIMIT 10"
   
   # Expected goals leaders
   bq query "SELECT player_name, team_name, goals, xgoals FROM \\`{settings.gcp_project_id}.nwsl_player_data.nwsl_player_xgoals_2024\\` ORDER BY xgoals DESC LIMIT 10"

💡 Next steps:
   1. Update MCP server to include player statistics tables
   2. Add comprehensive player analysis tools
   3. Create player comparison and scouting features
        """)
        
    except Exception as e:
        logger.error(f"❌ BigQuery ingestion failed: {e}")
        logger.info("💡 Data availability testing completed above - check those results")

if __name__ == "__main__":
    main()