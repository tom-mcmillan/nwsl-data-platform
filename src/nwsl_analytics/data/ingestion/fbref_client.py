"""
FBref API Client for NWSL analytics
Professional soccer statistics from FBRef.com via FBR API
"""

import logging
import time
from typing import List, Optional, Dict, Any
import requests
import pandas as pd
from google.cloud import bigquery

logger = logging.getLogger(__name__)

class FBrefAPIClient:
    """Client for fetching NWSL data from FBRef via FBR API"""
    
    def __init__(self, project_id: str, dataset_id: str, api_key: Optional[str] = None):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.api_key = api_key
        self.base_url = "https://fbrapi.com"
        self.client = bigquery.Client(project=project_id)
        
        # Headers for API requests
        self.headers = {
            "Authorization": f"Bearer {api_key}" if api_key else None,
            "Content-Type": "application/json",
            "User-Agent": "NWSL-Analytics/1.0"
        }
        
        # Remove Authorization header if no API key provided
        if not api_key:
            self.headers.pop("Authorization", None)
        
        # NWSL league configuration (will be populated after finding league_id)
        self.nwsl_league_id = None
        self.nwsl_country_id = None
    
    def find_nwsl_league_id(self) -> Optional[str]:
        """Find NWSL league ID using the FBR API"""
        try:
            # First, get countries to find USA
            countries_url = f"{self.base_url}/countries"
            logger.info(f"Fetching countries from: {countries_url}")
            
            response = requests.get(countries_url, headers=self.headers)
            response.raise_for_status()
            
            countries = response.json()
            
            # Find USA
            usa_id = None
            for country in countries:
                if country.get("name", "").lower() in ["usa", "united states", "united states of america"]:
                    usa_id = country.get("id")
                    self.nwsl_country_id = usa_id
                    logger.info(f"Found USA country ID: {usa_id}")
                    break
            
            if not usa_id:
                logger.error("Could not find USA country ID")
                return None
            
            # Now get leagues for USA
            leagues_url = f"{self.base_url}/leagues"
            logger.info(f"Fetching leagues from: {leagues_url}")
            
            response = requests.get(leagues_url, headers=self.headers, params={"country_id": usa_id})
            response.raise_for_status()
            
            leagues = response.json()
            
            # Find NWSL
            for league in leagues:
                league_name = league.get("name", "").lower()
                if "nwsl" in league_name or "national women's soccer league" in league_name:
                    self.nwsl_league_id = league.get("id")
                    logger.info(f"Found NWSL league ID: {self.nwsl_league_id}")
                    return self.nwsl_league_id
            
            logger.error("Could not find NWSL league ID")
            return None
            
        except Exception as e:
            logger.error(f"Error finding NWSL league ID: {e}")
            return None
    
    def get_league_seasons(self) -> List[Dict]:
        """Get available seasons for NWSL"""
        if not self.nwsl_league_id:
            self.find_nwsl_league_id()
        
        if not self.nwsl_league_id:
            logger.error("No NWSL league ID available")
            return []
        
        try:
            url = f"{self.base_url}/seasons"
            params = {"league_id": self.nwsl_league_id}
            
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            
            seasons = response.json()
            logger.info(f"Found {len(seasons)} NWSL seasons")
            return seasons
            
        except Exception as e:
            logger.error(f"Error fetching NWSL seasons: {e}")
            return []
    
    def get_team_season_stats(self, season_id: str) -> pd.DataFrame:
        """Get team season statistics"""
        if not self.nwsl_league_id:
            self.find_nwsl_league_id()
        
        try:
            url = f"{self.base_url}/teams/season-stats"
            params = {
                "league_id": self.nwsl_league_id,
                "season_id": season_id
            }
            
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if not data:
                logger.warning(f"No team season stats for season {season_id}")
                return pd.DataFrame()
            
            df = pd.DataFrame(data)
            df['season_id'] = season_id
            df['ingestion_date'] = pd.Timestamp.now()
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching team season stats: {e}")
            return pd.DataFrame()
    
    def get_player_season_stats(self, season_id: str) -> pd.DataFrame:
        """Get player season statistics"""
        if not self.nwsl_league_id:
            self.find_nwsl_league_id()
        
        try:
            url = f"{self.base_url}/players/season-stats"
            params = {
                "league_id": self.nwsl_league_id,
                "season_id": season_id
            }
            
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if not data:
                logger.warning(f"No player season stats for season {season_id}")
                return pd.DataFrame()
            
            df = pd.DataFrame(data)
            df['season_id'] = season_id
            df['ingestion_date'] = pd.Timestamp.now()
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching player season stats: {e}")
            return pd.DataFrame()
    
    def get_match_stats(self, season_id: str) -> pd.DataFrame:
        """Get match statistics and results"""
        if not self.nwsl_league_id:
            self.find_nwsl_league_id()
        
        try:
            url = f"{self.base_url}/matches"
            params = {
                "league_id": self.nwsl_league_id,
                "season_id": season_id
            }
            
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if not data:
                logger.warning(f"No match data for season {season_id}")
                return pd.DataFrame()
            
            df = pd.DataFrame(data)
            df['season_id'] = season_id
            df['ingestion_date'] = pd.Timestamp.now()
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching match data: {e}")
            return pd.DataFrame()
    
    def get_player_match_stats(self, season_id: str) -> pd.DataFrame:
        """Get detailed player match statistics"""
        if not self.nwsl_league_id:
            self.find_nwsl_league_id()
        
        try:
            url = f"{self.base_url}/players/match-stats"
            params = {
                "league_id": self.nwsl_league_id,
                "season_id": season_id
            }
            
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if not data:
                logger.warning(f"No player match stats for season {season_id}")
                return pd.DataFrame()
            
            df = pd.DataFrame(data)
            df['season_id'] = season_id
            df['ingestion_date'] = pd.Timestamp.now()
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching player match stats: {e}")
            return pd.DataFrame()
    
    def ingest_season_data(self, season_id: str) -> Dict[str, int]:
        """Ingest all available data for a specific season"""
        logger.info(f"📅 Ingesting NWSL season {season_id} data from FBRef API...")
        
        results = {'tables_created': 0, 'total_rows': 0}
        
        try:
            # Get team season stats
            team_stats = self.get_team_season_stats(season_id)
            if not team_stats.empty:
                rows = self._upload_to_bigquery(team_stats, f'nwsl_team_season_stats_{season_id}')
                if rows > 0:
                    results['total_rows'] += rows
                    results['tables_created'] += 1
                    logger.info(f"✅ Team season stats: {rows} rows")
            
            # Get player season stats
            player_stats = self.get_player_season_stats(season_id)
            if not player_stats.empty:
                rows = self._upload_to_bigquery(player_stats, f'nwsl_player_season_stats_{season_id}')
                if rows > 0:
                    results['total_rows'] += rows
                    results['tables_created'] += 1
                    logger.info(f"✅ Player season stats: {rows} rows")
            
            # Get match data
            match_data = self.get_match_stats(season_id)
            if not match_data.empty:
                rows = self._upload_to_bigquery(match_data, f'nwsl_matches_{season_id}')
                if rows > 0:
                    results['total_rows'] += rows
                    results['tables_created'] += 1
                    logger.info(f"✅ Match data: {rows} rows")
            
            # Get player match stats
            player_match_stats = self.get_player_match_stats(season_id)
            if not player_match_stats.empty:
                rows = self._upload_to_bigquery(player_match_stats, f'nwsl_player_match_stats_{season_id}')
                if rows > 0:
                    results['total_rows'] += rows
                    results['tables_created'] += 1
                    logger.info(f"✅ Player match stats: {rows} rows")
            
        except Exception as e:
            logger.error(f"Error ingesting season {season_id}: {e}")
        
        return results
    
    def _upload_to_bigquery(self, df: pd.DataFrame, table_name: str) -> int:
        """Upload DataFrame to BigQuery"""
        
        if df is None or len(df) == 0:
            return 0
        
        try:
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
                   .lower()
                for col in df.columns
            ]
            
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
    
    def test_connection(self) -> bool:
        """Test connection to FBR API"""
        try:
            url = f"{self.base_url}/countries"
            response = requests.get(url, headers=self.headers)
            
            if response.status_code == 401:
                logger.warning("⚠️ FBR API requires authentication. Please provide API key.")
                logger.info("🔑 Get API key from: https://fbrapi.com")
                return False
            
            response.raise_for_status()
            
            logger.info("✅ FBR API connection successful")
            return True
            
        except Exception as e:
            logger.error(f"❌ FBR API connection failed: {e}")
            return False
