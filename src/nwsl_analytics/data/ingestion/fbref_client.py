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
        self.last_request_time = 0
        self.rate_limit_seconds = 6  # FBref requires 6 seconds between requests
        
        # Headers for API requests
        self.headers = {
            "X-API-Key": api_key if api_key else None,
            "Content-Type": "application/json",
            "User-Agent": "NWSL-Analytics/1.0"
        }
        
        # Remove X-API-Key header if no API key provided
        if not api_key:
            self.headers.pop("X-API-Key", None)
        
        # NWSL league configuration (will be populated after finding league_id)
        self.nwsl_league_id = None
        self.nwsl_country_id = None
    
    def _enforce_rate_limit(self):
        """Enforce FBref's 6 second rate limit between requests"""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        
        if time_since_last_request < self.rate_limit_seconds:
            wait_time = self.rate_limit_seconds - time_since_last_request
            logger.info(f"⏳ Rate limit: waiting {wait_time:.1f} seconds...")
            time.sleep(wait_time)
        
        self.last_request_time = time.time()
    
    def find_nwsl_league_id(self) -> Optional[str]:
        """Find NWSL league ID using the FBR API"""
        try:
            # First, get countries to find USA
            countries_url = f"{self.base_url}/countries"
            logger.info(f"Fetching countries from: {countries_url}")
            
            self._enforce_rate_limit()
            response = requests.get(countries_url, headers=self.headers)
            response.raise_for_status()
            
            response_data = response.json()
            countries = response_data.get("data", [])
            
            # Find USA
            usa_country_code = None
            for country in countries:
                if country.get("country", "").lower() in ["usa", "united states", "united states of america"]:
                    usa_country_code = country.get("country_code")
                    self.nwsl_country_id = usa_country_code
                    logger.info(f"Found USA country code: {usa_country_code}")
                    break
            
            if not usa_country_code:
                logger.error("Could not find USA country code")
                return None
            
            # Now get leagues for USA
            leagues_url = f"{self.base_url}/leagues"
            logger.info(f"Fetching leagues from: {leagues_url}")
            
            self._enforce_rate_limit()
            response = requests.get(leagues_url, headers=self.headers, params={"country_code": usa_country_code})
            response.raise_for_status()
            
            response_data = response.json()
            league_data = response_data.get("data", [])
            
            # Find NWSL in domestic leagues
            for league_type_data in league_data:
                if league_type_data.get("league_type") == "domestic_leagues":
                    leagues = league_type_data.get("leagues", [])
                    for league in leagues:
                        league_name = league.get("competition_name", "").lower()
                        if "nwsl" in league_name or "national women's soccer league" in league_name:
                            self.nwsl_league_id = league.get("league_id")
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
            url = f"{self.base_url}/league-seasons"
            params = {"league_id": self.nwsl_league_id}
            
            self._enforce_rate_limit()
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            
            response_data = response.json()
            seasons = response_data.get("data", [])
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
            url = f"{self.base_url}/team-season-stats"
            params = {
                "league_id": self.nwsl_league_id,
                "season_id": season_id
            }
            
            self._enforce_rate_limit()
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            
            response_data = response.json()
            data = response_data.get("data", [])
            
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
            url = f"{self.base_url}/player-season-stats"
            params = {
                "league_id": self.nwsl_league_id,
                "season_id": season_id
            }
            
            self._enforce_rate_limit()
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            
            response_data = response.json()
            data = response_data.get("data", [])
            
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
            
            self._enforce_rate_limit()
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            
            response_data = response.json()
            data = response_data.get("data", [])
            
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
    
    def get_all_players_match_stats(self, season_id: str) -> pd.DataFrame:
        """Get detailed player match statistics for all players"""
        if not self.nwsl_league_id:
            self.find_nwsl_league_id()
        
        try:
            url = f"{self.base_url}/all-players-match-stats"
            params = {
                "league_id": self.nwsl_league_id,
                "season_id": season_id
            }
            
            self._enforce_rate_limit()
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            
            response_data = response.json()
            data = response_data.get("data", [])
            
            if not data:
                logger.warning(f"No all-players match stats for season {season_id}")
                return pd.DataFrame()
            
            df = pd.DataFrame(data)
            df['season_id'] = season_id
            df['ingestion_date'] = pd.Timestamp.now()
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching all-players match stats: {e}")
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
            
            # Get all players match stats
            all_player_match_stats = self.get_all_players_match_stats(season_id)
            if not all_player_match_stats.empty:
                rows = self._upload_to_bigquery(all_player_match_stats, f'nwsl_all_players_match_stats_{season_id}')
                if rows > 0:
                    results['total_rows'] += rows
                    results['tables_created'] += 1
                    logger.info(f"✅ All players match stats: {rows} rows")
            
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
            self._enforce_rate_limit()
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
