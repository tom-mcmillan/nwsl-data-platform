#!/usr/bin/env python3
"""
Comprehensive FBref API integration for NWSL player statistics
"""

import requests
import time
import json
import pandas as pd
from pathlib import Path

class FBrefAPI:
    def __init__(self):
        self.base_url = "https://fbrapi.com"
        self.api_key = None
        self.rate_limit_delay = 3  # 3 seconds between requests
        
    def generate_api_key(self):
        """Generate a new API key"""
        print("🔑 Generating FBref API key...")
        
        try:
            response = requests.post(f"{self.base_url}/generate_api_key")
            response.raise_for_status()
            
            data = response.json()
            self.api_key = data['api_key']
            
            print(f"✅ API key generated: {self.api_key}")
            return self.api_key
            
        except Exception as e:
            print(f"❌ Failed to generate API key: {e}")
            return None
    
    def _make_request(self, endpoint, params=None):
        """Make rate-limited API request"""
        if not self.api_key:
            raise ValueError("API key not set. Call generate_api_key() first.")
        
        headers = {"X-API-Key": self.api_key}
        url = f"{self.base_url}/{endpoint}"
        
        print(f"📡 Making request to: {endpoint}")
        
        try:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            
            # Rate limiting
            time.sleep(self.rate_limit_delay)
            
            return response.json()
            
        except Exception as e:
            print(f"❌ Request failed for {endpoint}: {e}")
            return None
    
    def get_countries(self):
        """Get all available countries"""
        return self._make_request("countries")
    
    def get_leagues(self, country_code):
        """Get leagues for a specific country"""
        return self._make_request("leagues", {"country_code": country_code})
    
    def get_league_seasons(self, league_id):
        """Get all seasons for a league"""
        return self._make_request("league-seasons", {"league_id": league_id})
    
    def get_teams(self, team_id, season_id=None):
        """Get team roster and schedule"""
        params = {"team_id": team_id}
        if season_id:
            params["season_id"] = season_id
        return self._make_request("teams", params)
    
    def get_player_season_stats(self, team_id, league_id, season_id):
        """Get comprehensive player season statistics"""
        params = {
            "team_id": team_id,
            "league_id": league_id,
            "season_id": season_id
        }
        return self._make_request("player-season-stats", params)

def find_nwsl_league():
    """Find NWSL league information"""
    api = FBrefAPI()
    
    # Generate API key
    if not api.generate_api_key():
        return None
    
    print("🔍 Searching for NWSL league...")
    
    # Get US leagues
    countries = api.get_countries()
    if not countries:
        return None
    
    # Find USA
    usa_code = None
    for country in countries.get('data', []):
        if country.get('country') == 'United States':
            usa_code = country.get('country_code')
            break
    
    if not usa_code:
        print("❌ USA not found in countries")
        return None
    
    print(f"✅ Found USA country code: {usa_code}")
    
    # Get US leagues
    leagues = api.get_leagues(usa_code)
    if not leagues:
        return None
    
    # Look for NWSL
    nwsl_league = None
    for league_type in leagues.get('data', []):
        for league in league_type.get('leagues', []):
            if 'nwsl' in league.get('competition_name', '').lower():
                nwsl_league = league
                break
        if nwsl_league:
            break
    
    if nwsl_league:
        print(f"✅ Found NWSL: {nwsl_league}")
        return api, nwsl_league
    else:
        print("❌ NWSL league not found")
        return None

def get_all_nwsl_player_data():
    """Get comprehensive NWSL player data from FBref API"""
    print("🚀 Starting comprehensive NWSL player data collection")
    print("=" * 60)
    
    # Find NWSL league
    result = find_nwsl_league()
    if not result:
        return None
    
    api, nwsl_league = result
    league_id = nwsl_league['league_id']
    
    print(f"📊 Using league ID: {league_id}")
    
    # Get available seasons
    seasons = api.get_league_seasons(league_id)
    if not seasons:
        print("❌ Could not get NWSL seasons")
        return None
    
    print(f"✅ Found {len(seasons.get('data', []))} seasons")
    
    # Get latest season
    latest_season = seasons['data'][0]['season_id'] if seasons['data'] else None
    if not latest_season:
        print("❌ No seasons available")
        return None
    
    print(f"📅 Using latest season: {latest_season}")
    
    # We need to get teams first, but the API documentation shows we need team_ids
    # Let's try to get league standings to find team IDs
    print("🔍 Need to find team IDs - this requires additional API exploration")
    
    return {
        'api': api,
        'league_id': league_id,
        'season_id': latest_season,
        'league_info': nwsl_league
    }

if __name__ == "__main__":
    result = get_all_nwsl_player_data()
    if result:
        print(f"\n🎉 Successfully initialized FBref API for NWSL")
        print(f"League ID: {result['league_id']}")
        print(f"Season: {result['season_id']}")
        print(f"League: {result['league_info']['competition_name']}")
    else:
        print("\n❌ Failed to initialize FBref API")