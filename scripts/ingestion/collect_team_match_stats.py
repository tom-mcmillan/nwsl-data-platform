#!/usr/bin/env python3
"""
Collect comprehensive NWSL team match statistics via FBref API
Gets detailed match-by-match team performance data including:
- schedule, keeper, shooting, passing, passing_types, gca, defense, possession, misc
"""

import requests
import time
import pandas as pd
from pathlib import Path
from datetime import datetime
import json

class NWSLTeamMatchStats:
    def __init__(self):
        self.base_url = "https://fbrapi.com"
        self.api_key = None
        self.rate_limit_delay = 4  # 4 seconds between requests
        
        # Match stat categories from the API documentation
        self.match_stat_categories = [
            'schedule',
            'keeper', 
            'shooting',
            'passing',
            'passing_types',
            'gca',
            'defense',
            'possession',
            'misc'
        ]
        
    def generate_api_key(self):
        """Generate a new API key"""
        print("🔑 Generating FBref API key...")
        
        try:
            response = requests.post(f"{self.base_url}/generate_api_key")
            response.raise_for_status()
            
            data = response.json()
            self.api_key = data['api_key']
            
            print(f"✅ API key generated")
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
        
        print(f"📡 Request: {endpoint}")
        
        try:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            
            # Rate limiting
            time.sleep(self.rate_limit_delay)
            
            return response.json()
            
        except requests.exceptions.HTTPError as e:
            if response.status_code == 500:
                print(f"⚠️ Server error for {endpoint}: {e}")
                return None
            else:
                print(f"❌ HTTP error for {endpoint}: {e}")
                return None
        except Exception as e:
            print(f"❌ Request failed for {endpoint}: {e}")
            return None
    
    def find_nwsl_league(self):
        """Find NWSL league information"""
        print("🔍 Finding NWSL league...")
        
        # Get countries
        countries = self._make_request("countries")
        if not countries:
            return None
        
        # Find USA
        usa_code = None
        for country in countries.get('data', []):
            if country.get('country') == 'United States':
                usa_code = country.get('country_code')
                break
        
        if not usa_code:
            print("❌ USA not found")
            return None
        
        print(f"✅ Found USA: {usa_code}")
        
        # Get US leagues
        leagues = self._make_request("leagues", {"country_code": usa_code})
        if not leagues:
            return None
        
        # Find NWSL leagues and test them
        nwsl_leagues = []
        for league_type in leagues.get('data', []):
            for league in league_type.get('leagues', []):
                comp_name = league.get('competition_name', '')
                if 'nwsl' in comp_name.lower():
                    nwsl_leagues.append(league)
        
        # Try each NWSL league to find one that works
        for league in nwsl_leagues:
            league_id = league['league_id']
            print(f"🔍 Testing league: {league['competition_name']} (ID: {league_id})")
            
            # Test if we can get seasons
            seasons = self._make_request("league-seasons", {"league_id": league_id})
            if seasons and seasons.get('data'):
                # Try to get a recent season
                recent_seasons = [s for s in seasons['data'] if '2024' in s['season_id'] or '2023' in s['season_id']]
                if recent_seasons:
                    season_id = recent_seasons[0]['season_id']
                    print(f"✅ Found working NWSL league with season: {season_id}")
                    
                    return {
                        'league_id': league_id,
                        'season_id': season_id,
                        'league_name': league['competition_name'],
                        'all_seasons': [s['season_id'] for s in seasons['data']]
                    }
        
        print("❌ No working NWSL leagues found")
        return None
    
    def get_teams_from_standings(self, league_id, season_id):
        """Get team information from league standings"""
        print(f"🏆 Getting teams from standings...")
        
        standings = self._make_request("league-standings", {
            "league_id": league_id,
            "season_id": season_id
        })
        
        if not standings or not standings.get('data'):
            print("❌ No standings found")
            return []
        
        teams = []
        for standings_table in standings['data']:
            for team in standings_table.get('standings', []):
                if 'team_id' in team and 'team_name' in team:
                    teams.append({
                        'team_id': team['team_id'],
                        'team_name': team['team_name']
                    })
        
        print(f"✅ Found {len(teams)} teams")
        return teams
    
    def get_team_match_stats(self, team_id, league_id, season_id):
        """Get all match statistics for a team in a season"""
        print(f"📊 Getting team match stats for team {team_id} in {season_id}...")
        
        stats = self._make_request("team-match-stats", {
            "team_id": team_id,
            "league_id": league_id,
            "season_id": season_id
        })
        
        if not stats or 'data' not in stats:
            print(f"❌ No team match stats found for team {team_id}")
            return []
        
        return stats['data']
    
    def process_team_match_data(self, match_data, team_info, league_info, season_id):
        """Process raw team match data into structured format"""
        processed_matches = []
        
        for match in match_data:
            meta_data = match.get('meta_data', {})
            stats = match.get('stats', {})
            
            # Base match information
            base_info = {
                'team_id': team_info['team_id'],
                'team_name': team_info['team_name'],
                'league_id': league_info['league_id'],
                'league_name': league_info['league_name'],
                'season': season_id,
                'match_id': meta_data.get('match_id'),
                'date': meta_data.get('date'),
                'round': meta_data.get('round'),
                'home_away': meta_data.get('home_away'),
                'opponent': meta_data.get('opponent'),
                'opponent_id': meta_data.get('opponent_id'),
                'ingestion_date': datetime.now().isoformat()
            }
            
            # Create separate records for each statistical category
            for category in self.match_stat_categories:
                if category in stats:
                    category_stats = stats[category].copy()
                    category_stats.update(base_info)
                    
                    processed_matches.append({
                        'category': category,
                        'data': category_stats
                    })
        
        return processed_matches
    
    def collect_all_team_match_stats(self):
        """Collect all NWSL team match statistics"""
        print("🚀 Starting NWSL Team Match Statistics Collection")
        print("=" * 70)
        
        # Generate API key
        if not self.generate_api_key():
            return False
        
        # Find NWSL league
        nwsl_info = self.find_nwsl_league()
        if not nwsl_info:
            return False
        
        league_id = nwsl_info['league_id']
        league_name = nwsl_info['league_name']
        available_seasons = nwsl_info['all_seasons']
        
        print(f"🏈 League: {league_name} (ID: {league_id})")
        print(f"📅 Available seasons: {available_seasons}")
        
        # Process multiple seasons if available
        seasons_to_process = [s for s in available_seasons if any(year in s for year in ['2023', '2024', '2025'])][:2]  # Last 2 seasons
        
        if not seasons_to_process:
            seasons_to_process = [available_seasons[0]]  # Use most recent
        
        print(f"🎯 Processing seasons: {seasons_to_process}")
        
        # Initialize data storage
        category_data = {category: [] for category in self.match_stat_categories}
        
        total_matches = 0
        
        # Process each season
        for season_id in seasons_to_process:
            print(f"\n📅 Processing Season: {season_id}")
            print("-" * 50)
            
            # Get teams for this season
            teams = self.get_teams_from_standings(league_id, season_id)
            if not teams:
                print(f"⚠️ No teams found for season {season_id}")
                continue
            
            # Process each team
            for i, team in enumerate(teams, 1):
                team_id = team['team_id']
                team_name = team['team_name']
                
                print(f"\n🏈 Team {i}/{len(teams)}: {team_name}")
                
                # Get team match stats
                team_matches = self.get_team_match_stats(team_id, league_id, season_id)
                
                if not team_matches:
                    continue
                
                total_matches += len(team_matches)
                print(f"✅ Found {len(team_matches)} matches")
                
                # Process match data
                processed_matches = self.process_team_match_data(
                    team_matches, team, nwsl_info, season_id
                )
                
                # Organize by category
                for match_record in processed_matches:
                    category = match_record['category']
                    data = match_record['data']
                    category_data[category].append(data)
        
        # Save and upload data
        print(f"\n📊 Collection complete: {total_matches} total team matches")
        print("💾 Saving and uploading data...")
        
        success_count = 0
        for category, data in category_data.items():
            if data:
                df = pd.DataFrame(data)
                
                # Save CSV
                csv_path = f"data/processed/team_match_{category}.csv"
                df.to_csv(csv_path, index=False)
                print(f"✅ {category}: {len(df)} match records saved to {csv_path}")
                
                # Upload to BigQuery
                if self.upload_to_bigquery(df, category):
                    success_count += 1
            else:
                print(f"⚠️ No data for: {category}")
        
        print(f"\n🎉 Successfully uploaded {success_count} team match stat categories to BigQuery!")
        return True
    
    def upload_to_bigquery(self, df, category):
        """Upload category data to BigQuery"""
        project_id = "nwsl-data"
        table_id = f"nwsl_fbref.team_match_{category}"
        
        print(f"📤 Uploading {category} to {table_id}")
        
        try:
            df.to_gbq(
                destination_table=table_id,
                project_id=project_id,
                if_exists='replace',
                chunksize=1000,
                progress_bar=False
            )
            
            print(f"✅ Upload successful: {len(df)} records")
            return True
            
        except Exception as e:
            print(f"❌ Upload failed for {category}: {e}")
            return False

def main():
    """Main function"""
    collector = NWSLTeamMatchStats()
    
    success = collector.collect_all_team_match_stats()
    
    if success:
        print("\n🎉 Team match statistics collection completed!")
        print("📊 New BigQuery tables available:")
        for category in collector.match_stat_categories:
            print(f"   - nwsl_fbref.team_match_{category}")
        print("\nThese tables contain detailed match-by-match team performance for tactical analysis!")
    else:
        print("\n❌ Collection failed")

if __name__ == "__main__":
    main()