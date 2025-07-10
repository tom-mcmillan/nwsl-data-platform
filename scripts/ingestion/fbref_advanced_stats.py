#!/usr/bin/env python3
"""
FBref API integration for NWSL advanced player statistics
Gets all advanced stat categories: passing, passing_types, defense, possession, misc, keeper
"""

import requests
import time
import pandas as pd
from datetime import datetime

class FBrefAdvancedStats:
    def __init__(self):
        self.base_url = "https://fbrapi.com"
        self.api_key = None
        self.rate_limit_delay = 4  # 4 seconds between requests
        
        # Advanced stat categories from the documentation
        self.advanced_categories = [
            'passing',
            'passing_types', 
            'defense',
            'possession',
            'misc',
            'keeper'
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
        """Find NWSL league ID"""
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
        
        # Find NWSL leagues
        nwsl_leagues = []
        print("📋 US leagues:")
        for league_type in leagues.get('data', []):
            for league in league_type.get('leagues', []):
                comp_name = league.get('competition_name', '')
                league_id = league.get('league_id')
                print(f"  - {comp_name} (ID: {league_id})")
                
                if 'nwsl' in comp_name.lower():
                    nwsl_leagues.append(league)
        
        if not nwsl_leagues:
            print("❌ No NWSL leagues found")
            return None
        
        # Try the main NWSL league first (ID: 182)
        main_nwsl = None
        for league in nwsl_leagues:
            if league.get('league_id') == 182:  # National Women's Soccer League
                main_nwsl = league
                break
        
        if main_nwsl:
            league_id = main_nwsl['league_id']
            print(f"🔍 Testing main NWSL: {main_nwsl['competition_name']} (ID: {league_id})")
            
            # Test seasons
            seasons = self._make_request("league-seasons", {"league_id": league_id})
            if seasons and seasons.get('data'):
                latest_season = seasons['data'][0]['season_id']
                print(f"✅ Found working NWSL league with season: {latest_season}")
                
                return {
                    'league_id': league_id,
                    'season_id': latest_season,
                    'league_name': main_nwsl['competition_name']
                }
        
        # Fallback: try other NWSL leagues
        for league in nwsl_leagues:
            if league.get('league_id') == 182:  # Skip main NWSL as we already tried it
                continue
                
            league_id = league['league_id']
            print(f"🔍 Testing league: {league['competition_name']} (ID: {league_id})")
            
            # Test if we can get seasons
            seasons = self._make_request("league-seasons", {"league_id": league_id})
            if seasons and seasons.get('data'):
                latest_season = seasons['data'][0]['season_id']
                print(f"✅ Found working NWSL league with season: {latest_season}")
                
                return {
                    'league_id': league_id,
                    'season_id': latest_season,
                    'league_name': league['competition_name']
                }
        
        print("❌ No working NWSL leagues found")
        return None
    
    def get_teams_from_standings(self, league_id, season_id):
        """Get team IDs from league standings"""
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
    
    def get_team_player_season_stats(self, team_id, league_id, season_id):
        """Get all player season stats for a team (this includes advanced categories)"""
        print(f"📊 Getting player season stats for team {team_id}...")
        
        stats = self._make_request("player-season-stats", {
            "team_id": team_id,
            "league_id": league_id,
            "season_id": season_id
        })
        
        if not stats or 'players' not in stats:
            print(f"❌ No player stats found for team {team_id}")
            return []
        
        return stats['players']
    
    def collect_all_advanced_stats(self):
        """Collect all NWSL advanced player statistics"""
        print("🚀 Starting NWSL advanced stats collection")
        print("=" * 60)
        
        # Generate API key
        if not self.generate_api_key():
            return False
        
        # Find NWSL league
        nwsl_info = self.find_nwsl_league()
        if not nwsl_info:
            return False
        
        league_id = nwsl_info['league_id']
        season_id = nwsl_info['season_id']
        league_name = nwsl_info['league_name']
        
        print(f"🏈 Using: {league_name} (ID: {league_id}) - Season: {season_id}")
        
        # Get teams
        teams = self.get_teams_from_standings(league_id, season_id)
        if not teams:
            return False
        
        # Initialize data storage
        category_data = {category: [] for category in self.advanced_categories}
        category_data['basic_stats'] = []  # Also store basic stats
        
        total_players = 0
        
        # Process each team
        for i, team in enumerate(teams, 1):
            team_id = team['team_id']
            team_name = team['team_name']
            
            print(f"\n🏈 Team {i}/{len(teams)}: {team_name}")
            print("-" * 40)
            
            # Get all player stats for this team
            players = self.get_team_player_season_stats(team_id, league_id, season_id)
            
            if not players:
                continue
            
            total_players += len(players)
            print(f"✅ Found {len(players)} players")
            
            # Process each player
            for player in players:
                meta_data = player.get('meta_data', {})
                stats = player.get('stats', {})
                
                # Base player info
                base_info = {
                    'player_id': meta_data.get('player_id'),
                    'player_name': meta_data.get('player_name'),
                    'team_name': team_name,
                    'team_id': team_id,
                    'season': season_id,
                    'league_name': league_name,
                    'position': stats.get('stats', {}).get('positions'),
                    'age': meta_data.get('age'),
                    'ingestion_date': datetime.now().isoformat()
                }
                
                # Extract each advanced category
                for category in self.advanced_categories:
                    if category in stats:
                        category_stats = stats[category].copy()
                        category_stats.update(base_info)
                        category_data[category].append(category_stats)
                
                # Also save basic stats
                if 'stats' in stats:
                    basic_stats = stats['stats'].copy()
                    basic_stats.update(base_info)
                    category_data['basic_stats'].append(basic_stats)
        
        # Save and upload data
        print(f"\n📊 Collection complete: {total_players} total players")
        print("💾 Saving and uploading data...")
        
        success_count = 0
        for category, data in category_data.items():
            if data:
                df = pd.DataFrame(data)
                
                # Save CSV
                csv_path = f"data/processed/player_{category}_{season_id}.csv"
                df.to_csv(csv_path, index=False)
                print(f"✅ {category}: {len(df)} records saved to {csv_path}")
                
                # Upload to BigQuery
                if self.upload_to_bigquery(df, category, season_id):
                    success_count += 1
            else:
                print(f"⚠️ No data for: {category}")
        
        print(f"\n🎉 Successfully uploaded {success_count} categories to BigQuery!")
        return True
    
    def upload_to_bigquery(self, df, category, season_id):
        """Upload category data to BigQuery"""
        project_id = "nwsl-data"
        table_id = f"nwsl_fbref.player_{category}_{season_id}"
        
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
    collector = FBrefAdvancedStats()
    
    success = collector.collect_all_advanced_stats()
    
    if success:
        print("\n🎉 Advanced stats collection completed!")
        print("📊 New BigQuery tables available:")
        for category in collector.advanced_categories + ['basic_stats']:
            print(f"   - nwsl_fbref.player_{category}_SEASON")
        print("\nThese tables contain detailed advanced statistics for comprehensive analytics!")
    else:
        print("\n❌ Collection failed")

if __name__ == "__main__":
    main()