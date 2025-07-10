#!/usr/bin/env python3
"""
Collect comprehensive NWSL match-level player statistics via FBref API
Uses the All Players Match Stats endpoint to get detailed player performance for both teams in each match
This provides maximum data coverage as requested by the user
"""

import requests
import time
import pandas as pd
from pathlib import Path
from datetime import datetime
import json

class NWSLMatchLevelStats:
    def __init__(self):
        self.base_url = "https://fbrapi.com"
        self.api_key = None
        self.rate_limit_delay = 6  # 6 seconds between requests (FBref requirement)
        
        # Match stat categories for all-players-match-stats endpoint
        self.match_stat_categories = [
            'stats',
            'passing',
            'passing_types',
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
                # Try to get recent seasons
                recent_seasons = [s for s in seasons['data'] if any(year in s['season_id'] for year in ['2024', '2025', '2023'])]
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
    
    def get_match_ids_from_fixtures(self, league_id, season_id):
        """Get match IDs from league fixtures"""
        print(f"🏆 Getting match IDs from fixtures for {season_id}...")
        
        fixtures = self._make_request("league-fixtures", {
            "league_id": league_id,
            "season_id": season_id
        })
        
        if not fixtures or not fixtures.get('data'):
            print("❌ No fixtures found")
            return []
        
        match_ids = []
        for round_data in fixtures['data']:
            for match in round_data.get('matches', []):
                if 'match_id' in match:
                    match_ids.append({
                        'match_id': match['match_id'],
                        'date': match.get('date'),
                        'round': match.get('round'),
                        'home_team': match.get('home_team'),
                        'away_team': match.get('away_team'),
                        'status': match.get('status')
                    })
        
        print(f"✅ Found {len(match_ids)} matches")
        return match_ids
    
    def get_all_players_match_stats(self, match_id):
        """Get all players match stats for a specific match"""
        print(f"📊 Getting all players match stats for match {match_id}...")
        
        stats = self._make_request("all-players-match-stats", {
            "match_id": match_id
        })
        
        if not stats or 'data' not in stats:
            print(f"❌ No match stats found for match {match_id}")
            return None
        
        return stats['data']
    
    def process_match_player_data(self, match_data, match_info, league_info):
        """Process raw match player data into structured format"""
        processed_data = {
            'individual_stats': [],
            'team_aggregates': []
        }
        
        if not match_data or 'teams' not in match_data:
            return processed_data
        
        # Process each team
        for team_data in match_data['teams']:
            team_name = team_data.get('team_name')
            team_id = team_data.get('team_id')
            
            team_totals = {category: {} for category in self.match_stat_categories}
            player_count = 0
            
            # Process each player in the team
            for player in team_data.get('players', []):
                player_count += 1
                
                meta_data = player.get('meta_data', {})
                stats = player.get('stats', {})
                
                # Base player info
                base_info = {
                    'match_id': match_info['match_id'],
                    'date': match_info['date'],
                    'round': match_info['round'],
                    'home_team': match_info['home_team'],
                    'away_team': match_info['away_team'],
                    'player_id': meta_data.get('player_id'),
                    'player_name': meta_data.get('player_name'),
                    'team_name': team_name,
                    'team_id': team_id,
                    'league_id': league_info['league_id'],
                    'league_name': league_info['league_name'],
                    'season': league_info['season_id'],
                    'position': meta_data.get('position'),
                    'jersey_number': meta_data.get('jersey_number'),
                    'ingestion_date': datetime.now().isoformat()
                }
                
                # Process each statistical category
                for category in self.match_stat_categories:
                    if category in stats:
                        category_stats = stats[category].copy()
                        category_stats.update(base_info)
                        
                        processed_data['individual_stats'].append({
                            'category': category,
                            'data': category_stats
                        })
                        
                        # Aggregate for team totals
                        for stat_key, stat_value in stats[category].items():
                            if isinstance(stat_value, (int, float)):
                                if stat_key not in team_totals[category]:
                                    team_totals[category][stat_key] = 0
                                team_totals[category][stat_key] += stat_value
            
            # Create team aggregate records
            for category in self.match_stat_categories:
                if team_totals[category]:
                    team_stats = team_totals[category].copy()
                    team_stats.update({
                        'match_id': match_info['match_id'],
                        'date': match_info['date'],
                        'round': match_info['round'],
                        'home_team': match_info['home_team'],
                        'away_team': match_info['away_team'],
                        'team_name': team_name,
                        'team_id': team_id,
                        'league_id': league_info['league_id'],
                        'league_name': league_info['league_name'],
                        'season': league_info['season_id'],
                        'players_count': player_count,
                        'ingestion_date': datetime.now().isoformat()
                    })
                    
                    processed_data['team_aggregates'].append({
                        'category': category,
                        'data': team_stats
                    })
        
        return processed_data
    
    def collect_all_match_level_data(self):
        """Collect all NWSL match-level player and team statistics"""
        print("🚀 Starting NWSL Match-Level Data Collection")
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
        seasons_to_process = [s for s in available_seasons if any(year in s for year in ['2024', '2025', '2023'])][:2]  # Last 2 seasons
        
        if not seasons_to_process:
            seasons_to_process = [available_seasons[0]]  # Use most recent
        
        print(f"🎯 Processing seasons: {seasons_to_process}")
        
        # Initialize data storage
        individual_category_data = {category: [] for category in self.match_stat_categories}
        team_category_data = {category: [] for category in self.match_stat_categories}
        
        total_matches_processed = 0
        total_players_processed = 0
        
        # Process each season
        for season_id in seasons_to_process:
            print(f"\n📅 Processing Season: {season_id}")
            print("-" * 50)
            
            # Update league info for this season
            current_league_info = nwsl_info.copy()
            current_league_info['season_id'] = season_id
            
            # Get match IDs for this season
            match_ids = self.get_match_ids_from_fixtures(league_id, season_id)
            if not match_ids:
                print(f"⚠️ No matches found for season {season_id}")
                continue
            
            # Limit matches for testing (remove this in production)
            # match_ids = match_ids[:5]  # Process first 5 matches
            
            # Process each match
            for i, match_info in enumerate(match_ids, 1):
                match_id = match_info['match_id']
                
                print(f"\n🏆 Match {i}/{len(match_ids)}: {match_info['home_team']} vs {match_info['away_team']}")
                print(f"   Date: {match_info['date']}, Round: {match_info['round']}")
                
                # Get all players match stats
                match_data = self.get_all_players_match_stats(match_id)
                
                if not match_data:
                    continue
                
                total_matches_processed += 1
                
                # Process match data
                processed_data = self.process_match_player_data(
                    match_data, match_info, current_league_info
                )
                
                # Count players processed
                players_in_match = len(processed_data['individual_stats'])
                total_players_processed += players_in_match
                print(f"✅ Processed {players_in_match} player records")
                
                # Organize individual player data by category
                for record in processed_data['individual_stats']:
                    category = record['category']
                    data = record['data']
                    individual_category_data[category].append(data)
                
                # Organize team aggregate data by category
                for record in processed_data['team_aggregates']:
                    category = record['category']
                    data = record['data']
                    team_category_data[category].append(data)
        
        # Save and upload data
        print(f"\n📊 Collection complete:")
        print(f"   Matches processed: {total_matches_processed}")
        print(f"   Player records: {total_players_processed}")
        print("💾 Saving and uploading data...")
        
        success_count = 0
        
        # Upload individual player match data
        for category, data in individual_category_data.items():
            if data:
                df = pd.DataFrame(data)
                
                # Save CSV
                csv_path = f"data/processed/player_match_{category}.csv"
                df.to_csv(csv_path, index=False)
                print(f"✅ Individual {category}: {len(df)} records saved to {csv_path}")
                
                # Upload to BigQuery
                if self.upload_to_bigquery(df, f"player_match_{category}"):
                    success_count += 1
            else:
                print(f"⚠️ No individual data for: {category}")
        
        # Upload team aggregate match data
        for category, data in team_category_data.items():
            if data:
                df = pd.DataFrame(data)
                
                # Save CSV
                csv_path = f"data/processed/team_match_{category}.csv"
                df.to_csv(csv_path, index=False)
                print(f"✅ Team {category}: {len(df)} records saved to {csv_path}")
                
                # Upload to BigQuery
                if self.upload_to_bigquery(df, f"team_match_{category}"):
                    success_count += 1
            else:
                print(f"⚠️ No team data for: {category}")
        
        print(f"\n🎉 Successfully uploaded {success_count} match-level stat categories to BigQuery!")
        return True
    
    def upload_to_bigquery(self, df, table_suffix):
        """Upload category data to BigQuery"""
        project_id = "nwsl-data"
        table_id = f"nwsl_fbref.{table_suffix}"
        
        print(f"📤 Uploading to {table_id}")
        
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
            print(f"❌ Upload failed for {table_suffix}: {e}")
            return False

def main():
    """Main function"""
    collector = NWSLMatchLevelStats()
    
    success = collector.collect_all_match_level_data()
    
    if success:
        print("\n🎉 Match-level statistics collection completed!")
        print("📊 New BigQuery tables available:")
        print("   Individual Player Match Data:")
        for category in collector.match_stat_categories:
            print(f"   - nwsl_fbref.player_match_{category}")
        print("   Team Match Aggregates:")
        for category in collector.match_stat_categories:
            print(f"   - nwsl_fbref.team_match_{category}")
        print("\nThese tables provide maximum data coverage with match-level granularity for both individual and team analysis!")
    else:
        print("\n❌ Collection failed")

if __name__ == "__main__":
    main()