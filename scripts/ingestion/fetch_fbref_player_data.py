
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
        print(f"\n📊 Getting player data for {season_id}...")
        
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
