#!/usr/bin/env python3
"""
Test all FBref endpoints to see what works
"""

import os
import requests
import json

def test_endpoints():
    api_key = os.getenv('FBREF_API_KEY', 'KvcVSKb_a49kmsKc6nnFAPfyaLPwLqiKm4VBxA2fvmY')
    nwsl_league_id = "182"
    
    headers = {
        "X-API-Key": api_key,
        "Content-Type": "application/json",
        "User-Agent": "NWSL-Analytics/1.0"
    }
    
    endpoints_to_test = [
        ("team-season-stats", "2024"),
        ("team-season-stats", "2025"),
        ("player-season-stats", "2024"),
        ("player-season-stats", "2025"),
        ("matches", "2024"),
        ("matches", "2025"),
        ("all-players-match-stats", "2024"),
        ("all-players-match-stats", "2025"),
    ]
    
    for endpoint, season in endpoints_to_test:
        print(f"\n🔍 Testing {endpoint} for {season}...")
        
        url = f"https://fbrapi.com/{endpoint}"
        params = {
            "league_id": nwsl_league_id,
            "season_id": season
        }
        
        try:
            response = requests.get(url, headers=headers, params=params)
            
            if response.status_code == 200:
                data = response.json().get("data", [])
                print(f"   ✅ Success: {len(data)} records")
                
                if data and endpoint == "team-season-stats":
                    # Show team stats fields
                    sample = data[0]
                    print(f"   📊 Sample fields: {', '.join(list(sample.keys())[:5])}...")
                
                elif data and "player" in endpoint:
                    # Show player fields  
                    sample = data[0]
                    player_name = sample.get('player_name', sample.get('name', 'Unknown'))
                    print(f"   👤 Sample player: {player_name}")
                    print(f"   📊 Fields: {len(sample.keys())} total")
                    
            else:
                print(f"   ❌ Failed: {response.status_code}")
                if response.status_code != 404:
                    print(f"   Error: {response.text[:100]}...")
                
        except Exception as e:
            print(f"   💥 Exception: {e}")

if __name__ == "__main__":
    test_endpoints()