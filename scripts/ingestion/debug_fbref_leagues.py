#!/usr/bin/env python3
"""
Debug FBref Leagues
Check what leagues are available in the FBref API
"""

import os
import requests
import json

def debug_leagues():
    api_key = os.getenv('FBREF_API_KEY', 'KvcVSKb_a49kmsKc6nnFAPfyaLPwLqiKm4VBxA2fvmY')
    
    headers = {
        "X-API-Key": api_key,
        "Content-Type": "application/json",
        "User-Agent": "NWSL-Analytics/1.0"
    }
    
    # Get countries
    print("🌍 Getting countries...")
    response = requests.get("https://fbrapi.com/countries", headers=headers)
    countries = response.json().get("data", [])
    
    usa_codes = []
    for country in countries:
        country_name = country.get("country", "").lower()
        if "usa" in country_name or "united states" in country_name or "america" in country_name:
            usa_codes.append({
                "name": country.get("country"),
                "code": country.get("country_code")
            })
    
    print(f"🇺🇸 Found USA-related countries: {usa_codes}")
    
    # Try each USA code
    for usa_info in usa_codes:
        print(f"\n📊 Getting leagues for {usa_info['name']} ({usa_info['code']})...")
        
        response = requests.get("https://fbrapi.com/leagues", headers=headers, params={"country_code": usa_info['code']})
        league_data = response.json().get("data", [])
        
        print(f"Found {len(league_data)} league types")
        
        for league_type in league_data:
            type_name = league_type.get("league_type")
            leagues = league_type.get("leagues", [])
            print(f"\n  📂 {type_name}: {len(leagues)} leagues")
            
            for league in leagues:
                league_name = league.get("competition_name", "")
                league_id = league.get("league_id", "")
                print(f"    - {league_name} (ID: {league_id})")
                
                # Check if this could be NWSL
                if any(keyword in league_name.lower() for keyword in ["nwsl", "women", "soccer", "national"]):
                    print(f"      ⚽ POTENTIAL MATCH!")

if __name__ == "__main__":
    debug_leagues()