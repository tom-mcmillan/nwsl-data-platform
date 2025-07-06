#!/usr/bin/env python3
"""
Debug FBref API responses
"""

import os
import requests
import json

# API setup
api_key = os.getenv('FBREF_API_KEY', 'KvcVSKb_a49kmsKc6nnFAPfyaLPwLqiKm4VBxA2fvmY')
headers = {"X-API-Key": api_key}
base_url = "https://fbrapi.com"

print("🔍 Debugging FBref API responses...")

# Test countries
print("\n1. Testing /countries")
response = requests.get(f"{base_url}/countries", headers=headers)
print(f"Status: {response.status_code}")
if response.status_code == 200:
    data = response.json()
    print(f"Response keys: {list(data.keys())}")
    print(f"First few countries: {data.get('data', [])[:3]}")

# Test leagues for USA
print("\n2. Testing /leagues for USA")
response = requests.get(f"{base_url}/leagues", headers=headers, params={"country_code": "USA"})
print(f"Status: {response.status_code}")
if response.status_code == 200:
    data = response.json()
    print(f"Response keys: {list(data.keys())}")
    for league_type in data.get('data', []):
        print(f"League type: {league_type.get('league_type')}")
        for league in league_type.get('leagues', [])[:2]:
            print(f"  - {league.get('competition_name')} (ID: {league.get('league_id')}, Gender: {league.get('gender')})")

# Test league seasons for NWSL (ID 182)
print("\n3. Testing /league-seasons for NWSL")
response = requests.get(f"{base_url}/league-seasons", headers=headers, params={"league_id": "182"})
print(f"Status: {response.status_code}")
if response.status_code == 200:
    data = response.json()
    print(f"Response keys: {list(data.keys())}")
    seasons = data.get('data', [])
    print(f"Number of seasons: {len(seasons)}")
    print("Season structure:")
    for season in seasons[:3]:
        print(f"  Season: {json.dumps(season, indent=2)}")