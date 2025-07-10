#!/usr/bin/env python3
"""
Check what methods are available in itscalledsoccer AmericanSoccerAnalysis class
"""

from itscalledsoccer.client import AmericanSoccerAnalysis
import pandas as pd

def check_available_methods():
    """Check what methods are available in ASA client"""
    asa = AmericanSoccerAnalysis()
    
    # Get all methods that don't start with underscore
    methods = [method for method in dir(asa) if not method.startswith('_') and callable(getattr(asa, method))]
    
    print("🔍 Available methods in AmericanSoccerAnalysis:")
    for i, method in enumerate(methods, 1):
        print(f"   {i:2d}. {method}")
    
    # Test the methods we found
    print(f"\n📊 Testing key methods...")
    
    # Test players (we know this works)
    try:
        players = asa.get_players(leagues=['nwsl'])
        print(f"✅ get_players: {len(players)} records")
        print(f"   Columns: {list(players.columns)}")
    except Exception as e:
        print(f"❌ get_players failed: {e}")
    
    # Test games with correct parameters
    try:
        games = asa.get_games(leagues=['nwsl'], seasons=['2024'])  # Use string seasons
        print(f"✅ get_games: {len(games)} records")
        print(f"   Columns: {list(games.columns)}")
    except Exception as e:
        print(f"❌ get_games failed: {e}")
        
    # Test salaries
    try:
        salaries = asa.get_player_salaries(leagues=['nwsl'])
        print(f"✅ get_player_salaries: {len(salaries)} records")
        print(f"   Columns: {list(salaries.columns)}")
    except Exception as e:
        print(f"❌ get_player_salaries failed: {e}")
        
    # Test goals added
    try:
        goals_added = asa.get_player_goals_added(leagues=['nwsl'], seasons=['2024'])
        print(f"✅ get_player_goals_added: {len(goals_added)} records")
        print(f"   Columns: {list(goals_added.columns)}")
    except Exception as e:
        print(f"❌ get_player_goals_added failed: {e}")
        
    # Test goalkeeper stats
    try:
        gk_stats = asa.get_goalkeeper_stats(leagues=['nwsl'], seasons=['2024'])
        print(f"✅ get_goalkeeper_stats: {len(gk_stats)} records")
        print(f"   Columns: {list(gk_stats.columns)}")
    except Exception as e:
        print(f"❌ get_goalkeeper_stats failed: {e}")

if __name__ == "__main__":
    check_available_methods()