#!/usr/bin/env python3
"""
Analyze NWSL player Excel file to understand structure and data availability
"""

import pandas as pd
import numpy as np

def analyze_excel_file():
    file_path = "data/raw/excel/Player Standard Stats 2025 NWSL_rev.xlsx"
    
    try:
        # Read Excel file
        print("📊 Reading restructured NWSL Player Stats Excel file...")
        df = pd.read_excel(file_path)
        
        print(f"✅ Successfully loaded {len(df)} rows and {len(df.columns)} columns")
        
        # Show basic info
        print(f"\n📋 Dataset Info:")
        print(f"   Rows: {len(df):,}")
        print(f"   Columns: {len(df.columns)}")
        
        # Show column names
        print(f"\n📝 Column Names:")
        for i, col in enumerate(df.columns, 1):
            print(f"   {i:2d}. {col}")
        
        # Show data types
        print(f"\n🔍 Data Types:")
        for col in df.columns:
            dtype = df[col].dtype
            non_null = df[col].count()
            print(f"   {col[:30]:30s} | {str(dtype):15s} | {non_null:4d} non-null")
        
        # Show sample data (first 3 rows)
        print(f"\n👀 Sample Data (first 3 rows):")
        print(df.head(3).to_string())
        
        # Check for key soccer metrics
        key_metrics = ['goals', 'assists', 'minutes', 'xg', 'xa', 'shots', 'passes', 'tackles']
        found_metrics = []
        
        print(f"\n🎯 Looking for key soccer metrics:")
        for metric in key_metrics:
            matches = [col for col in df.columns if metric.lower() in col.lower()]
            if matches:
                found_metrics.extend(matches)
                print(f"   ✅ {metric}: {matches}")
            else:
                print(f"   ❌ {metric}: Not found")
        
        # Show unique teams
        team_cols = [col for col in df.columns if 'team' in col.lower() or 'squad' in col.lower()]
        if team_cols:
            print(f"\n🏟️ Teams in dataset:")
            for team_col in team_cols[:1]:  # Just first team column
                unique_teams = df[team_col].dropna().unique()
                print(f"   {len(unique_teams)} teams: {list(unique_teams)}")
        
        # Show players with most goals/minutes
        if found_metrics:
            goal_cols = [col for col in found_metrics if 'goal' in col.lower()]
            minute_cols = [col for col in found_metrics if 'min' in col.lower()]
            
            if goal_cols:
                goal_col = goal_cols[0]
                print(f"\n⚽ Top 5 Goal Scorers ({goal_col}):")
                top_scorers = df.nlargest(5, goal_col)[['Player', goal_col]]
                print(top_scorers.to_string(index=False))
            
            if minute_cols:
                minute_col = minute_cols[0]
                print(f"\n⏱️ Top 5 by Minutes ({minute_col}):")
                top_minutes = df.nlargest(5, minute_col)[['Player', minute_col]]
                print(top_minutes.to_string(index=False))
        
        # Check data coverage
        print(f"\n📈 Data Coverage:")
        player_col = 'Player' if 'Player' in df.columns else df.columns[0]
        total_players = df[player_col].count()
        print(f"   Total players with data: {total_players}")
        
        # Summary assessment
        print(f"\n" + "="*60)
        print(f"🎯 ASSESSMENT FOR ANALYTICS")
        print(f"="*60)
        
        # Check if we have the metrics needed for your analytics tools
        required_for_analytics = {
            'Expected Goals Calculator': ['xg', 'goal'],
            'Shot Quality Profiler': ['shot', 'goal', 'target'],
            'Replacement Value Estimator': ['goal', 'assist', 'minute'],
            'Defensive Impact Tracker': ['tackle', 'intercept', 'block'],
            'Player Performance Analysis': ['xg', 'xa', 'goal', 'assist']
        }
        
        for tool, requirements in required_for_analytics.items():
            available = []
            missing = []
            
            for req in requirements:
                found = any(req.lower() in col.lower() for col in df.columns)
                if found:
                    available.append(req)
                else:
                    missing.append(req)
            
            if len(available) >= len(requirements) * 0.7:  # 70% of requirements met
                status = "✅ FEASIBLE"
            else:
                status = "❌ NEEDS MORE DATA"
            
            print(f"   {status} {tool}")
            if missing:
                print(f"      Missing: {missing}")
        
        print(f"\n💡 This data looks {'EXCELLENT' if len(found_metrics) > 5 else 'GOOD'} for advanced soccer analytics!")
        
        return df
        
    except Exception as e:
        print(f"❌ Error reading Excel file: {e}")
        return None

if __name__ == "__main__":
    df = analyze_excel_file()