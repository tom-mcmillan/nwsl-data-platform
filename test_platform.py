#!/usr/bin/env python3
"""
Test NWSL Data Platform
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from google.cloud import bigquery
from nwsl_analytics.config.settings import settings

print("🔍 Testing NWSL Data Platform")
print(f"📊 Project: {settings.gcp_project_id}")
print(f"💾 Dataset: {settings.bigquery_dataset_id}")

client = bigquery.Client(project=settings.gcp_project_id)

# Test 1: Team Stats with xG
print("\n1️⃣ Top teams by xG in 2024:")
query = """
SELECT 
  meta_data.team_name,
  stats.stats.ttl_gls as goals,
  ROUND(stats.stats.ttl_xg, 2) as xG,
  ROUND(stats.stats.ttl_gls - stats.stats.ttl_xg, 2) as xG_diff,
  stats.possession.avg_poss as possession
FROM `nwsl-data.nwsl_fbref.nwsl_team_season_stats_2024` 
ORDER BY stats.stats.ttl_xg DESC 
LIMIT 5
"""
results = client.query(query).to_dataframe()
print(results.to_string(index=False))

# Test 2: Match Data
print("\n2️⃣ Recent matches:")
query = """
SELECT 
  date,
  home,
  away,
  attendance,
  venue
FROM `nwsl-data.nwsl_fbref.nwsl_matches_2024` 
WHERE date >= '2024-10-01'
ORDER BY date DESC
LIMIT 5
"""
results = client.query(query).to_dataframe()
print(results.to_string(index=False))

# Test 3: Check available tables
print("\n3️⃣ Available tables:")
tables = client.list_tables(f"{settings.gcp_project_id}.{settings.bigquery_dataset_id}")
for table in tables:
    print(f"  - {table.table_id}")

print("\n✅ Platform test complete!")