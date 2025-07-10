#!/usr/bin/env python3
"""
Process all historical NWSL player statistics Excel files and upload to BigQuery
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import re

def extract_year_from_filename(filename):
    """Extract year from Excel filename"""
    match = re.search(r'(\d{4})', filename)
    return int(match.group(1)) if match else None

def clean_column_names(df):
    """Clean column names for BigQuery compatibility"""
    df.columns = [col.replace('+', '_plus_').replace('-', '_minus_') for col in df.columns]
    return df

def process_excel_file(file_path, year):
    """Process a single Excel file"""
    print(f"📊 Processing {file_path.name} (Year: {year})")
    
    try:
        # Read Excel file
        df = pd.read_excel(file_path)
        
        # Clean column names
        df = clean_column_names(df)
        
        # Add metadata columns
        df['season'] = year
        df['data_source'] = 'fbref_excel'
        df['ingestion_date'] = datetime.now().isoformat()
        
        # Handle missing values
        df = df.fillna({
            'Nation': 'Unknown',
            'Age': '0-000',
            'Born': 0
        })
        
        # Convert numeric columns properly
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        print(f"✅ Processed {len(df)} players for {year}")
        return df
        
    except Exception as e:
        print(f"❌ Error processing {file_path.name}: {e}")
        return None

def upload_to_bigquery(df, year):
    """Upload year data to BigQuery"""
    project_id = "nwsl-data"
    table_id = f"nwsl_fbref.player_stats_{year}"
    
    print(f"📤 Uploading {year} data to BigQuery: {table_id}")
    
    try:
        df.to_gbq(
            destination_table=table_id,
            project_id=project_id,
            if_exists='replace',
            chunksize=1000,
            progress_bar=False
        )
        
        print(f"✅ Successfully uploaded {len(df)} players for {year}")
        return True
        
    except Exception as e:
        print(f"❌ Upload failed for {year}: {e}")
        return False

def create_unified_view():
    """Create a unified view across all years"""
    print("🔗 Creating unified multi-year view...")
    
    from google.cloud import bigquery
    client = bigquery.Client(project="nwsl-data")
    
    # Get all player_stats tables
    dataset = client.dataset("nwsl_fbref")
    tables = list(dataset.list_tables())
    player_tables = [table.table_id for table in tables if table.table_id.startswith('player_stats_')]
    
    if not player_tables:
        print("❌ No player stats tables found")
        return False
    
    # Create UNION ALL query
    union_queries = []
    for table in sorted(player_tables):
        union_queries.append(f"SELECT * FROM `nwsl-data.nwsl_fbref.{table}`")
    
    create_view_sql = f"""
    CREATE OR REPLACE VIEW `nwsl-data.nwsl_fbref.player_stats_all_years` AS
    {' UNION ALL '.join(union_queries)}
    """
    
    try:
        query_job = client.query(create_view_sql)
        query_job.result()
        print("✅ Created unified view: nwsl_fbref.player_stats_all_years")
        return True
    except Exception as e:
        print(f"❌ Failed to create unified view: {e}")
        return False

def main():
    """Main processing function"""
    print("🚀 Processing All NWSL Player Statistics Files")
    print("=" * 60)
    
    # Find all Excel files
    excel_dir = Path("data/raw/excel")
    excel_files = list(excel_dir.glob("*.xlsx"))
    
    if not excel_files:
        print("❌ No Excel files found in data/raw/excel/")
        return
    
    print(f"📁 Found {len(excel_files)} Excel files")
    
    processed_count = 0
    uploaded_count = 0
    all_data = []
    
    # Process each file
    for file_path in sorted(excel_files):
        year = extract_year_from_filename(file_path.name)
        if not year:
            print(f"⚠️ Could not extract year from {file_path.name}, skipping")
            continue
        
        # Process file
        df = process_excel_file(file_path, year)
        if df is not None:
            processed_count += 1
            all_data.append(df)
            
            # Upload to BigQuery
            if upload_to_bigquery(df, year):
                uploaded_count += 1
            
            # Save processed CSV
            output_path = f"data/processed/player_stats_{year}.csv"
            df.to_csv(output_path, index=False)
            print(f"💾 Saved to: {output_path}")
        
        print()  # Empty line for readability
    
    # Create unified view
    if uploaded_count > 0:
        create_unified_view()
    
    # Summary statistics
    print("📊 PROCESSING SUMMARY")
    print("=" * 30)
    print(f"Files found: {len(excel_files)}")
    print(f"Files processed: {processed_count}")
    print(f"Files uploaded: {uploaded_count}")
    
    if all_data:
        total_players = sum(len(df) for df in all_data)
        years = sorted([df['season'].iloc[0] for df in all_data])
        unique_teams = set()
        for df in all_data:
            unique_teams.update(df['Squad'].unique())
        
        print(f"Total players across all years: {total_players}")
        print(f"Years processed: {years}")
        print(f"Unique teams: {len(unique_teams)}")
        print(f"Teams: {sorted(unique_teams)}")
    
    print(f"\n🎉 Processing complete!")
    if uploaded_count > 0:
        print("📊 Data available in BigQuery:")
        print("   - Individual year tables: nwsl_fbref.player_stats_YYYY")
        print("   - Unified view: nwsl_fbref.player_stats_all_years")

if __name__ == "__main__":
    main()