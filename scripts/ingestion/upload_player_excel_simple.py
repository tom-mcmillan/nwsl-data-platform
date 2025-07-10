#!/usr/bin/env python3
"""
Simple Excel to BigQuery upload for NWSL player statistics
"""

import pandas as pd
import numpy as np
from datetime import datetime

def upload_excel_to_bigquery():
    """Upload Excel player data directly to BigQuery using pandas-gbq"""
    print("🚀 Starting simple Excel to BigQuery upload")
    print("=" * 50)
    
    # Read Excel file
    excel_path = "data/raw/excel/Player Standard Stats 2025 NWSL_rev.xlsx"
    print(f"📊 Reading Excel file: {excel_path}")
    
    try:
        df = pd.read_excel(excel_path)
        print(f"✅ Loaded {len(df)} rows and {len(df.columns)} columns")
        
        # Clean column names for BigQuery
        df.columns = [col.replace('+', '_plus_').replace('-', '_minus_') for col in df.columns]
        
        # Add metadata
        df['season'] = 2025
        df['data_source'] = 'fbref_excel'
        df['ingestion_date'] = datetime.now().isoformat()
        
        # Handle missing values
        df = df.fillna({
            'Nation': 'Unknown',
            'Age': '0-000',
            'Born': 0
        })
        
        # Convert numeric columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        print(f"✅ Data prepared: {len(df)} rows, {len(df.columns)} columns")
        
        # Upload to BigQuery using pandas-gbq
        project_id = "nwsl-data"
        table_id = "nwsl_fbref.player_stats_2025"
        
        print(f"📤 Uploading to BigQuery: {table_id}")
        
        df.to_gbq(
            destination_table=table_id,
            project_id=project_id,
            if_exists='replace',
            chunksize=1000,
            progress_bar=True
        )
        
        print(f"✅ Successfully uploaded {len(df)} rows to {table_id}")
        
        # Save processed data locally too
        output_path = "data/processed/player_stats_2025.csv"
        df.to_csv(output_path, index=False)
        print(f"💾 Also saved to: {output_path}")
        
        return True
        
    except Exception as e:
        print(f"❌ Upload failed: {e}")
        return False

if __name__ == "__main__":
    success = upload_excel_to_bigquery()
    if success:
        print("\n🎉 Upload completed successfully!")
        print("📊 Player data is now available in BigQuery")
    else:
        print("\n❌ Upload failed")