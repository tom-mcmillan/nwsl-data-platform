#!/usr/bin/env python3
"""
Ingest Excel player statistics into BigQuery
"""

import pandas as pd
import numpy as np
from pathlib import Path
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig
import os
import sys
from datetime import datetime

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent.parent))

def clean_column_names(df):
    """Clean column names for BigQuery compatibility"""
    df.columns = [col.replace('+', '_plus_').replace('-', '_minus_') for col in df.columns]
    return df

def process_excel_file(file_path):
    """Process Excel file and prepare for BigQuery upload"""
    print(f"📊 Processing {file_path}")
    
    # Read Excel file
    df = pd.read_excel(file_path)
    print(f"✅ Loaded {len(df)} rows and {len(df.columns)} columns")
    
    # Clean column names for BigQuery
    df = clean_column_names(df)
    
    # Add metadata columns
    df['season'] = 2025
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
    
    print(f"✅ Cleaned data: {len(df)} rows, {len(df.columns)} columns")
    return df

def create_bigquery_schema():
    """Define BigQuery schema for player statistics"""
    return [
        bigquery.SchemaField("Rk", "INTEGER"),
        bigquery.SchemaField("Player", "STRING"),
        bigquery.SchemaField("Nation", "STRING"),
        bigquery.SchemaField("Pos", "STRING"),
        bigquery.SchemaField("Squad", "STRING"),
        bigquery.SchemaField("Age", "STRING"),
        bigquery.SchemaField("Born", "FLOAT"),
        bigquery.SchemaField("PT_MP", "INTEGER"),
        bigquery.SchemaField("PT_Starts", "INTEGER"),
        bigquery.SchemaField("PT_Min", "INTEGER"),
        bigquery.SchemaField("PT_90s", "FLOAT"),
        bigquery.SchemaField("PERF_Gls", "INTEGER"),
        bigquery.SchemaField("PERF_Ast", "INTEGER"),
        bigquery.SchemaField("PERF_G_plus_A", "INTEGER"),
        bigquery.SchemaField("PERF_G_minus_PK", "INTEGER"),
        bigquery.SchemaField("PERF_PK", "INTEGER"),
        bigquery.SchemaField("PERF_PKatt", "INTEGER"),
        bigquery.SchemaField("PERF_CrdY", "INTEGER"),
        bigquery.SchemaField("PERF_CrdR", "INTEGER"),
        bigquery.SchemaField("EXP_xG", "FLOAT"),
        bigquery.SchemaField("EXP_npxG", "FLOAT"),
        bigquery.SchemaField("EXP_xAG", "FLOAT"),
        bigquery.SchemaField("EXP_npxG_plus_xAG", "FLOAT"),
        bigquery.SchemaField("PROG_PrgC", "INTEGER"),
        bigquery.SchemaField("PROG_PrgP", "INTEGER"),
        bigquery.SchemaField("PROG_PrgR", "INTEGER"),
        bigquery.SchemaField("P90_Gls", "FLOAT"),
        bigquery.SchemaField("P90_Ast", "FLOAT"),
        bigquery.SchemaField("P90_G_plus_A", "FLOAT"),
        bigquery.SchemaField("P90_G_minus_PK", "FLOAT"),
        bigquery.SchemaField("P90_G_plus_A_minus_PK", "FLOAT"),
        bigquery.SchemaField("P90_xG", "FLOAT"),
        bigquery.SchemaField("P90_xAG", "FLOAT"),
        bigquery.SchemaField("P90_xG_plus_xAG", "FLOAT"),
        bigquery.SchemaField("P90_npxG", "FLOAT"),
        bigquery.SchemaField("P90_npxG_plus_xAG", "FLOAT"),
        bigquery.SchemaField("Matches", "STRING"),
        bigquery.SchemaField("season", "INTEGER"),
        bigquery.SchemaField("data_source", "STRING"),
        bigquery.SchemaField("ingestion_date", "STRING"),
    ]

def upload_to_bigquery(df, dataset_id, table_id):
    """Upload processed data to BigQuery"""
    print(f"🔧 Debug: Initializing BigQuery client...")
    client = bigquery.Client()
    print(f"🔧 Debug: Project: {client.project}")
    
    # Create table reference
    print(f"🔧 Debug: Creating table reference for {dataset_id}.{table_id}")
    table_ref = client.dataset(dataset_id).table(table_id)
    
    # Configure load job
    job_config = LoadJobConfig(
        schema=create_bigquery_schema(),
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=False,
    )
    
    print(f"📤 Uploading to BigQuery: {dataset_id}.{table_id}")
    
    try:
        # Upload data directly from DataFrame
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Wait for job to complete
        
        print(f"✅ Successfully uploaded {len(df)} rows to {dataset_id}.{table_id}")
        
        # Show table info
        table = client.get_table(table_ref)
        print(f"📊 Table info: {table.num_rows} rows, {len(table.schema)} columns")
        
    except Exception as e:
        print(f"❌ Upload failed: {e}")
        # Try alternative upload method using pandas-gbq
        print("🔄 Attempting alternative upload method...")
        
        table_full_name = f"{dataset_id}.{table_id}"
        df.to_gbq(
            destination_table=table_full_name,
            project_id=client.project,
            if_exists='replace',
            chunksize=1000
        )
        print(f"✅ Successfully uploaded via pandas-gbq: {len(df)} rows")

def main():
    """Main ingestion function"""
    print("🚀 Starting Excel Player Statistics Ingestion")
    print("=" * 60)
    
    # Configuration
    excel_file = "data/raw/excel/Player Standard Stats 2025 NWSL_rev.xlsx"
    dataset_id = "nwsl_fbref"
    table_id = "player_stats_2025"
    
    try:
        # Process Excel file
        df = process_excel_file(excel_file)
        
        # Save processed data locally
        output_path = "data/processed/player_stats_2025.csv"
        df.to_csv(output_path, index=False)
        print(f"💾 Saved processed data to {output_path}")
        
        # Upload to BigQuery
        print(f"🔧 Debug: About to upload to {dataset_id}.{table_id}")
        print(f"🔧 Debug: DataFrame shape: {df.shape}")
        upload_to_bigquery(df, dataset_id, table_id)
        
        print("\n🎉 Ingestion completed successfully!")
        print(f"📊 Data available at: {dataset_id}.{table_id}")
        
    except Exception as e:
        print(f"❌ Error during ingestion: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())