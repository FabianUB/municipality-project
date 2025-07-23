import os
from dagster import (
    asset,
    job,
    schedule,
    Definitions,
    AssetMaterialization,
    Output,
)
import pandas as pd
import glob
from pathlib import Path

@asset
def convert_demography_excel_to_csv() -> Output[dict]:
    """Convert Excel files from raw/demography/ to CSV files in clean/demography/"""
    
    raw_path = "/opt/dagster/raw/demography"
    clean_path = "/opt/dagster/clean/demography"
    
    # Create clean directory if it doesn't exist
    os.makedirs(clean_path, exist_ok=True)
    
    excel_files = glob.glob(f"{raw_path}/*.xls*")
    converted_files = []
    
    for file_path in excel_files:
        filename = Path(file_path).stem
        
        try:
            # Demography files have headers on row 2 (index 1)
            df = pd.read_excel(file_path, header=1)
            
            # Save as CSV
            csv_path = f"{clean_path}/{filename}.csv"
            df.to_csv(csv_path, index=False)
            
            converted_files.append({
                "source": filename,
                "output": f"{filename}.csv",
                "rows": len(df),
                "columns": len(df.columns)
            })
            
        except Exception as e:
            context.log.warning(f"Failed to convert {file_path}: {e}")
            continue
    
    return Output(
        {"converted_files": converted_files},
        metadata={
            "files_converted": len(converted_files),
            "total_files": len(excel_files),
        }
    )

@job
def excel_to_csv_pipeline():
    """Pipeline to convert Excel files to CSV"""
    convert_demography_excel_to_csv()

@schedule(
    job=excel_to_csv_pipeline,
    cron_schedule="0 1 * * *",  # Run daily at 1 AM
)
def daily_conversion_schedule(context):
    return {}

# Define all assets and jobs
defs = Definitions(
    assets=[convert_demography_excel_to_csv],
    jobs=[excel_to_csv_pipeline],
    schedules=[daily_conversion_schedule],
)