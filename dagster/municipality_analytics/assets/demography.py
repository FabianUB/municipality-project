"""
Demography data processing assets for municipality analytics pipeline.

Handles the complete ETL process for Spanish municipality population data:
1. Excel to CSV conversion with smart header detection
2. PostgreSQL schema creation
3. Data loading with standardization and validation
"""

import os
import glob
import pandas as pd
from pathlib import Path
from sqlalchemy import text
from dagster import asset, Output, AssetExecutionContext

from ..utils.data_processing import detect_header_row, clean_dataframe, standardize_demography_columns
from ..resources.database import get_db_connection, get_data_source_config


@asset
def convert_demography_excel_to_csv(context: AssetExecutionContext) -> Output[dict]:
    """
    Convert Excel files from raw/ine/demography/ to CSV files in clean/ine/demography/.
    
    This asset handles the first stage of the demography ETL pipeline:
    - Detects variable header positions in INE Excel files
    - Standardizes column names across 28 years of data
    - Applies data cleaning and validation
    - Outputs clean CSV files for further processing
    
    Returns:
        Output containing conversion statistics and file metadata
    """
    raw_path = "/opt/dagster/raw/ine/demography"
    clean_path = "/opt/dagster/clean/ine/demography"
    
    # Create clean directory if it doesn't exist
    os.makedirs(clean_path, exist_ok=True)
    
    excel_files = glob.glob(f"{raw_path}/*.xls*")
    context.log.info(f"Found {len(excel_files)} Excel files in {raw_path}")
    context.log.info(f"Files: {[Path(f).name for f in excel_files]}")
    
    converted_files = []
    
    for file_path in excel_files:
        filename = Path(file_path).stem
        
        # Extract year from filename for column standardization
        year = _extract_year_from_filename(filename)
        
        try:
            context.log.info(f"Processing {filename} (year {year})")
            
            # First, read without specifying header to analyze structure
            df_raw = pd.read_excel(file_path, header=None)
            context.log.info(f"Raw Excel shape: {df_raw.shape}")
            
            # Detect the actual header row
            header_row = detect_header_row(df_raw)
            context.log.info(f"Detected header row: {header_row}")
            
            # Re-read with the detected header
            df = pd.read_excel(file_path, header=header_row)
            context.log.info(f"After reading with header - shape: {df.shape}")
            context.log.info(f"Original columns: {list(df.columns)[:10]}")
            
            # Clean the dataframe with year info for column standardization
            df = clean_dataframe(df, year=year)
            context.log.info(f"After cleaning - shape: {df.shape}")
            context.log.info(f"Standardized columns: {list(df.columns)[:10]}")
            
            # Additional validation - ensure we have meaningful data
            if len(df) == 0 or len(df.columns) == 0:
                context.log.warning(f"Skipping {filename} - no data after cleaning")
                continue
                
            # Save as CSV
            csv_path = f"{clean_path}/{filename}.csv"
            df.to_csv(csv_path, index=False, encoding='utf-8')
            context.log.info(f"Saved CSV: {csv_path}")
            
            converted_files.append({
                "source": filename,
                "output": f"{filename}.csv",
                "rows": len(df),
                "columns": len(df.columns),
                "header_row_detected": header_row,
                "year": year,
                "column_names": list(df.columns)[:5]
            })
            
        except Exception as e:
            context.log.error(f"Failed to convert {file_path}: {e}")
            import traceback
            context.log.error(f"Traceback: {traceback.format_exc()}")
            continue
    
    return Output(
        {"converted_files": converted_files},
        metadata={
            "files_converted": len(converted_files),
            "total_files": len(excel_files),
            "success_rate": f"{len(converted_files)}/{len(excel_files)}"
        }
    )


@asset(deps=[convert_demography_excel_to_csv])
def create_raw_schema(context: AssetExecutionContext) -> Output[str]:
    """
    Create raw schema in PostgreSQL if it doesn't exist.
    
    This asset ensures the database schema structure is in place
    before attempting to load demography data.
    
    Returns:
        Output containing the schema name that was created
    """
    context.log.info("Creating raw schema in PostgreSQL")
    
    try:
        engine = get_db_connection()
        context.log.info("Database connection established for schema creation")
        
        with engine.connect() as conn:
            # Create raw schema
            result = conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))
            conn.commit()
            context.log.info("Raw schema created successfully")
            
        return Output(
            "raw",
            metadata={"schema_created": "raw"}
        )
    except Exception as e:
        context.log.error(f"Failed to create raw schema: {e}")
        import traceback
        context.log.error(f"Traceback: {traceback.format_exc()}")
        raise Exception(f"Failed to create raw schema: {e}")


@asset(deps=[create_raw_schema])
def load_demography_to_postgres(context: AssetExecutionContext) -> Output[dict]:
    """
    Load CSV files from clean/ine/demography/ into PostgreSQL raw schema.
    
    This asset performs the final stage of demography ETL:
    - Reads all cleaned CSV files
    - Applies final standardization across all years
    - Adds data lineage metadata
    - Loads into a single PostgreSQL table with proper handling of existing data
    
    Returns:
        Output containing loading statistics and metadata
    """
    clean_path = "/opt/dagster/clean/ine/demography"
    csv_files = glob.glob(f"{clean_path}/*.csv")
    context.log.info(f"Found {len(csv_files)} CSV files in {clean_path}")
    context.log.info(f"CSV files: {[Path(f).name for f in csv_files]}")
    
    if len(csv_files) == 0:
        context.log.warning("No CSV files found! Check if Excel to CSV conversion worked.")
        return Output(
            {"loaded_tables": []},
            metadata={"error": "No CSV files found to load"}
        )
    
    engine = get_db_connection()
    context.log.info("Database connection established")
    
    # Step 1: Read and standardize ALL CSV files first
    all_dataframes = []
    loaded_tables = []
    
    context.log.info("STEP 1: Reading and standardizing all CSV files")
    
    for csv_file in csv_files:
        filename = Path(csv_file).stem
        year = _extract_year_from_filename(filename)
        
        try:
            context.log.info(f"Loading CSV: {filename}")
            
            # Read CSV file
            df = pd.read_csv(csv_file)
            context.log.info(f"CSV shape: {df.shape}")
            
            # Skip empty files
            if len(df) == 0:
                context.log.warning(f"Skipping {filename} - empty CSV file")
                continue
                
            # Apply standardized column names
            df = standardize_demography_columns(df, year)
            context.log.info(f"Standardized columns: {list(df.columns)[:10]}")
            
            # Add metadata columns for data lineage
            source_config = get_data_source_config('demography')
            df['data_year'] = year
            df['source_file'] = filename
            df['data_source'] = source_config['source_name']
            df['data_source_full'] = source_config['source_full_name']
            df['data_category'] = source_config['category']
            df['source_url'] = source_config['url']
            df['source_description'] = source_config['description']
            df['ingestion_timestamp'] = pd.Timestamp.now()
            
            all_dataframes.append(df)
            loaded_tables.append({
                "source_file": filename,
                "year": year,
                "rows_loaded": len(df),
                "columns": list(df.columns),
            })
            
            context.log.info(f"Successfully standardized {len(df)} rows from {filename}")
            
        except Exception as e:
            context.log.error(f"Failed to read/standardize {filename}: {e}")
            import traceback
            context.log.error(f"Traceback: {traceback.format_exc()}")
            continue
    
    if not all_dataframes:
        context.log.error("No dataframes were successfully processed!")
        return Output(
            {"loaded_tables": []},
            metadata={"error": "No dataframes were successfully processed"}
        )
    
    # Step 2: Combine all dataframes and load to PostgreSQL
    context.log.info("STEP 2: Combining all standardized dataframes")
    
    try:
        # Combine all dataframes with consistent columns
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        context.log.info(f"Combined dataframe shape: {combined_df.shape}")
        context.log.info(f"Final columns: {list(combined_df.columns)}")
        
        # Load the combined dataframe to PostgreSQL
        table_name = "raw_demography_population"
        context.log.info(f"Loading combined data to {table_name}")
        
        # Handle existing table that might have dependent views
        with engine.connect() as conn:
            # Check if table exists
            table_exists_query = text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'raw' 
                    AND table_name = 'raw_demography_population'
                );
            """)
            table_exists = conn.execute(table_exists_query).fetchone()[0]
            
            if table_exists:
                # Truncate existing table instead of dropping (to preserve dependent views)
                context.log.info("Table exists, truncating data to preserve dependent views")
                truncate_query = text("TRUNCATE TABLE raw.raw_demography_population")
                conn.execute(truncate_query)
                conn.commit()
                
                # Use append mode since table structure already exists
                combined_df.to_sql(
                    name=table_name,
                    con=engine,
                    schema='raw',
                    if_exists='append',
                    index=False,
                    method='multi'
                )
            else:
                # Create new table normally
                context.log.info("Creating new table")
                combined_df.to_sql(
                    name=table_name,
                    con=engine,
                    schema='raw',
                    if_exists='replace',
                    index=False,
                    method='multi'
                )
        
        context.log.info(f"Successfully loaded {len(combined_df)} total rows to PostgreSQL")
        
        return Output(
            {"loaded_tables": loaded_tables},
            metadata={
                "table_name": "raw.raw_demography_population",
                "years_loaded": [table["year"] for table in loaded_tables],
                "total_rows": len(combined_df),
                "files_processed": len(loaded_tables),
                "final_columns": list(combined_df.columns),
            }
        )
        
    except Exception as e:
        context.log.error(f"Failed to combine and load dataframes: {e}")
        import traceback
        context.log.error(f"Traceback: {traceback.format_exc()}")
        raise


def _extract_year_from_filename(filename: str) -> int:
    """
    Extract year from INE filename format (e.g., pobmun24 -> 2024).
    
    Args:
        filename: Original filename without extension
        
    Returns:
        Four-digit year
    """
    year_str = filename.replace('pobmun', '')
    if len(year_str) == 2:
        year_int = int(year_str)
        # Convert 2-digit year to 4-digit (96-99 = 1996-1999, 00-24 = 2000-2024)
        return 1900 + year_int if year_int >= 96 else 2000 + year_int
    else:
        return int(year_str)