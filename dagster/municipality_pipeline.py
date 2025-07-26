import os
import shutil
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
from sqlalchemy import create_engine, text
import logging

# Data source configuration
DATA_SOURCES = {
    'demography': {
        'source_name': 'INE',
        'source_full_name': 'Instituto Nacional de Estadística',
        'category': 'demography',
        'url': 'https://www.ine.es/jaxiT3/Tabla.htm?t=2852',
        'description': 'Municipal population data by year'
    }
    # Future data sources can be added here
    # 'economy': {
    #     'source_name': 'Banco de España',
    #     'category': 'economy',
    #     'url': 'https://...',
    #     'description': 'Economic indicators'
    # }
}

# Database connection setup
def get_db_connection():
    """Create database connection using environment variables"""
    db_user = os.getenv('DAGSTER_POSTGRES_USER')
    db_password = os.getenv('DAGSTER_POSTGRES_PASSWORD')
    db_host = os.getenv('DAGSTER_POSTGRES_HOSTNAME')
    db_port = os.getenv('DAGSTER_POSTGRES_PORT')
    db_name = os.getenv('DAGSTER_POSTGRES_DB')
    
    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    return create_engine(connection_string)

def detect_header_row(df, max_rows_to_check=10):
    """Detect which row contains the actual headers by looking for text vs numeric patterns"""
    for row_idx in range(min(max_rows_to_check, len(df))):
        row_data = df.iloc[row_idx]
        
        # Skip completely empty rows
        if row_data.isna().all():
            continue
            
        # Check if this row looks like headers (mostly text, not numbers)
        non_null_values = row_data.dropna()
        if len(non_null_values) < 2:  # Need at least 2 non-null values
            continue
            
        # Count text vs numeric values
        text_count = sum(1 for val in non_null_values if isinstance(val, str) and not str(val).replace('.', '').replace(',', '').isdigit())
        numeric_count = len(non_null_values) - text_count
        
        # If mostly text, this is likely the header row
        if text_count >= numeric_count and len(non_null_values) >= 3:
            return row_idx
    
    # Default to row 1 (index 1) if no clear header found
    return 1

def standardize_demography_columns(df, year):
    """Standardize column names across different years of demography data"""
    import re
    
    new_columns = []
    for col in df.columns:
        col_str = str(col).strip().lower()
        
        # Standardize population columns (pob98, pob99, etc.)
        if re.match(r'pob\d{2}', col_str):
            new_columns.append('population_total')
        # Standardize municipality code columns
        elif any(term in col_str for term in ['codigo', 'cod', 'cmun']) and 'municipio' in col_str:
            new_columns.append('municipality_code')
        # Standardize municipality name columns
        elif any(term in col_str for term in ['municipio', 'nombre']) and 'codigo' not in col_str:
            new_columns.append('municipality_name')
        # Standardize province CODE columns (cpro, codigo_provincia, etc.)
        elif any(term in col_str for term in ['cpro', 'cod_prov', 'codigo_prov']):
            new_columns.append('province_code')
        # Standardize province NAME columns (provincia, nombre_provincia, etc.)
        elif 'provincia' in col_str and 'codigo' not in col_str and 'cpro' not in col_str:
            new_columns.append('province_name')
        # Handle the specific case where "province" appears as a standalone column
        elif col_str == 'province':
            # Determine if this is code or name based on data inspection
            # If it contains numeric data, it's likely a code
            if len(df) > 0:
                sample_values = df[col].dropna().head(10)
                if sample_values.dtype in ['int64', 'float64'] or all(str(val).isdigit() for val in sample_values if pd.notna(val)):
                    new_columns.append('province_code')
                else:
                    new_columns.append('province_name')
            else:
                new_columns.append('province_code')  # Default assumption
        # Standardize male population columns
        elif any(term in col_str for term in ['varon', 'hombre', 'varones', 'hombres', 'masculino']):
            new_columns.append('population_male')
        # Standardize female population columns
        elif any(term in col_str for term in ['mujer', 'mujeres', 'femenino', 'femenina']):
            new_columns.append('population_female')
        # Standardize total/both sexes columns
        elif any(term in col_str for term in ['total', 'ambos_sexos', 'ambos', 'suma', 'totales']):
            new_columns.append('population_total')
        # Standardize community/autonomy columns
        elif any(term in col_str for term in ['comunidad', 'autonoma', 'ccaa']):
            new_columns.append('autonomous_community')
        # Standardize island columns (for Canarias/Baleares)
        elif any(term in col_str for term in ['isla', 'island']):
            new_columns.append('island')
        else:
            # Keep other columns as cleaned names
            clean_name = col_str
            clean_name = clean_name.replace(' ', '_')
            clean_name = clean_name.replace('/', '_')
            clean_name = clean_name.replace('-', '_')
            clean_name = clean_name.replace('(', '')
            clean_name = clean_name.replace(')', '')
            clean_name = clean_name.replace('.', '')
            clean_name = clean_name.replace(',', '')
            # Remove any remaining special characters
            clean_name = ''.join(c for c in clean_name if c.isalnum() or c == '_')
            # Ensure it doesn't start with a number
            if clean_name and clean_name[0].isdigit():
                clean_name = 'col_' + clean_name
            new_columns.append(clean_name or f'unnamed_column_{len(new_columns)}')
    
    # Handle duplicate column names by adding suffixes
    seen = {}
    final_columns = []
    for col in new_columns:
        if col in seen:
            seen[col] += 1
            final_columns.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 0
            final_columns.append(col)
    
    df.columns = final_columns
    
    # Standardize data types to ensure consistency across years
    if 'province_code' in df.columns:
        # Convert province codes to strings (zero-padded to 2 digits)
        df['province_code'] = df['province_code'].astype(str).str.zfill(2)
    
    if 'municipality_code' in df.columns:
        # Convert municipality codes to strings (zero-padded to 5 digits)
        df['municipality_code'] = df['municipality_code'].astype(str).str.zfill(5)
    
    # Ensure population columns are numeric
    for col in df.columns:
        if 'population' in col:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df

def clean_dataframe(df, year=None):
    """Clean the dataframe by removing empty rows/columns and fixing column names"""
    # Remove completely empty rows
    df = df.dropna(how='all')
    
    # Remove completely empty columns
    df = df.dropna(how='all', axis=1)
    
    # Standardize column names for demography data
    if year:
        df = standardize_demography_columns(df, year)
    else:
        # Generic column cleaning
        new_columns = []
        for col in df.columns:
            if pd.isna(col) or str(col).strip() == '':
                # Generate a name for unnamed columns
                new_columns.append(f'unnamed_column_{len(new_columns)}')
            else:
                # Clean the column name
                clean_name = str(col).strip()
                clean_name = clean_name.lower()
                clean_name = clean_name.replace(' ', '_')
                clean_name = clean_name.replace('/', '_')
                clean_name = clean_name.replace('-', '_')
                clean_name = clean_name.replace('(', '')
                clean_name = clean_name.replace(')', '')
                clean_name = clean_name.replace('.', '')
                clean_name = clean_name.replace(',', '')
                # Remove any remaining special characters
                clean_name = ''.join(c for c in clean_name if c.isalnum() or c == '_')
                # Ensure it doesn't start with a number
                if clean_name and clean_name[0].isdigit():
                    clean_name = 'col_' + clean_name
                new_columns.append(clean_name or f'unnamed_column_{len(new_columns)}')
        
        df.columns = new_columns
    
    # Remove rows that appear to be subtotals or totals (often contain aggregate data)
    # Look for rows where the first column contains words like 'total', 'suma', etc.
    if len(df.columns) > 0:
        first_col = df.columns[0]
        if first_col in df.columns:
            df = df[~df[first_col].astype(str).str.lower().str.contains('total|suma|agregado', na=False)]
    
    return df

@asset
def convert_demography_excel_to_csv(context) -> Output[dict]:
    """Convert Excel files from raw/ine/demography/ to CSV files in clean/ine/demography/"""
    
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
        year_str = filename.replace('pobmun', '')
        if len(year_str) == 2:
            if int(year_str) >= 96:
                year = 1900 + int(year_str)
            else:
                year = 2000 + int(year_str)
        else:
            year = int(year_str)
        
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
            context.log.info(f"Original columns: {list(df.columns)[:10]}")  # First 10 columns
            
            # Clean the dataframe with year info for column standardization
            df = clean_dataframe(df, year=year)
            context.log.info(f"After cleaning - shape: {df.shape}")
            context.log.info(f"Standardized columns: {list(df.columns)[:10]}")  # First 10 columns
            
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
                "column_names": list(df.columns)[:5]  # First 5 column names for debugging
            })
            
        except Exception as e:
            # Log more details about the error
            context.log.error(f"Failed to convert {file_path}: {e}")
            import traceback
            context.log.error(f"Traceback: {traceback.format_exc()}")
            continue
    
    return Output(
        {"converted_files": converted_files},
        metadata={
            "files_converted": len(converted_files),
            "total_files": len(excel_files),
        }
    )

@asset(deps=[convert_demography_excel_to_csv])
def create_raw_schema(context) -> Output[str]:
    """Create raw schema in PostgreSQL if it doesn't exist"""
    
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
            metadata={
                "schema_created": "raw"
            }
        )
    except Exception as e:
        context.log.error(f"Failed to create raw schema: {e}")
        import traceback
        context.log.error(f"Traceback: {traceback.format_exc()}")
        raise Exception(f"Failed to create raw schema: {e}")

@asset(deps=[create_raw_schema])
def load_demography_to_postgres(context) -> Output[dict]:
    """Load CSV files from clean/ine/demography/ into PostgreSQL raw schema"""
    
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
        
        # Extract year from filename (e.g., pobmun24 -> 2024)
        year_str = filename.replace('pobmun', '')
        if len(year_str) == 2:
            if int(year_str) >= 96:
                year = 1900 + int(year_str)
            else:
                year = 2000 + int(year_str)
        else:
            year = int(year_str)
        
        try:
            context.log.info(f"Loading CSV: {filename}")
            
            # Read CSV file
            df = pd.read_csv(csv_file)
            context.log.info(f"CSV shape: {df.shape}")
            context.log.info(f"CSV columns: {list(df.columns)}")
            
            # Additional validation - skip files with no data or suspicious column names
            if len(df) == 0:
                context.log.warning(f"Skipping {filename} - empty CSV file")
                continue
                
            # Apply standardized column names using our existing function
            df = standardize_demography_columns(df, year)
            context.log.info(f"Standardized columns: {list(df.columns)[:10]}")
            
            # Add metadata columns for data lineage
            source_config = DATA_SOURCES['demography']
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

# ============================================================================
# CODES DATA PROCESSING ASSETS
# ============================================================================

@asset
def convert_municipality_dictionary_to_csv(context) -> Output[dict]:
    """Convert municipality dictionary Excel to CSV with standardized columns"""
    
    raw_file = "/opt/dagster/raw/ine/codes_data/diccionario25.xlsx"
    clean_path = "/opt/dagster/clean/ine/codes_data"
    
    # Create clean directory if it doesn't exist
    os.makedirs(clean_path, exist_ok=True)
    
    try:
        context.log.info(f"Processing municipality dictionary: {raw_file}")
        
        # Read Excel file - use row 1 as header (row 0 is title)
        df = pd.read_excel(raw_file, sheet_name='dic25', header=1)
        context.log.info(f"Original shape: {df.shape}")
        context.log.info(f"Original columns: {list(df.columns)}")
        
        # Standardize column names
        column_mapping = {
            'CODAUTO': 'autonomous_community_code',
            'CPRO': 'province_code', 
            'CMUN': 'municipality_code',
            'DC': 'check_digit',
            'NOMBRE': 'municipality_name'
        }
        
        df = df.rename(columns=column_mapping)
        context.log.info(f"Standardized columns: {list(df.columns)}")
        
        # Data type validation and conversion
        df['autonomous_community_code'] = df['autonomous_community_code'].astype(int)
        df['province_code'] = df['province_code'].astype(int)
        df['municipality_code'] = df['municipality_code'].astype(int)
        df['check_digit'] = df['check_digit'].astype(int)
        df['municipality_name'] = df['municipality_name'].astype(str).str.strip()
        
        # Data quality validation
        context.log.info(f"Autonomous communities: {sorted(df['autonomous_community_code'].unique())}")
        context.log.info(f"Provinces: {sorted(df['province_code'].unique())}")
        context.log.info(f"Municipalities count: {len(df)}")
        
        # Save as CSV
        csv_path = f"{clean_path}/municipality_dictionary.csv"
        df.to_csv(csv_path, index=False, encoding='utf-8')
        context.log.info(f"Saved clean CSV: {csv_path}")
        
        # Copy to dbt seeds directory (shared volume between containers)
        seeds_path = "/opt/dagster/dbt/seeds"
        os.makedirs(seeds_path, exist_ok=True)
        seeds_file_path = f"{seeds_path}/municipality_dictionary.csv"
        shutil.copy2(csv_path, seeds_file_path)
        context.log.info(f"Copied to dbt seeds: {seeds_file_path}")
        
        return Output(
            {
                "source_file": "diccionario25.xlsx",
                "output_file": "municipality_dictionary.csv",
                "rows": len(df),
                "columns": len(df.columns),
                "autonomous_communities": len(df['autonomous_community_code'].unique()),
                "provinces": len(df['province_code'].unique()),
                "municipalities": len(df)
            },
            metadata={
                "rows_processed": len(df),
                "columns": list(df.columns),
                "data_quality": "excellent"
            }
        )
        
    except Exception as e:
        context.log.error(f"Failed to process municipality dictionary: {e}")
        import traceback
        context.log.error(f"Traceback: {traceback.format_exc()}")
        raise

@asset  
def convert_provinces_mapping_to_csv(context) -> Output[dict]:
    """Convert provinces-autonomous communities mapping Excel to CSV with cleaning"""
    
    raw_file = "/opt/dagster/raw/ine/codes_data/provinces_ccaa.xlsx"
    clean_path = "/opt/dagster/clean/ine/codes_data"
    
    # Create clean directory if it doesn't exist
    os.makedirs(clean_path, exist_ok=True)
    
    try:
        context.log.info(f"Processing provinces mapping: {raw_file}")
        
        # Read Excel file - use row 1 as header (row 0 is title)
        df = pd.read_excel(raw_file, sheet_name='Hoja 1', header=1)
        context.log.info(f"Original shape: {df.shape}")
        context.log.info(f"Original columns: {list(df.columns)}")
        
        # Data cleaning: Remove problematic rows
        # Remove rows where CODAUTO is string (like "Ciudades Autónomas:" header)
        initial_rows = len(df)
        df = df[pd.to_numeric(df['CODAUTO'], errors='coerce').notna()]
        context.log.info(f"Removed {initial_rows - len(df)} header/separator rows")
        
        # Standardize column names
        column_mapping = {
            'CODAUTO': 'autonomous_community_code',
            'Comunidad Autónoma': 'autonomous_community_name',
            'CPRO': 'province_code',
            'Provincia': 'province_name'
        }
        
        df = df.rename(columns=column_mapping)
        context.log.info(f"Standardized columns: {list(df.columns)}")
        
        # Data type conversion and cleaning
        df['autonomous_community_code'] = df['autonomous_community_code'].astype(int)
        df['province_code'] = df['province_code'].astype(int)
        df['autonomous_community_name'] = df['autonomous_community_name'].astype(str).str.strip()
        df['province_name'] = df['province_name'].astype(str).str.strip()
        
        # Sort by codes for consistent output
        df = df.sort_values(['autonomous_community_code', 'province_code'])
        
        # Data quality validation
        context.log.info(f"Autonomous communities: {len(df['autonomous_community_code'].unique())}")
        context.log.info(f"Provinces: {len(df['province_code'].unique())}")
        context.log.info(f"Total mapping records: {len(df)}")
        
        # Save as CSV
        csv_path = f"{clean_path}/provinces_autonomous_communities.csv"
        df.to_csv(csv_path, index=False, encoding='utf-8')
        context.log.info(f"Saved clean CSV: {csv_path}")
        
        # Copy to dbt seeds directory (shared volume between containers)
        seeds_path = "/opt/dagster/dbt/seeds"
        os.makedirs(seeds_path, exist_ok=True)
        seeds_file_path = f"{seeds_path}/provinces_autonomous_communities.csv"
        shutil.copy2(csv_path, seeds_file_path)
        context.log.info(f"Copied to dbt seeds: {seeds_file_path}")
        
        return Output(
            {
                "source_file": "provinces_ccaa.xlsx", 
                "output_file": "provinces_autonomous_communities.csv",
                "rows": len(df),
                "columns": len(df.columns),
                "autonomous_communities": len(df['autonomous_community_code'].unique()),
                "provinces": len(df['province_code'].unique()),
                "rows_removed": initial_rows - len(df)
            },
            metadata={
                "rows_processed": len(df),
                "columns": list(df.columns),
                "data_quality": "cleaned"
            }
        )
        
    except Exception as e:
        context.log.error(f"Failed to process provinces mapping: {e}")
        import traceback
        context.log.error(f"Traceback: {traceback.format_exc()}")
        raise

@asset(deps=[convert_municipality_dictionary_to_csv, convert_provinces_mapping_to_csv])
def validate_codes_data(context) -> Output[dict]:
    """Validate consistency between municipality dictionary and provinces mapping"""
    
    clean_path = "/opt/dagster/clean/ine/codes_data"
    
    try:
        context.log.info("Validating codes data consistency")
        
        # Read both cleaned CSV files
        municipalities_df = pd.read_csv(f"{clean_path}/municipality_dictionary.csv")
        provinces_df = pd.read_csv(f"{clean_path}/provinces_autonomous_communities.csv")
        
        context.log.info(f"Municipalities data: {len(municipalities_df)} rows")
        context.log.info(f"Provinces data: {len(provinces_df)} rows")
        
        # Validation 1: Check autonomous community codes consistency
        muni_auto_codes = set(municipalities_df['autonomous_community_code'].unique())
        prov_auto_codes = set(provinces_df['autonomous_community_code'].unique())
        
        auto_codes_match = muni_auto_codes == prov_auto_codes
        context.log.info(f"Autonomous community codes match: {auto_codes_match}")
        if not auto_codes_match:
            context.log.warning(f"Mismatch in autonomous community codes:")
            context.log.warning(f"  Municipalities: {sorted(muni_auto_codes)}")
            context.log.warning(f"  Provinces: {sorted(prov_auto_codes)}")
        
        # Validation 2: Check province codes consistency  
        muni_prov_codes = set(municipalities_df['province_code'].unique())
        prov_prov_codes = set(provinces_df['province_code'].unique())
        
        prov_codes_match = muni_prov_codes == prov_prov_codes
        context.log.info(f"Province codes match: {prov_codes_match}")
        if not prov_codes_match:
            context.log.warning(f"Mismatch in province codes:")
            context.log.warning(f"  Municipalities: {sorted(muni_prov_codes)}")
            context.log.warning(f"  Provinces: {sorted(prov_prov_codes)}")
        
        # Validation 3: Check for orphaned municipalities (no matching province)
        municipality_provinces = municipalities_df[['autonomous_community_code', 'province_code']].drop_duplicates()
        mapping_provinces = provinces_df[['autonomous_community_code', 'province_code']].drop_duplicates()
        
        merged = municipality_provinces.merge(
            mapping_provinces, 
            on=['autonomous_community_code', 'province_code'], 
            how='left', 
            indicator=True
        )
        
        orphaned_count = len(merged[merged['_merge'] == 'left_only'])
        context.log.info(f"Orphaned municipalities (no province mapping): {orphaned_count}")
        
        # Summary statistics
        validation_results = {
            "autonomous_codes_consistent": auto_codes_match,
            "province_codes_consistent": prov_codes_match,
            "orphaned_municipalities": orphaned_count,
            "total_autonomous_communities": len(muni_auto_codes),
            "total_provinces": len(muni_prov_codes),
            "total_municipalities": len(municipalities_df),
            "validation_passed": auto_codes_match and prov_codes_match and orphaned_count == 0
        }
        
        context.log.info(f"Codes data validation results: {validation_results}")
        
        return Output(
            validation_results,
            metadata={
                "validation_status": "passed" if validation_results["validation_passed"] else "issues_found",
                "issues_count": sum([not auto_codes_match, not prov_codes_match, orphaned_count > 0])
            }
        )
        
    except Exception as e:
        context.log.error(f"Failed to validate codes data: {e}")
        import traceback
        context.log.error(f"Traceback: {traceback.format_exc()}")
        raise

@job
def codes_data_etl_pipeline():
    """ETL pipeline for codes data: Excel → CSV with validation"""
    dictionary_conversion = convert_municipality_dictionary_to_csv()
    mapping_conversion = convert_provinces_mapping_to_csv()
    validation = validate_codes_data()

@job
def demography_etl_pipeline():
    """Complete ETL pipeline: Excel → CSV → PostgreSQL (manual execution only)"""
    csv_conversion = convert_demography_excel_to_csv()
    schema_creation = create_raw_schema()
    db_loading = load_demography_to_postgres()

# Define all assets and jobs (no schedules - manual execution only)
defs = Definitions(
    assets=[
        # Demography pipeline assets
        convert_demography_excel_to_csv,
        create_raw_schema,
        load_demography_to_postgres,
        # Codes data pipeline assets
        convert_municipality_dictionary_to_csv,
        convert_provinces_mapping_to_csv,
        validate_codes_data
    ],
    jobs=[
        demography_etl_pipeline,
        codes_data_etl_pipeline
    ],
)