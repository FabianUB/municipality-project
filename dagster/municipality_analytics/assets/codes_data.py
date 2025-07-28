"""
Geographic codes data processing assets for municipality analytics pipeline.

Handles the ETL process for Spanish municipality and province reference data:
1. Municipality dictionary processing and validation
2. Province-autonomous community mapping
3. Data consistency validation across datasets
4. Automatic copying to dbt seeds for transformation layer
"""

import os
import shutil
import pandas as pd
import requests
from bs4 import BeautifulSoup
from dagster import asset, Output, AssetExecutionContext


@asset(
    description="Download INE municipality dictionary Excel file",
    group_name="codes_data_etl"
)
def download_ine_dictionary(context: AssetExecutionContext) -> Output[dict]:
    """
    Download the INE municipality dictionary from https://www.ine.es/daco/daco42/codmun/diccionario25.xlsx
    
    This asset downloads the official municipality dictionary that contains
    all Spanish municipality codes and names.
    
    Returns:
        Output containing download statistics
    """
    dictionary_url = "https://www.ine.es/daco/daco42/codmun/diccionario25.xlsx"
    raw_path = "/opt/dagster/raw/ine/codes_data"
    file_path = f"{raw_path}/diccionario25.xlsx"
    
    # Create directory if it doesn't exist
    os.makedirs(raw_path, exist_ok=True)
    
    try:
        # Check if file already exists
        if os.path.exists(file_path):
            context.log.info(f"File already exists: {file_path}")
            file_size = os.path.getsize(file_path)
        else:
            context.log.info(f"Downloading INE dictionary from: {dictionary_url}")
            
            # Download the Excel file
            response = requests.get(dictionary_url, stream=True)
            response.raise_for_status()
            
            # Save file
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            context.log.info(f"Downloaded dictionary: {file_path}")
            
            # Get file size
            file_size = os.path.getsize(file_path)
        
        return Output(
            {
                "download_url": dictionary_url,
                "file_path": file_path,
                "file_size": file_size
            },
            metadata={
                "file_size_mb": round(file_size / (1024 * 1024), 2),
                "download_url": dictionary_url
            }
        )
        
    except Exception as e:
        context.log.error(f"Failed to download INE dictionary: {e}")
        import traceback
        context.log.error(f"Traceback: {traceback.format_exc()}")
        raise


@asset(
    description="Download provinces CCAA data from INE HTML table",
    group_name="codes_data_etl"
)
def download_provinces_ccaa_data(context: AssetExecutionContext) -> Output[dict]:
    """
    Extract provinces and autonomous communities data from INE HTML table
    at https://www.ine.es/daco/daco42/codmun/cod_ccaa_provincia.htm
    
    This asset scrapes the HTML table and converts it to Excel format
    for consistent processing with other INE data.
    
    Returns:
        Output containing extraction statistics
    """
    html_url = "https://www.ine.es/daco/daco42/codmun/cod_ccaa_provincia.htm"
    raw_path = "/opt/dagster/raw/ine/codes_data"
    file_path = f"{raw_path}/provinces_ccaa.xlsx"
    
    # Create directory if it doesn't exist
    os.makedirs(raw_path, exist_ok=True)
    
    try:
        # Check if file already exists
        if os.path.exists(file_path):
            context.log.info(f"File already exists: {file_path}")
            # Read existing file to get metadata
            df = pd.read_excel(file_path, sheet_name='Hoja 1', header=0)
        else:
            context.log.info(f"Extracting provinces data from: {html_url}")
            
            # Fetch the HTML page
            response = requests.get(html_url)
            response.raise_for_status()
            
            # Parse HTML with BeautifulSoup
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find the table - it's usually the first table on the page
            table = soup.find('table')
            if not table:
                raise ValueError("No table found in the HTML page")
            
            # Extract table data
            rows = []
            for tr in table.find_all('tr'):
                cells = [td.get_text(strip=True) for td in tr.find_all(['td', 'th'])]
                if cells:  # Only add non-empty rows
                    rows.append(cells)
            
            context.log.info(f"Extracted {len(rows)} rows from HTML table")
            
            # Convert to DataFrame
            if len(rows) > 1:
                # First row as headers
                df = pd.DataFrame(rows[1:], columns=rows[0])
            else:
                raise ValueError("Not enough data rows found in table")
            
            context.log.info(f"Table shape: {df.shape}")
            context.log.info(f"Columns: {list(df.columns)}")
            
            # Save as Excel file for consistency with other INE data processing
            df.to_excel(file_path, sheet_name='Hoja 1', index=False)
            context.log.info(f"Saved as Excel: {file_path}")
        
        return Output(
            {
                "source_url": html_url,
                "file_path": file_path,
                "rows_extracted": len(df),
                "columns": list(df.columns)
            },
            metadata={
                "rows_extracted": len(df),
                "columns_count": len(df.columns),
                "source_url": html_url
            }
        )
        
    except Exception as e:
        context.log.error(f"Failed to extract provinces CCAA data: {e}")
        import traceback
        context.log.error(f"Traceback: {traceback.format_exc()}")
        raise


@asset(deps=[download_ine_dictionary])
def convert_municipality_dictionary_to_csv(context: AssetExecutionContext) -> Output[dict]:
    """
    Convert municipality dictionary Excel to CSV with standardized columns.
    
    Processes the official INE municipality dictionary which serves as the
    authoritative source for Spanish municipality codes and names.
    
    Key transformations:
    - Standardizes column names to English equivalents
    - Validates data types and ranges
    - Automatically copies result to dbt seeds for further processing
    
    Returns:
        Output containing processing statistics and data quality metrics
    """
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
        
        # Standardize column names to English
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
        
        # Data quality validation and logging
        autonomous_communities = sorted(df['autonomous_community_code'].unique())
        provinces = sorted(df['province_code'].unique())
        
        context.log.info(f"Autonomous communities: {autonomous_communities}")
        context.log.info(f"Provinces: {provinces}")
        context.log.info(f"Municipalities count: {len(df)}")
        
        # Validate expected ranges
        assert 1 <= min(autonomous_communities) and max(autonomous_communities) <= 19, \
            f"Autonomous community codes out of range: {autonomous_communities}"
        assert 1 <= min(provinces) and max(provinces) <= 52, \
            f"Province codes out of range: {provinces}"
        
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
                "autonomous_communities": len(autonomous_communities),
                "provinces": len(provinces),
                "municipalities": len(df)
            },
            metadata={
                "rows_processed": len(df),
                "columns": list(df.columns),
                "data_quality": "excellent",
                "autonomous_community_range": f"{min(autonomous_communities)}-{max(autonomous_communities)}",
                "province_range": f"{min(provinces)}-{max(provinces)}"
            }
        )
        
    except Exception as e:
        context.log.error(f"Failed to process municipality dictionary: {e}")
        import traceback
        context.log.error(f"Traceback: {traceback.format_exc()}")
        raise


@asset(deps=[download_provinces_ccaa_data])
def convert_provinces_mapping_to_csv(context: AssetExecutionContext) -> Output[dict]:
    """
    Convert provinces-autonomous communities mapping Excel to CSV with cleaning.
    
    Processes the official mapping between Spanish provinces and autonomous communities,
    which is essential for hierarchical geographic analysis.
    
    Key transformations:
    - Removes header/separator rows that interfere with data processing
    - Standardizes column names to English equivalents
    - Validates geographic code consistency
    - Automatically copies result to dbt seeds
    
    Returns:
        Output containing processing statistics and data quality metrics
    """
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
        rows_removed = initial_rows - len(df)
        context.log.info(f"Removed {rows_removed} header/separator rows")
        
        # Standardize column names to English
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
        
        # Data quality validation and logging
        autonomous_communities = len(df['autonomous_community_code'].unique())
        provinces = len(df['province_code'].unique())
        
        context.log.info(f"Autonomous communities: {autonomous_communities}")
        context.log.info(f"Provinces: {provinces}")
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
                "autonomous_communities": autonomous_communities,
                "provinces": provinces,
                "rows_removed": rows_removed
            },
            metadata={
                "rows_processed": len(df),
                "columns": list(df.columns),
                "data_quality": "cleaned",
                "cleaning_actions": f"Removed {rows_removed} invalid rows"
            }
        )
        
    except Exception as e:
        context.log.error(f"Failed to process provinces mapping: {e}")
        import traceback
        context.log.error(f"Traceback: {traceback.format_exc()}")
        raise


@asset(deps=[convert_municipality_dictionary_to_csv, convert_provinces_mapping_to_csv])
def validate_codes_data(context: AssetExecutionContext) -> Output[dict]:
    """
    Validate consistency between municipality dictionary and provinces mapping.
    
    Performs comprehensive data quality validation to ensure geographic code
    consistency across all reference datasets. This is critical for reliable
    joins in the analytics layer.
    
    Validation checks:
    1. Autonomous community codes consistency across datasets
    2. Province codes consistency across datasets  
    3. Orphaned municipalities (no matching province)
    4. Overall data integrity assessment
    
    Returns:
        Output containing validation results and data quality metrics
    """
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
            missing_in_prov = muni_auto_codes - prov_auto_codes
            missing_in_muni = prov_auto_codes - muni_auto_codes
            context.log.warning(f"Autonomous community codes mismatch:")
            context.log.warning(f"  In municipalities but not provinces: {sorted(missing_in_prov)}")
            context.log.warning(f"  In provinces but not municipalities: {sorted(missing_in_muni)}")
        
        # Validation 2: Check province codes consistency  
        muni_prov_codes = set(municipalities_df['province_code'].unique())
        prov_prov_codes = set(provinces_df['province_code'].unique())
        
        prov_codes_match = muni_prov_codes == prov_prov_codes
        context.log.info(f"Province codes match: {prov_codes_match}")
        if not prov_codes_match:
            missing_in_prov = muni_prov_codes - prov_prov_codes
            missing_in_muni = prov_prov_codes - muni_prov_codes
            context.log.warning(f"Province codes mismatch:")
            context.log.warning(f"  In municipalities but not provinces: {sorted(missing_in_prov)}")
            context.log.warning(f"  In provinces but not municipalities: {sorted(missing_in_muni)}")
        
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
        
        if orphaned_count > 0:
            orphaned_combos = merged[merged['_merge'] == 'left_only'][['autonomous_community_code', 'province_code']]
            context.log.warning(f"Orphaned province combinations: {orphaned_combos.values.tolist()}")
        
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
        
        validation_status = "passed" if validation_results["validation_passed"] else "issues_found"
        context.log.info(f"Codes data validation status: {validation_status}")
        context.log.info(f"Validation results: {validation_results}")
        
        return Output(
            validation_results,
            metadata={
                "validation_status": validation_status,
                "issues_count": sum([not auto_codes_match, not prov_codes_match, orphaned_count > 0]),
                "data_integrity_score": f"{sum([auto_codes_match, prov_codes_match, orphaned_count == 0])}/3"
            }
        )
        
    except Exception as e:
        context.log.error(f"Failed to validate codes data: {e}")
        import traceback
        context.log.error(f"Traceback: {traceback.format_exc()}")
        raise