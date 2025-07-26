"""
Codes data processing assets for municipality analytics pipeline
"""
import os
import shutil
import pandas as pd
from dagster import asset, Output, AssetExecutionContext


@asset
def convert_municipality_dictionary_to_csv(context: AssetExecutionContext) -> Output[dict]:
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
def convert_provinces_mapping_to_csv(context: AssetExecutionContext) -> Output[dict]:
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
def validate_codes_data(context: AssetExecutionContext) -> Output[dict]:
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