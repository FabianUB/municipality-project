"""
Pipeline job definitions for municipality analytics.

Defines the orchestration jobs that coordinate asset execution:
1. Domain-specific jobs for focused data processing
2. Full integrated pipeline for end-to-end analytics
"""

from dagster import job

from ..assets.demography import (
    convert_demography_excel_to_csv,
    create_raw_schema,
    load_demography_to_postgres
)
from ..assets.codes_data import (
    convert_municipality_dictionary_to_csv,
    convert_provinces_mapping_to_csv,
    validate_codes_data
)
from ..assets.dbt_models import dbt_build_all_models
from ..assets.sepe_unemployment import (
    sepe_raw_xls_files, 
    sepe_files_inventory, 
    sepe_clean_data,
    sepe_data_summary,
    load_sepe_unemployment_to_postgres,
    load_sepe_contracts_to_postgres
)


@job
def codes_data_etl_pipeline():
    """
    ETL pipeline for geographic codes data processing.
    
    Processes Spanish municipality and province reference data:
    1. PostgreSQL schema initialization (shared infrastructure)
    2. Municipality dictionary: Official INE municipality codes and names
    3. Province mapping: Province to autonomous community relationships
    4. Data validation: Consistency checks across datasets
    
    Outputs:
    - Clean CSV files in clean/ine/codes_data/
    - Validated reference data automatically copied to dbt/seeds/
    - Data quality validation results
    
    Use case: Run independently to refresh geographic reference data
    """
    schema_creation = create_raw_schema()
    dictionary_conversion = convert_municipality_dictionary_to_csv()
    mapping_conversion = convert_provinces_mapping_to_csv()
    validation = validate_codes_data()


@job
def demography_etl_pipeline():
    """
    ETL pipeline for demographic data processing.
    
    Processes 28 years of Spanish municipality population data (1996-2024):
    1. PostgreSQL schema initialization (shared infrastructure)
    2. Excel to CSV conversion with smart header detection
    3. Data standardization and loading to raw tables
    
    Key features:
    - Handles variable Excel formats across years
    - Standardizes column names and data types
    - Preserves data lineage and metadata
    - Loads to PostgreSQL with proper error handling
    
    Use case: Run independently to refresh demographic data
    """
    schema_creation = create_raw_schema()
    csv_conversion = convert_demography_excel_to_csv()
    db_loading = load_demography_to_postgres()


@job
def full_analytics_pipeline():
    """
    Complete end-to-end analytics pipeline.
    
    Executes the full municipality analytics workflow:
    1. Geographic codes data processing and validation
    2. Demographic data extraction, transformation, and loading
    3. SEPE unemployment and contracts data processing
    4. dbt transformations: staging → intermediate → marts
    
    Execution flow:
    ```
    Codes Data Processing ─┐
                          │
    Demography Data ETL ──┼─→ dbt Transformations
                          │
    SEPE Data ETL ────────┘
    ```
    
    Dependencies are automatically resolved:
    - dbt models wait for all data sources to be ready
    - All data quality validations must pass
    - Comprehensive error handling and rollback capabilities
    
    Outputs:
    - Raw data in PostgreSQL (raw schema)
    - Staging models (public_staging schema)
    - Intermediate models (public_intermediate schema)  
    - Marts models (public_marts schema)
    
    Use case: Primary production pipeline for complete analytics refresh
    """
    # Shared infrastructure
    schema_creation = create_raw_schema()
    
    # Geographic reference data processing
    dictionary_conversion = convert_municipality_dictionary_to_csv()
    mapping_conversion = convert_provinces_mapping_to_csv()
    codes_validation = validate_codes_data()
    
    # Demographic data processing
    csv_conversion = convert_demography_excel_to_csv()
    db_loading = load_demography_to_postgres()
    
    # SEPE employment data processing  
    sepe_raw = sepe_raw_xls_files()
    sepe_inventory = sepe_files_inventory()
    sepe_cleaned = sepe_clean_data()
    sepe_summary = sepe_data_summary()
    sepe_unemployment_db = load_sepe_unemployment_to_postgres()
    sepe_contracts_db = load_sepe_contracts_to_postgres()
    
    # dbt transformations (depends on all data sources being ready)
    dbt_models = dbt_build_all_models()


@job
def sepe_unemployment_etl_pipeline():
    """
    Complete ETL pipeline for SEPE unemployment and contracts data.
    
    Full end-to-end processing of SEPE employment data:
    1. PostgreSQL schema initialization (shared infrastructure)
    2. Downloads raw XLS files from SEPE website
    3. Cleans and processes files into organized CSV format
    4. Loads unemployment and contracts data to PostgreSQL raw schema
    5. Generates comprehensive data summary
    
    Key features:
    - Respectful web scraping with delays
    - Optimized parallel file processing with python-calamine
    - Dual format support (OLD/NEW SEPE formats)
    - Consolidated monthly CSV output
    - PostgreSQL integration with proper schema management
    
    Outputs:
    - Raw XLS files in raw/sepe/ directory
    - Clean CSV files in clean/sepe/ directory  
    - PostgreSQL tables: raw.raw_sepe_unemployment, raw.raw_sepe_contracts
    - Processing summary and data quality metrics
    
    Use case: Complete SEPE data pipeline from extraction to database loading
    """
    schema_creation = create_raw_schema()
    raw_files = sepe_raw_xls_files()
    inventory = sepe_files_inventory()
    clean_data = sepe_clean_data()
    summary = sepe_data_summary()
    unemployment_db = load_sepe_unemployment_to_postgres()
    contracts_db = load_sepe_contracts_to_postgres()