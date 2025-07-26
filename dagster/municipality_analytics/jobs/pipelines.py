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
from ..assets.dbt_models import municipality_dbt_models


@job
def codes_data_etl_pipeline():
    """
    ETL pipeline for geographic codes data processing.
    
    Processes Spanish municipality and province reference data:
    1. Municipality dictionary: Official INE municipality codes and names
    2. Province mapping: Province to autonomous community relationships
    3. Data validation: Consistency checks across datasets
    
    Outputs:
    - Clean CSV files in clean/ine/codes_data/
    - Validated reference data automatically copied to dbt/seeds/
    - Data quality validation results
    
    Use case: Run independently to refresh geographic reference data
    """
    dictionary_conversion = convert_municipality_dictionary_to_csv()
    mapping_conversion = convert_provinces_mapping_to_csv()
    validation = validate_codes_data()


@job
def demography_etl_pipeline():
    """
    ETL pipeline for demographic data processing.
    
    Processes 28 years of Spanish municipality population data (1996-2024):
    1. Excel to CSV conversion with smart header detection
    2. PostgreSQL schema initialization
    3. Data standardization and loading to raw tables
    
    Key features:
    - Handles variable Excel formats across years
    - Standardizes column names and data types
    - Preserves data lineage and metadata
    - Loads to PostgreSQL with proper error handling
    
    Use case: Run independently to refresh demographic data
    """
    csv_conversion = convert_demography_excel_to_csv()
    schema_creation = create_raw_schema()
    db_loading = load_demography_to_postgres()


@job
def full_analytics_pipeline():
    """
    Complete end-to-end analytics pipeline.
    
    Executes the full municipality analytics workflow:
    1. Geographic codes data processing and validation
    2. Demographic data extraction, transformation, and loading
    3. dbt transformations: staging → intermediate → marts
    
    Execution flow:
    ```
    Codes Data Processing ─┐
                          ├─→ dbt Transformations
    Demography Data ETL ──┘
    ```
    
    Dependencies are automatically resolved:
    - dbt models wait for both data sources to be ready
    - All data quality validations must pass
    - Comprehensive error handling and rollback capabilities
    
    Outputs:
    - Raw data in PostgreSQL (raw schema)
    - Staging models (public_staging schema)
    - Intermediate models (public_intermediate schema)  
    - Marts models (public_marts schema)
    
    Use case: Primary production pipeline for complete analytics refresh
    """
    # Geographic reference data processing
    dictionary_conversion = convert_municipality_dictionary_to_csv()
    mapping_conversion = convert_provinces_mapping_to_csv()
    codes_validation = validate_codes_data()
    
    # Demographic data processing
    csv_conversion = convert_demography_excel_to_csv()
    schema_creation = create_raw_schema()
    db_loading = load_demography_to_postgres()
    
    # dbt transformations (depends on both data sources being ready)
    dbt_models = municipality_dbt_models()