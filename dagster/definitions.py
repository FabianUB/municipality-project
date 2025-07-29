"""
Municipality Analytics Pipeline - Main Definitions

Dagster definitions for the complete municipality analytics pipeline.
This module brings together all assets, jobs, and resources for execution.

Architecture:
- Modular asset organization by domain (demography, codes_data, dbt_models)
- Comprehensive utility functions for data processing
- Database resources with proper configuration management
- Orchestrated jobs for both focused and end-to-end execution

Data Flow:
Raw Excel/CSV → Clean CSV → PostgreSQL → dbt Models → Analytics Tables
"""

from dagster import Definitions

# Import all assets from domain modules
from municipality_analytics.assets.demography import (
    download_ine_demography_zip,
    convert_demography_excel_to_csv,
    create_raw_schema,
    load_demography_to_postgres
)
from municipality_analytics.assets.codes_data import (
    download_ine_dictionary,
    download_provinces_ccaa_data,
    convert_municipality_dictionary_to_csv,
    convert_provinces_mapping_to_csv,
    validate_codes_data
)
from municipality_analytics.assets.dbt_models import municipality_dbt_models
from municipality_analytics.assets.sepe_unemployment import (
    sepe_raw_xls_files, 
    sepe_files_inventory,
    sepe_clean_data,
    sepe_data_summary,
    load_sepe_unemployment_to_postgres,
    load_sepe_contracts_to_postgres
)

# Import all jobs from pipelines module
from municipality_analytics.jobs.pipelines import (
    codes_data_etl_pipeline,
    demography_etl_pipeline,
    full_analytics_pipeline,
    sepe_unemployment_etl_pipeline
)


# Define all Dagster components for execution
defs = Definitions(
    assets=[
        # Demography data processing assets
        download_ine_demography_zip,
        convert_demography_excel_to_csv,
        create_raw_schema,
        load_demography_to_postgres,
        
        # Geographic codes data processing assets
        download_ine_dictionary,
        download_provinces_ccaa_data,
        convert_municipality_dictionary_to_csv,
        convert_provinces_mapping_to_csv,
        validate_codes_data,
        
        # dbt transformation assets
        municipality_dbt_models,
        
        # SEPE unemployment data assets
        sepe_raw_xls_files,
        sepe_files_inventory,
        sepe_clean_data,
        sepe_data_summary,
        load_sepe_unemployment_to_postgres,
        load_sepe_contracts_to_postgres
    ],
    jobs=[
        # Domain-specific pipelines for focused execution
        demography_etl_pipeline,
        codes_data_etl_pipeline,
        sepe_unemployment_etl_pipeline,
        
        # Complete integrated analytics pipeline
        full_analytics_pipeline
    ]
    # Note: Resources are handled within individual modules
    # No external resources needed for current file-based dbt integration
)