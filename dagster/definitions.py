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
    convert_demography_excel_to_csv,
    create_raw_schema,
    load_demography_to_postgres
)
from municipality_analytics.assets.codes_data import (
    convert_municipality_dictionary_to_csv,
    convert_provinces_mapping_to_csv,
    validate_codes_data
)
from municipality_analytics.assets.dbt_models import municipality_dbt_models

# Import all jobs from pipelines module
from municipality_analytics.jobs.pipelines import (
    codes_data_etl_pipeline,
    demography_etl_pipeline,
    full_analytics_pipeline
)


# Define all Dagster components for execution
defs = Definitions(
    assets=[
        # Demography data processing assets
        convert_demography_excel_to_csv,
        create_raw_schema,
        load_demography_to_postgres,
        
        # Geographic codes data processing assets
        convert_municipality_dictionary_to_csv,
        convert_provinces_mapping_to_csv,
        validate_codes_data,
        
        # dbt transformation assets
        municipality_dbt_models
    ],
    jobs=[
        # Domain-specific pipelines for focused execution
        demography_etl_pipeline,
        codes_data_etl_pipeline,
        
        # Complete integrated analytics pipeline
        full_analytics_pipeline
    ]
    # Note: Resources are handled within individual modules
    # No external resources needed for current file-based dbt integration
)