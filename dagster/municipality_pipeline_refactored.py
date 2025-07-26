"""
Refactored municipality analytics pipeline with dbt integration
"""
from pathlib import Path
from dagster import Definitions, job
from dagster_dbt import DbtCliResource

# Import assets from modular files
from assets.demography import (
    convert_demography_excel_to_csv,
    create_raw_schema,
    load_demography_to_postgres
)
from assets.codes_data import (
    convert_municipality_dictionary_to_csv,
    convert_provinces_mapping_to_csv,
    validate_codes_data
)
from assets.dbt_models import municipality_dbt_assets

# Configure dbt resource
DBT_PROJECT_PATH = "/opt/dagster/dbt"
dbt_resource = DbtCliResource(project_dir=DBT_PROJECT_PATH)


@job
def codes_data_etl_pipeline():
    """ETL pipeline for codes data: Excel → CSV with validation"""
    dictionary_conversion = convert_municipality_dictionary_to_csv()
    mapping_conversion = convert_provinces_mapping_to_csv()
    validation = validate_codes_data()


@job
def demography_etl_pipeline():
    """ETL pipeline: Excel → CSV → PostgreSQL (manual execution only)"""
    csv_conversion = convert_demography_excel_to_csv()
    schema_creation = create_raw_schema()
    db_loading = load_demography_to_postgres()


@job
def full_analytics_pipeline():
    """
    Complete analytics pipeline: Raw data → PostgreSQL → dbt transformations
    
    Execution order:
    1. Process codes data (municipalities and provinces)
    2. Process demography data (Excel → CSV → PostgreSQL)
    3. Run dbt models (staging → intermediate → marts)
    """
    # Codes data processing
    dictionary_conversion = convert_municipality_dictionary_to_csv()
    mapping_conversion = convert_provinces_mapping_to_csv()
    codes_validation = validate_codes_data()
    
    # Demography data processing
    csv_conversion = convert_demography_excel_to_csv()
    schema_creation = create_raw_schema()
    db_loading = load_demography_to_postgres()
    
    # dbt transformations (depends on both data loading and codes validation)
    dbt_models = municipality_dbt_assets()


# Define all assets, jobs, and resources
defs = Definitions(
    assets=[
        # Demography pipeline assets
        convert_demography_excel_to_csv,
        create_raw_schema,
        load_demography_to_postgres,
        # Codes data pipeline assets
        convert_municipality_dictionary_to_csv,
        convert_provinces_mapping_to_csv,
        validate_codes_data,
        # dbt assets
        municipality_dbt_assets
    ],
    jobs=[
        demography_etl_pipeline,
        codes_data_etl_pipeline,
        full_analytics_pipeline
    ],
    resources={
        "dbt": dbt_resource
    }
)