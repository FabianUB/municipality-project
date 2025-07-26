"""
dbt integration assets for municipality analytics pipeline
"""
import os
from pathlib import Path
from dagster_dbt import DbtCliResource, dbt_assets
from dagster import AssetExecutionContext


# Configure dbt project path
DBT_PROJECT_PATH = "/opt/dagster/dbt"


@dbt_assets(
    project_dir=DBT_PROJECT_PATH,
    # Define upstream dependencies - dbt models should run after data loading
    deps=["load_demography_to_postgres", "validate_codes_data"]
)
def municipality_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """
    Execute all dbt models in the municipality analytics project.
    
    This asset represents all dbt models and will be executed after:
    - Raw demography data is loaded to PostgreSQL
    - Codes data is processed and validated
    """
    # Run dbt build (compile, run, and test)
    yield from dbt.cli(["build"], context=context).stream()