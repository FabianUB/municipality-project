"""
Database resources and configuration for municipality analytics pipeline.

Provides database connections and data source configurations.
"""

import os
from sqlalchemy import create_engine
from typing import Dict, Any


# Data source configuration registry
DATA_SOURCES: Dict[str, Dict[str, Any]] = {
    'demography': {
        'source_name': 'INE',
        'source_full_name': 'Instituto Nacional de Estadística',
        'category': 'demography',
        'url': 'https://www.ine.es/jaxiT3/Tabla.htm?t=2852',
        'description': 'Municipal population data by year',
        'years_available': list(range(1996, 2025)),
        'update_frequency': 'annual'
    },
    # Future data sources can be added here
    'economy': {
        'source_name': 'Banco de España',
        'source_full_name': 'Banco de España',
        'category': 'economy', 
        'url': 'https://www.bde.es/',
        'description': 'Economic indicators by municipality',
        'years_available': list(range(2010, 2025)),
        'update_frequency': 'quarterly'
    }
}


def get_db_connection():
    """
    Create database connection using environment variables.
    
    Returns:
        SQLAlchemy engine for PostgreSQL database
        
    Raises:
        ValueError: If required environment variables are missing
    """
    required_vars = [
        'DAGSTER_POSTGRES_USER',
        'DAGSTER_POSTGRES_PASSWORD', 
        'DAGSTER_POSTGRES_HOSTNAME',
        'DAGSTER_POSTGRES_DB'
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {missing_vars}")
    
    db_user = os.getenv('DAGSTER_POSTGRES_USER')
    db_password = os.getenv('DAGSTER_POSTGRES_PASSWORD')
    db_host = os.getenv('DAGSTER_POSTGRES_HOSTNAME')
    db_port = os.getenv('DAGSTER_POSTGRES_PORT', '5432')
    db_name = os.getenv('DAGSTER_POSTGRES_DB')
    
    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    return create_engine(connection_string)


def get_data_source_config(source_name: str) -> Dict[str, Any]:
    """
    Get configuration for a specific data source.
    
    Args:
        source_name: Name of the data source (e.g., 'demography', 'economy')
        
    Returns:
        Dictionary containing source configuration
        
    Raises:
        KeyError: If source_name is not found in DATA_SOURCES
    """
    if source_name not in DATA_SOURCES:
        available_sources = list(DATA_SOURCES.keys())
        raise KeyError(f"Unknown data source '{source_name}'. Available: {available_sources}")
    
    return DATA_SOURCES[source_name]


def get_all_data_sources() -> Dict[str, Dict[str, Any]]:
    """
    Get all available data source configurations.
    
    Returns:
        Dictionary of all data source configurations
    """
    return DATA_SOURCES.copy()