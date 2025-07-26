"""
Database resources and configuration for municipality analytics pipeline
"""
import os
from sqlalchemy import create_engine

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


def get_db_connection():
    """Create database connection using environment variables"""
    db_user = os.getenv('DAGSTER_POSTGRES_USER')
    db_password = os.getenv('DAGSTER_POSTGRES_PASSWORD')
    db_host = os.getenv('DAGSTER_POSTGRES_HOSTNAME')
    db_port = os.getenv('DAGSTER_POSTGRES_PORT')
    db_name = os.getenv('DAGSTER_POSTGRES_DB')
    
    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    return create_engine(connection_string)