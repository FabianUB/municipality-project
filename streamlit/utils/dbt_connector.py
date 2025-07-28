import os
import pandas as pd
from sqlalchemy import create_engine
import streamlit as st

@st.cache_resource
def get_database_connection():
    """Create database connection using environment variables"""
    host = os.getenv('POSTGRES_HOST', 'localhost')
    db = os.getenv('POSTGRES_DB', 'municipality_analytics')
    user = os.getenv('POSTGRES_USER', 'postgres')
    password = os.getenv('POSTGRES_PASSWORD', 'postgres')
    
    connection_string = f"postgresql://{user}:{password}@{host}:5432/{db}"
    return create_engine(connection_string)

@st.cache_data
def load_data_from_table(table_name):
    """Load data from a PostgreSQL table"""
    engine = get_database_connection()
    query = f"SELECT * FROM {table_name}"
    return pd.read_sql(query, engine)

@st.cache_data
def execute_query(query):
    """Execute a custom SQL query"""
    engine = get_database_connection()
    return pd.read_sql(query, engine)