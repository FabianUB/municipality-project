import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine
from typing import Optional, Dict, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DbtConnector:
    """Connector to access dbt models and analyses directly"""
    
    def __init__(self):
        self.engine = None
        self._connect()
    
    def _connect(self):
        """Establish database connection to dbt models"""
        try:
            # Database connection (same as dbt target)
            db_host = os.getenv('POSTGRES_HOST', 'postgres')
            db_port = os.getenv('POSTGRES_PORT', '5432')
            db_name = os.getenv('POSTGRES_DB', 'analytics')
            db_user = os.getenv('POSTGRES_USER', 'analytics_user')
            db_password = os.getenv('POSTGRES_PASSWORD', 'your_secure_password')
            
            connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            self.engine = create_engine(connection_string, pool_pre_ping=True)
            
            logger.info("Connected to dbt models successfully")
            
        except Exception as e:
            logger.error(f"Failed to connect to dbt models: {str(e)}")
            st.error(f"dbt connection failed: {str(e)}")
            self.engine = None
    
    @st.cache_data(ttl=600)  # Cache for 10 minutes
    def get_mart_population_summary(_self, year: Optional[int] = None, 
                                   province: Optional[str] = None) -> pd.DataFrame:
        """Get data from mart_population_summary dbt model"""
        sql = "SELECT * FROM public_marts_demography.mart_population_summary"
        conditions = []
        params = {}
        
        if year:
            conditions.append("data_year = %(year)s")
            params['year'] = year
            
        if province:
            conditions.append("province_name = %(province)s")
            params['province'] = province
            
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
            
        sql += " ORDER BY population_total DESC"
        
        return _self._execute_query(sql, params)
    
    @st.cache_data(ttl=600)
    def get_top_municipalities(_self, year: int = 2024, limit: int = 20) -> pd.DataFrame:
        """Execute dbt analysis: top municipalities by population"""
        sql = """
        SELECT 
            municipality_name,
            province_name,
            population_total,
            population_male,
            population_female,
            municipality_size_category,
            province_population_rank
        FROM public_marts_demography.mart_population_summary
        WHERE data_year = %(year)s
        ORDER BY population_total DESC 
        LIMIT %(limit)s
        """
        return _self._execute_query(sql, {'year': year, 'limit': limit})
    
    @st.cache_data(ttl=600)
    def get_population_growth(_self, year: int = 2024, min_population: int = 10000) -> pd.DataFrame:
        """Execute dbt analysis: population growth analysis"""
        sql = """
        SELECT 
            municipality_name,
            province_name,
            population_total as current_population,
            previous_year_population,
            population_change,
            population_change_pct,
            municipality_size_category
        FROM public_marts_demography.mart_population_summary
        WHERE data_year = %(year)s
          AND population_change_pct IS NOT NULL
          AND population_total > %(min_pop)s
        ORDER BY population_change_pct DESC
        LIMIT 50
        """
        return _self._execute_query(sql, {'year': year, 'min_pop': min_population})
    
    @st.cache_data(ttl=600)
    def get_provincial_summary(_self, year: int = 2024) -> pd.DataFrame:
        """Execute dbt analysis: provincial summary"""
        sql = """
        SELECT 
            province_name,
            COUNT(DISTINCT municipality_name) as municipality_count,
            SUM(population_total) as total_population,
            ROUND(AVG(population_total), 0) as avg_municipality_population,
            MAX(population_total) as largest_municipality_pop,
            MIN(population_total) as smallest_municipality_pop,
            ROUND(
                (SUM(population_male)::NUMERIC / SUM(population_total)) * 100, 2
            ) as male_percentage,
            ROUND(
                (SUM(population_female)::NUMERIC / SUM(population_total)) * 100, 2
            ) as female_percentage
        FROM public_marts_demography.mart_population_summary
        WHERE data_year = %(year)s
        GROUP BY province_name
        ORDER BY total_population DESC
        """
        return _self._execute_query(sql, {'year': year})
    
    @st.cache_data(ttl=600)
    def get_size_distribution(_self, year: int = 2024) -> pd.DataFrame:
        """Execute dbt analysis: municipality size distribution"""
        sql = """
        SELECT 
            municipality_size_category,
            COUNT(*) as municipality_count,
            SUM(population_total) as total_population,
            ROUND(AVG(population_total), 0) as avg_population,
            ROUND(
                (COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER ()) * 100, 2
            ) as percentage_of_municipalities,
            ROUND(
                (SUM(population_total)::NUMERIC / SUM(SUM(population_total)) OVER ()) * 100, 2
            ) as percentage_of_population
        FROM public_marts_demography.mart_population_summary
        WHERE data_year = %(year)s
        GROUP BY municipality_size_category
        ORDER BY 
            CASE municipality_size_category
                WHEN 'Large (100k+)' THEN 1
                WHEN 'Medium (20k-100k)' THEN 2  
                WHEN 'Small (5k-20k)' THEN 3
                WHEN 'Very Small (1k-5k)' THEN 4
                WHEN 'Micro (<1k)' THEN 5
            END
        """
        return _self._execute_query(sql, {'year': year})
    
    @st.cache_data(ttl=600)
    def get_historical_trends(_self, municipalities: list, start_year: int = 2010) -> pd.DataFrame:
        """Execute dbt analysis: historical trends for specific municipalities"""
        municipality_list = "','".join(municipalities)
        sql = f"""
        SELECT 
            municipality_name,
            province_name,
            data_year,
            population_total,
            population_change,
            population_change_pct
        FROM public_marts_demography.mart_population_summary
        WHERE municipality_name IN ('{municipality_list}')
          AND data_year >= %(start_year)s
        ORDER BY municipality_name, data_year
        """
        return _self._execute_query(sql, {'start_year': start_year})
    
    def _execute_query(self, sql: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Execute SQL query against dbt models"""
        try:
            if self.engine is None:
                self._connect()
            
            if self.engine is None:
                raise Exception("No database connection available")
            
            df = pd.read_sql(sql, self.engine, params=params)
            logger.info(f"dbt query executed successfully, returned {len(df)} rows")
            return df
            
        except Exception as e:
            logger.error(f"dbt query execution failed: {str(e)}")
            st.error(f"Query failed: {str(e)}")
            return pd.DataFrame()
    
    def get_available_years(self) -> list:
        """Get all available years from dbt models"""
        try:
            sql = "SELECT DISTINCT data_year FROM public_marts_demography.mart_population_summary ORDER BY data_year"
            result = self._execute_query(sql)
            return result['data_year'].tolist() if not result.empty else [2024]
        except:
            return [2024]
    
    def get_provinces(self) -> list:
        """Get all available provinces from dbt models"""
        try:
            sql = "SELECT DISTINCT province_name FROM public_marts_demography.mart_population_summary ORDER BY province_name"
            result = self._execute_query(sql)
            return result['province_name'].tolist() if not result.empty else []
        except:
            return []

# Global dbt connector instance
@st.cache_resource
def get_dbt_connector():
    """Get cached dbt connector"""
    return DbtConnector()