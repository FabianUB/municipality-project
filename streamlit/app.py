import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from utils.dbt_connector import load_data_from_table, execute_query

st.set_page_config(
    page_title="Spanish Municipality Analytics",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("🏛️ Spanish Municipality Analytics Dashboard")
st.markdown("### Population & Geographic Analysis (1996-2024)")

try:
    # Load main data tables
    demography_data = execute_query("""
        SELECT * FROM analytics.dim_demography 
        WHERE municipality_code IS NOT NULL 
        ORDER BY year DESC, total_population DESC
        LIMIT 1000
    """)
    
    codes_data = execute_query("""
        SELECT * FROM analytics.dim_codes_data
        WHERE municipality_code IS NOT NULL
        ORDER BY municipality_name
    """)
    
    # Sidebar filters
    st.sidebar.header("Filters")
    
    # Year filter
    years = sorted(demography_data['year'].unique())
    selected_years = st.sidebar.multiselect(
        "Select Years", 
        years, 
        default=[max(years)] if years else []
    )
    
    # Province filter
    provinces = sorted(codes_data['province_name'].dropna().unique())
    selected_provinces = st.sidebar.multiselect(
        "Select Provinces", 
        provinces,
        default=[]
    )
    
    # Filter data
    filtered_data = demography_data[demography_data['year'].isin(selected_years)] if selected_years else demography_data
    
    if selected_provinces:
        municipality_codes = codes_data[codes_data['province_name'].isin(selected_provinces)]['municipality_code']
        filtered_data = filtered_data[filtered_data['municipality_code'].isin(municipality_codes)]
    
    # Main dashboard
    if not filtered_data.empty:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Population", f"{filtered_data['total_population'].sum():,}")
        
        with col2:
            st.metric("Total Municipalities", f"{filtered_data['municipality_code'].nunique():,}")
        
        with col3:
            avg_pop = filtered_data['total_population'].mean()
            st.metric("Avg Population", f"{avg_pop:,.0f}")
        
        with col4:
            if len(selected_years) > 1:
                growth = filtered_data.groupby('year')['total_population'].sum().pct_change().iloc[-1] * 100
                st.metric("Population Growth", f"{growth:.1f}%")
            else:
                st.metric("Years Selected", len(selected_years))
        
        # Charts
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Population by Year")
            if len(selected_years) > 1:
                yearly_pop = filtered_data.groupby('year')['total_population'].sum().reset_index()
                fig = px.line(yearly_pop, x='year', y='total_population', 
                             title="Total Population Trend")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Select multiple years to see trend")
        
        with col2:
            st.subheader("Top 10 Municipalities")
            top_municipalities = filtered_data.nlargest(10, 'total_population')[
                ['municipality_code', 'total_population']
            ]
            # Merge with codes_data to get municipality names
            top_with_names = top_municipalities.merge(
                codes_data[['municipality_code', 'municipality_name']], 
                on='municipality_code', 
                how='left'
            )
            fig = px.bar(top_with_names, x='municipality_name', y='total_population',
                        title="Largest Municipalities by Population")
            fig.update_xaxis(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
        
        # Gender distribution
        st.subheader("Gender Distribution")
        if 'population_male' in filtered_data.columns and 'population_female' in filtered_data.columns:
            total_male = filtered_data['population_male'].sum()
            total_female = filtered_data['population_female'].sum()
            
            fig = go.Figure(data=[
                go.Pie(labels=['Male', 'Female'], 
                      values=[total_male, total_female],
                      hole=.3)
            ])
            fig.update_layout(title="Population by Gender")
            st.plotly_chart(fig, use_container_width=True)
        
        # Data table
        st.subheader("Filtered Data")
        display_data = filtered_data.merge(
            codes_data[['municipality_code', 'municipality_name', 'province_name']], 
            on='municipality_code', 
            how='left'
        )
        st.dataframe(display_data.head(100), use_container_width=True)
        
    else:
        st.warning("No data available for the selected filters.")

except Exception as e:
    st.error(f"Database connection error: {str(e)}")
    st.info("Make sure the PostgreSQL database is running and dbt models are materialized.")
    
    # Show sample data structure for debugging
    st.subheader("Debug Information")
    st.code(f"Error: {str(e)}")