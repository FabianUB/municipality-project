import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from utils.dbt_connector import get_dbt_connector

# Page configuration
st.set_page_config(
    page_title="Municipality Analytics Dashboard",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        text-align: center;
        margin-bottom: 2rem;
        color: #1f77b4;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 10px;
        border-left: 5px solid #1f77b4;
    }
    .sidebar .sidebar-content {
        background-color: #f8f9fa;
    }
</style>
""", unsafe_allow_html=True)

def main():
    # Header
    st.markdown('<h1 class="main-header">🏛️ Municipality Analytics Dashboard</h1>', unsafe_allow_html=True)
    st.markdown("**Comprehensive demographic analysis of Spanish municipalities (1996-2024)**")
    
    # Initialize dbt connector
    dbt = get_dbt_connector()
    
    # Sidebar filters
    st.sidebar.header("📊 Filters & Options")
    
    # Get available data
    available_years = dbt.get_available_years()
    available_provinces = dbt.get_provinces()
    
    # Year selection
    selected_year = st.sidebar.selectbox(
        "Select Year",
        options=available_years,
        index=len(available_years)-1 if available_years else 0,
        help="Choose the year for analysis"
    )
    
    # Province filter (optional)
    province_filter = st.sidebar.multiselect(
        "Filter by Province(s)",
        options=available_provinces,
        help="Leave empty to show all provinces"
    )
    
    # Main dashboard tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📈 Overview", 
        "🏆 Rankings", 
        "📊 Growth Analysis", 
        "🗺️ Provincial View", 
        "📉 Historical Trends"
    ])
    
    with tab1:
        overview_dashboard(dbt, selected_year, province_filter)
    
    with tab2:
        rankings_dashboard(dbt, selected_year, province_filter)
    
    with tab3:
        growth_dashboard(dbt, selected_year, province_filter)
    
    with tab4:
        provincial_dashboard(dbt, selected_year)
    
    with tab5:
        historical_dashboard(dbt)

def overview_dashboard(dbt, year, province_filter):
    """Overview dashboard with key metrics"""
    st.header(f"📈 Overview Dashboard - {year}")
    
    # Get summary data
    if province_filter:
        summary_data = pd.concat([
            dbt.get_mart_population_summary(year=year, province=prov) 
            for prov in province_filter
        ])
    else:
        summary_data = dbt.get_mart_population_summary(year=year)
    
    if summary_data.empty:
        st.warning("No data available for the selected filters.")
        return
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_pop = summary_data['population_total'].sum()
        st.metric(
            "Total Population",
            f"{total_pop:,}",
            help="Total population across all selected municipalities"
        )
    
    with col2:
        total_municipalities = len(summary_data)
        st.metric(
            "Municipalities",
            f"{total_municipalities:,}",
            help="Number of municipalities in selection"
        )
    
    with col3:
        avg_pop = summary_data['population_total'].mean()
        st.metric(
            "Average Population",
            f"{avg_pop:,.0f}",
            help="Average population per municipality"
        )
    
    with col4:
        if 'population_change_pct' in summary_data.columns:
            avg_growth = summary_data['population_change_pct'].mean()
            st.metric(
                "Average Growth",
                f"{avg_growth:.2f}%",
                help="Average year-over-year population growth"
            )
    
    # Size distribution
    st.subheader("Municipality Size Distribution")
    size_dist = dbt.get_size_distribution(year)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Pie chart for municipality count
        fig_pie = px.pie(
            size_dist, 
            values='municipality_count', 
            names='municipality_size_category',
            title="Distribution by Municipality Count",
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        st.plotly_chart(fig_pie, use_container_width=True)
    
    with col2:
        # Bar chart for population distribution
        fig_bar = px.bar(
            size_dist,
            x='municipality_size_category',
            y='percentage_of_population',
            title="Population Distribution by Size Category",
            labels={'percentage_of_population': 'Percentage of Total Population'}
        )
        fig_bar.update_layout(xaxis_tickangle=45)
        st.plotly_chart(fig_bar, use_container_width=True)

def rankings_dashboard(dbt, year, province_filter):
    """Rankings dashboard based on dbt analysis"""
    st.header(f"🏆 Municipality Rankings - {year}")
    
    # Top municipalities
    st.subheader("Top 20 Municipalities by Population")
    top_municipalities = dbt.get_top_municipalities(year=year, limit=20)
    
    if not top_municipalities.empty:
        # Apply province filter if selected
        if province_filter:
            top_municipalities = top_municipalities[
                top_municipalities['province_name'].isin(province_filter)
            ]
        
        # Bar chart
        fig = px.bar(
            top_municipalities.head(15),
            x='population_total',
            y='municipality_name',
            orientation='h',
            title="Top 15 Municipalities by Population",
            labels={'population_total': 'Population', 'municipality_name': 'Municipality'},
            color='municipality_size_category',
            hover_data=['province_name']
        )
        fig.update_layout(height=600)
        st.plotly_chart(fig, use_container_width=True)
        
        # Data table
        st.subheader("Detailed Rankings")
        st.dataframe(
            top_municipalities[['municipality_name', 'province_name', 'population_total', 
                             'population_male', 'population_female', 'municipality_size_category']],
            use_container_width=True
        )

def growth_dashboard(dbt, year, province_filter):
    """Growth analysis dashboard"""
    st.header(f"📊 Population Growth Analysis - {year}")
    
    # Growth settings
    col1, col2 = st.columns([1, 3])
    
    with col1:
        min_population = st.number_input(
            "Minimum Population",
            min_value=1000,
            max_value=100000,
            value=10000,
            step=5000,
            help="Filter municipalities by minimum population"
        )
    
    # Get growth data
    growth_data = dbt.get_population_growth(year=year, min_population=min_population)
    
    if not growth_data.empty:
        # Apply province filter
        if province_filter:
            growth_data = growth_data[growth_data['province_name'].isin(province_filter)]
        
        if not growth_data.empty:
            # Top growing municipalities
            st.subheader("🚀 Fastest Growing Municipalities")
            top_growth = growth_data.head(10)
            
            fig_growth = px.bar(
                top_growth,
                x='population_change_pct',
                y='municipality_name',
                orientation='h',
                title="Top 10 Growing Municipalities (% Change)",
                labels={'population_change_pct': 'Population Change (%)', 'municipality_name': 'Municipality'},
                color='population_change_pct',
                color_continuous_scale='Greens',
                hover_data=['current_population', 'population_change']
            )
            st.plotly_chart(fig_growth, use_container_width=True)
            
            # Declining municipalities
            st.subheader("📉 Declining Municipalities")
            declining = growth_data[growth_data['population_change_pct'] < 0].tail(10)
            
            if not declining.empty:
                fig_decline = px.bar(
                    declining,
                    x='population_change_pct',
                    y='municipality_name',
                    orientation='h',
                    title="Most Declining Municipalities (% Change)",
                    labels={'population_change_pct': 'Population Change (%)', 'municipality_name': 'Municipality'},
                    color='population_change_pct',
                    color_continuous_scale='Reds',
                    hover_data=['current_population', 'population_change']
                )
                st.plotly_chart(fig_decline, use_container_width=True)
            else:
                st.info("No declining municipalities found with the current filters.")
            
            # Scatter plot: Size vs Growth
            st.subheader("Population Size vs Growth Rate")
            fig_scatter = px.scatter(
                growth_data,
                x='current_population',
                y='population_change_pct',
                size='current_population',
                color='municipality_size_category',
                hover_name='municipality_name',
                hover_data=['province_name'],
                title="Municipality Size vs Growth Rate",
                labels={
                    'current_population': 'Current Population',
                    'population_change_pct': 'Growth Rate (%)'
                }
            )
            fig_scatter.update_layout(xaxis_type="log")
            st.plotly_chart(fig_scatter, use_container_width=True)

def provincial_dashboard(dbt, year):
    """Provincial summary dashboard"""
    st.header(f"🗺️ Provincial Analysis - {year}")
    
    # Get provincial data
    provincial_data = dbt.get_provincial_summary(year=year)
    
    if not provincial_data.empty:
        # Top provinces by population
        st.subheader("Top 15 Provinces by Population")
        top_provinces = provincial_data.head(15)
        
        fig_provinces = px.bar(
            top_provinces,
            x='total_population',
            y='province_name',
            orientation='h',
            title="Total Population by Province",
            labels={'total_population': 'Total Population', 'province_name': 'Province'},
            hover_data=['municipality_count', 'avg_municipality_population']
        )
        fig_provinces.update_layout(height=600)
        st.plotly_chart(fig_provinces, use_container_width=True)
        
        # Province metrics
        col1, col2 = st.columns(2)
        
        with col1:
            # Average municipality size
            fig_avg = px.bar(
                top_provinces,
                x='avg_municipality_population',
                y='province_name',
                orientation='h',
                title="Average Municipality Size by Province",
                labels={'avg_municipality_population': 'Average Population', 'province_name': 'Province'}
            )
            st.plotly_chart(fig_avg, use_container_width=True)
        
        with col2:
            # Municipality count
            fig_count = px.bar(
                top_provinces,
                x='municipality_count',
                y='province_name',
                orientation='h',
                title="Number of Municipalities by Province",
                labels={'municipality_count': 'Municipality Count', 'province_name': 'Province'}
            )
            st.plotly_chart(fig_count, use_container_width=True)
        
        # Detailed provincial table
        st.subheader("Provincial Statistics")
        st.dataframe(provincial_data, use_container_width=True)

def historical_dashboard(dbt):
    """Historical trends dashboard"""
    st.header("📉 Historical Population Trends")
    
    # Municipality selection for trends
    st.subheader("Select Municipalities for Trend Analysis")
    
    # Predefined major cities
    major_cities = ['Madrid', 'Barcelona', 'València', 'Sevilla', 'Zaragoza', 'Málaga']
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        selected_municipalities = st.multiselect(
            "Choose municipalities",
            options=major_cities,
            default=major_cities[:4],
            help="Select municipalities to compare trends"
        )
    
    with col2:
        start_year = st.selectbox(
            "Start Year",
            options=[2000, 2005, 2010, 2015],
            index=2,
            help="Choose starting year for trend analysis"
        )
    
    if selected_municipalities:
        # Get historical data
        historical_data = dbt.get_historical_trends(
            municipalities=selected_municipalities,
            start_year=start_year
        )
        
        if not historical_data.empty:
            # Line chart for population trends
            fig_trends = px.line(
                historical_data,
                x='data_year',
                y='population_total',
                color='municipality_name',
                title=f"Population Trends ({start_year}-2024)",
                labels={'data_year': 'Year', 'population_total': 'Population'},
                markers=True
            )
            st.plotly_chart(fig_trends, use_container_width=True)
            
            # Growth rate trends
            growth_data = historical_data[historical_data['population_change_pct'].notna()]
            if not growth_data.empty:
                fig_growth_trends = px.line(
                    growth_data,
                    x='data_year',
                    y='population_change_pct',
                    color='municipality_name',
                    title=f"Growth Rate Trends ({start_year}-2024)",
                    labels={'data_year': 'Year', 'population_change_pct': 'Growth Rate (%)'},
                    markers=True
                )
                fig_growth_trends.add_hline(y=0, line_dash="dash", line_color="red")
                st.plotly_chart(fig_growth_trends, use_container_width=True)

if __name__ == "__main__":
    main()