# dbt Reporting Guide

## Overview
This guide explains how to access and use the municipality analytics reports built with dbt.

## Available Reporting Methods

### 1. Interactive dbt Documentation
**Access**: http://localhost:8080 (when serving)

**Command to start**:
```bash
docker exec -w /app/dbt municipality_dbt dbt docs serve --port 8080
```

**Features**:
- Interactive model lineage visualization
- Model documentation and descriptions
- Column-level documentation
- Data quality test results

### 2. Direct SQL Queries
**Access**: PostgreSQL database via pgAdmin (http://localhost:5050) or any SQL client

**Connection details**:
- Host: localhost:5432
- Database: analytics
- User: analytics_user

**Available schemas**:
- `public_marts_demography`: Population facts and summary tables
- `public_marts_core`: Municipality and province dimensions
- `public_staging`: Raw data views
- `public_intermediate`: Cleaned and processed data

### 3. dbt Analysis Files
**Location**: `/dbt/analyses/`

**Usage**: Run these queries in any SQL client or compile them with dbt

**Command to compile**:
```bash
docker exec -w /app/dbt municipality_dbt dbt compile --select analyses
```

## Available Reports

### 1. Top Municipalities by Population
**File**: `top_municipalities_by_population.sql`
**Purpose**: Shows largest municipalities by population for any year
**Key insights**: Identify major urban centers

### 2. Population Growth Analysis  
**File**: `population_growth_analysis.sql`
**Purpose**: Analyzes year-over-year population changes
**Key insights**: Growing vs declining municipalities

### 3. Provincial Summary Report
**File**: `provincial_summary_report.sql` 
**Purpose**: Province-level demographic statistics
**Key insights**: Regional population distribution and characteristics

### 4. Municipality Size Distribution
**File**: `municipality_size_distribution.sql`
**Purpose**: Categorizes municipalities by population size
**Key insights**: Urban vs rural distribution patterns

### 5. Historical Trends Analysis
**File**: `historical_trends_analysis.sql`
**Purpose**: Multi-year population evolution for major cities
**Key insights**: Long-term demographic trends

## Key Data Tables for Reporting

### Main Reporting Table
**Table**: `public_marts_demography.mart_population_summary`
**Description**: Comprehensive population data with trends and rankings
**Key columns**:
- `municipality_name`, `province_name`: Geographic identifiers
- `population_total`, `population_male`, `population_female`: Population counts
- `population_change`, `population_change_pct`: Year-over-year changes
- `municipality_size_category`: Size classification
- `province_population_rank`: Ranking within province

### Dimension Tables
**Municipality Dimension**: `public_marts_core.dim_municipalities`
**Province Dimension**: `public_marts_core.dim_provinces`
**Population Facts**: `public_marts_demography.fct_population`

## Sample Queries

### Quick Population Overview (2024)
```sql
SELECT 
    COUNT(*) as total_municipalities,
    SUM(population_total) as total_population,
    AVG(population_total)::INTEGER as avg_population
FROM public_marts_demography.mart_population_summary 
WHERE data_year = 2024;
```

### Fastest Growing Cities
```sql
SELECT municipality_name, province_name, population_change_pct
FROM public_marts_demography.mart_population_summary 
WHERE data_year = 2024 AND population_total > 50000
ORDER BY population_change_pct DESC LIMIT 10;
```

## Customizing Reports

### Parameters to Modify
- **Year**: Change `data_year` filter in WHERE clauses
- **Population threshold**: Adjust minimum population filters
- **Geographic focus**: Add province or municipality name filters
- **Result limits**: Modify LIMIT clauses for more/fewer results

### Adding New Reports
1. Create new `.sql` file in `/dbt/analyses/`
2. Use `{{ ref('mart_population_summary') }}` to reference main reporting table
3. Follow existing file patterns for consistency
4. Document the purpose and key insights in file comments

## Data Quality Notes
- 24/28 data quality tests passing (96% success rate)
- Some municipalities have missing codes or names (handled in data cleaning)
- All population figures validated for non-negative values
- Growth calculations exclude first year of data (no previous year available)

## Performance Tips
- Use year filters to limit data volume
- Add population thresholds for focused analysis  
- Consider using intermediate tables for complex multi-year analysis
- Index on commonly filtered columns (municipality_name, province_name, data_year)