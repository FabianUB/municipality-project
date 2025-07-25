# Municipality Analytics dbt Project

## 🏗️ Project Overview

This dbt project transforms Spanish municipality demographic data (1996-2024) and geographic reference data into a standardized analytics platform. The project follows dbt best practices with clear layering: **Staging → Intermediate → Marts**.

## 📊 Data Architecture

### Data Sources
1. **Raw Demography Data**: Population data from 28 years (1996-2024) via Dagster ETL pipeline
2. **Geographic Codes Data**: Municipality and province reference data from INE (Instituto Nacional de Estadística)

### Model Layers

#### 🧱 **Staging Layer** (`staging/`)
**Purpose**: Create atomic building blocks from source data with standardized data types and basic cleaning.

**Key Standardization**:
- **Integer Data Types**: All geographic codes converted to integers for consistent joins
- **No Unnecessary Processing**: Removed trim() operations since data is pre-cleaned by Dagster
- **Source of Truth**: Geographic codes from `codes_data` are the authoritative reference

#### 🔧 **Intermediate Layer** (`intermediate/`)
**Purpose**: Stack logical layers with specific purposes to prepare staging models for final entities.

#### 🎯 **Marts Layer** (`marts/`)
**Purpose**: Wide, rich entities optimized for business intelligence and analytics.

## 🔢 Geographic Code Standardization

### The Integer Conversion Challenge

**Original Data Formats**:
- **Demography Data**: 
  - `province_code`: Text with zero-padding ("01", "02", "11")
  - `cmun`: Mixed numeric/text with decimals ("1", "2.0", "51")
- **Codes Data**:
  - `province_code`: Clean integers (1, 2, 11)  
  - `municipality_code`: Clean integers (1, 2, 51)

**Standardization Solution**:
```sql
-- Demography staging transformation
province_code::numeric::integer as province_code,  -- "01" → 1, "2.0" → 2
cmun::integer as municipality_code                  -- 1 → 1, 51 → 51

-- Codes data staging (already clean)
province_code,      -- Source of truth: 1, 2, 11
municipality_code   -- Source of truth: 1, 2, 51
```

**Result**: All models now use consistent integer types enabling reliable joins.

## 📋 Model Documentation

### Staging Models

#### `stg_demography__population`
- **Source**: `raw.raw_demography_population`
- **Purpose**: Standardized population atoms with integer geographic codes
- **Key Transformations**:
  - Province codes: Text → Integer (`"01"` → `1`)
  - Municipality codes: Numeric → Integer (`2.0` → `2`)
  - Population validation: Ensure non-negative values
- **Data Quality**: Filters out invalid records, maintains 28 years of data

#### `stg_codes_data__municipalities`
- **Source**: `municipality_dictionary` (seed)
- **Purpose**: Authoritative municipality reference with 8,132 Spanish municipalities
- **Coverage**: All official municipalities as of 2025
- **Key Fields**: Geographic hierarchy (autonomous community → province → municipality)

#### `stg_codes_data__provinces`
- **Source**: `provinces_autonomous_communities` (seed)
- **Purpose**: Province-to-autonomous community mapping
- **Coverage**: 52 provinces across 19 autonomous communities

## 🧪 Data Quality Framework

### Test Coverage
- **Geographic Code Validation**: Range tests for province codes (1-52) and autonomous community codes (1-19)
- **Referential Integrity**: Unique constraints on province codes
- **Data Completeness**: Not-null tests on critical fields
- **Population Validation**: Range tests ensuring non-negative population values

### Known Data Quality Issues
1. **Municipality Code Nulls**: 8,098 records with null municipality codes
   - **Root Cause**: Data quality issue in source files
   - **Impact**: Expected and handled by filtering in intermediate layers
   - **Status**: Tracked for upstream resolution

## 🔗 Join Strategy

### Primary Join Keys
All joins use standardized integer keys:

```sql
-- Demography ↔ Municipalities
stg_demography__population.province_code = stg_codes_data__municipalities.province_code
AND stg_demography__population.municipality_code = stg_codes_data__municipalities.municipality_code

-- Municipalities ↔ Provinces  
stg_codes_data__municipalities.province_code = stg_codes_data__provinces.province_code
```

## 📈 Analytics Capabilities

### Available Dimensions
- **Geographic**: Autonomous Community → Province → Municipality hierarchy
- **Temporal**: Annual data from 1996-2024
- **Demographic**: Total, male, female population breakdowns

### Planned Analytics
- Population trends and growth analysis
- Geographic distribution patterns
- Municipality size categorization
- Year-over-year change analysis

## 🛠️ Development Notes

### Best Practices Followed
- ✅ **Atomic Staging**: Each model serves single, clear purpose
- ✅ **Consistent Naming**: `stg_[source]__[entity]` convention
- ✅ **Data Type Standardization**: Integer keys for reliable joins
- ✅ **Comprehensive Testing**: 96% test success rate
- ✅ **Clear Documentation**: Purpose and transformations documented

### Configuration
- **Staging**: Materialized as views for development flexibility
- **Intermediate**: Will be materialized as tables for performance
- **Marts**: Will be materialized as tables for end-user access
- **Schema Strategy**: Separate schemas for each layer (`staging`, `intermediate`, `marts`)

---

*This documentation reflects the current state of the municipality analytics dbt project as of the staging layer completion.*