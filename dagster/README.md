# Municipality Analytics Pipeline - Refactored Architecture

## 🏗️ Architecture Overview

The municipality analytics pipeline has been refactored into a modular, maintainable structure following Python packaging best practices.

```
dagster/
├── definitions.py                          # Main Dagster definitions
├── workspace.yaml                          # Workspace configuration
└── municipality_analytics/                 # Main package
    ├── __init__.py                         # Package metadata
    ├── assets/                             # Asset modules by domain
    │   ├── __init__.py
    │   ├── demography.py                   # Population data processing
    │   ├── codes_data.py                   # Geographic reference data
    │   └── dbt_models.py                   # dbt integration
    ├── jobs/                               # Pipeline orchestration
    │   ├── __init__.py
    │   └── pipelines.py                    # Job definitions
    ├── resources/                          # Shared resources
    │   ├── __init__.py
    │   └── database.py                     # DB connections & configs
    └── utils/                              # Utility functions
        ├── __init__.py
        └── data_processing.py              # Data transformation utils
```

## 📊 Module Organization

### 🎯 **Assets** (`municipality_analytics/assets/`)

**Demography Assets** (`demography.py`):
- `convert_demography_excel_to_csv`: Excel → CSV with smart header detection
- `create_raw_schema`: PostgreSQL schema initialization
- `load_demography_to_postgres`: Data loading with standardization

**Codes Data Assets** (`codes_data.py`):
- `convert_municipality_dictionary_to_csv`: Municipality reference processing
- `convert_provinces_mapping_to_csv`: Province-autonomous community mapping
- `validate_codes_data`: Data consistency validation

**dbt Models Asset** (`dbt_models.py`):
- `municipality_dbt_models`: dbt transformation execution via file triggers

### 🔧 **Utilities** (`municipality_analytics/utils/`)

**Data Processing** (`data_processing.py`):
- `detect_header_row()`: Smart Excel header detection
- `standardize_demography_columns()`: Column name standardization across years
- `clean_dataframe()`: DataFrame cleaning and validation
- Helper functions for data type standardization

### 🗄️ **Resources** (`municipality_analytics/resources/`)

**Database** (`database.py`):
- `get_db_connection()`: PostgreSQL connection with error handling
- `DATA_SOURCES`: Centralized data source configuration registry
- `get_data_source_config()`: Configuration retrieval functions

### 🚀 **Jobs** (`municipality_analytics/jobs/`)

**Pipelines** (`pipelines.py`):
- `codes_data_etl_pipeline`: Geographic reference data processing
- `demography_etl_pipeline`: Population data ETL
- `full_analytics_pipeline`: Complete end-to-end analytics workflow

## 🔄 Benefits of Refactored Architecture

### 1. **Separation of Concerns**
- Each module has a single, clear responsibility
- Assets grouped by business domain
- Utilities separated from business logic

### 2. **Improved Maintainability**
- Easy to locate and modify specific functionality
- Clear dependency relationships
- Comprehensive documentation and type hints

### 3. **Better Testing Structure**
- Individual modules can be tested in isolation
- Mock capabilities for database and file operations
- Clear interfaces for unit testing

### 4. **Team Scalability**
- New team members can understand structure quickly
- Parallel development on different modules
- Clear ownership boundaries

### 5. **Code Reusability**
- Utility functions available across all assets
- Centralized configuration management
- Consistent error handling patterns

## 🚦 Usage

### Starting the Pipeline
```bash
# Build and start containers
docker-compose build dagster
docker-compose up -d

# Access Dagster UI
http://localhost:3000
```

### Available Jobs
1. **`codes_data_etl_pipeline`**: Process geographic reference data only
2. **`demography_etl_pipeline`**: Process population data only  
3. **`full_analytics_pipeline`**: Complete end-to-end workflow ⭐

### Development Workflow
1. **Modify assets**: Edit individual asset files in `assets/`
2. **Add utilities**: Extend `utils/data_processing.py`
3. **Update jobs**: Modify orchestration in `jobs/pipelines.py`
4. **Test changes**: Use domain-specific jobs for focused testing

## 📋 Migration Notes

### Key Changes from Integrated Version
- ✅ **Modular structure**: Single file → multiple focused modules
- ✅ **Clear imports**: Explicit dependencies between modules
- ✅ **Better documentation**: Comprehensive docstrings and type hints
- ✅ **Improved error handling**: Centralized database connection management
- ✅ **Enhanced logging**: Detailed context and metadata tracking

### Backward Compatibility
- All existing functionality preserved
- Same job names and execution patterns
- Identical output schemas and data quality

### Future Extensibility
- Easy to add new data sources (economy, infrastructure, etc.)
- Simple integration of additional transformation tools
- Clear patterns for new asset development

## 🧪 Testing

The refactored structure enables comprehensive testing:

```python
# Test individual utilities
from municipality_analytics.utils.data_processing import detect_header_row

# Test database connections
from municipality_analytics.resources.database import get_db_connection

# Test individual assets
from municipality_analytics.assets.demography import convert_demography_excel_to_csv
```

## 🎯 Next Steps

1. **Enhanced Testing**: Add unit tests for each module
2. **Configuration Management**: Environment-specific configurations
3. **Monitoring**: Add observability and alerting
4. **Documentation**: Generate API docs from docstrings
5. **CI/CD**: Automated testing and deployment pipelines