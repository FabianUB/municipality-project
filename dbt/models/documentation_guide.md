# 📚 dbt Documentation Guide

## 🌐 Accessing Documentation

**Interactive Documentation**: http://localhost:8080

**Command to serve docs**:
```bash
docker exec -w /app/dbt municipality_dbt dbt docs serve --port 8080
```

## 📖 Documentation Highlights

### 🔢 Geographic Code Standardization Story

The documentation now comprehensively explains our integer standardization approach:

1. **The Challenge**: Mixed data types across sources
   - Demography: Text with padding ("01", "02") and decimals ("2.0")
   - Codes: Clean integers (1, 2)

2. **The Solution**: Codes_data as source of truth
   - All models standardized to integer format
   - Consistent join keys across all layers

3. **The Implementation**: Smart casting
   - `province_code::numeric::integer` for decimal handling
   - Data type tests ensure consistency

### 📊 Key Documentation Sections

#### **Sources** (`_sources.yml`)
- **Raw Data**: Detailed explanations of original formats and known issues
- **Seeds**: Complete description of geographic reference data
- **Data Lineage**: Tracks from original Excel files through processing

#### **Models** (`_models.yml`)
- **Purpose**: Clear explanation of each model's role as atomic building blocks
- **Transformations**: Detailed description of data type conversions
- **Data Quality**: Known issues and test coverage explained
- **Join Strategy**: How models connect via integer keys

#### **Project Overview** (`overview.md`)
- **Architecture**: Complete explanation of staging → intermediate → marts flow
- **Standardization**: Deep dive into integer conversion rationale
- **Development Notes**: Best practices and conventions followed

## 🎯 Key Features in Documentation

### Interactive Elements
- **Model Lineage**: Visual representation of data flow
- **Column Details**: Hover over any column for detailed descriptions
- **Test Results**: See which tests pass/fail and why
- **Source Relationships**: Trace data from Excel files to final models

### Search Functionality
- Search for specific models, columns, or concepts
- Find all references to "integer standardization"
- Locate data quality explanations

### Technical Details
- **SQL Compilation**: See exact SQL generated for each model
- **Test Definitions**: Understand what each test validates
- **Schema Information**: Column types, constraints, relationships

## 🔍 Navigation Tips

1. **Start with Overview**: Read `overview.md` for project context
2. **Explore Sources**: Understand raw data characteristics
3. **Review Staging Models**: See how atomic building blocks are created
4. **Check Tests**: Validate data quality understanding
5. **Follow Lineage**: Trace data transformations visually

## 🏷️ Key Terms Explained in Docs

- **Atomic Building Blocks**: Individual staging models serving single purposes
- **Source of Truth**: Codes_data as authoritative geographic reference
- **Integer Standardization**: Converting all geographic codes to consistent integer format
- **Join Compatibility**: Enabling reliable relationships between models
- **Data Lineage**: Tracking from original Excel files to final models

---

**Pro Tip**: The documentation auto-updates when you run `dbt docs generate`, so it always reflects your latest model changes!