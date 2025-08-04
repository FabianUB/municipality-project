# Spanish Municipality Local Data Stack

This purpose of this project is to get all the possible data for each Spanish municipality or city from multiple sources of data (multiple ministeries, local entities, agencies...), parse and clean the data to then model it, and display the information on a dashboard.


## 🏗️ Architecture

```mermaid
graph TB
    subgraph "Docker Environment"
        subgraph "Data Sources"
            A[Raw Excel Files<br/>INE, SEPE, etc.]
        end
        
        subgraph "Dagster Container"
            B[Python ELT Scripts<br/>Data Transformation]
            C[Dagster Orchestrator<br/>Pipeline Management]
        end
        
        subgraph "Storage Layer"
            D[CSV Files<br/>Processed Data]
            E[(PostgreSQL<br/>Database)]
        end
        
        subgraph "Analytics Layer"
            F[dbt Models<br/>Data Transformation]
        end
        
        subgraph "Presentation Layer"
            G[Streamlit Dashboard<br/>Data Visualization]
        end
        
        subgraph "Admin Tools"
            H[pgAdmin<br/>Database Management]
        end
    end
    
    A --> B
    B --> D
    B --> E
    C --> B
    D --> E
    E --> F
    F --> E
    E --> G
    E --> H
    
    style A fill:#e1f5fe
    style B fill:#f3e5f5
    style C fill:#e8f5e8
    style D fill:#fff3e0
    style E fill:#ffebee
    style F fill:#f1f8e9
    style G fill:#e3f2fd
    style H fill:#fce4ec
```

### 📁 Project Structure
```
municipality-project/
├── dagster/                    # ETL pipeline code
│   
├── dbt/                       # Data transformation models
│   ├── models/
│   │   ├── staging/           # Raw data standardization
│   │   ├── intermediate/      # Business logic layer
│   │   └── marts/            # Analytics-ready tables
│   └── analyses/             # Report templates
│   └── macros/               # Reusable code across models
├── streamlit/                # BI dashboard application
│   ├── app.py               # Main dashboard
│   ├── utils/               # Database connectors
│   └── Dockerfile           # Container configuration
├── raw/                     # Source Excel files (gitignored)
├── clean/        # Processed CSV files (gitignored)  
├── docker-compose.yml        # Service orchestration
└── .env                      # Environment configuration (gitignored)
```


## 🚀 Quick Start

### Prerequisites
- Docker and Docker Compose
- 8GB+ RAM recommended
- Ports 3000, 5050, 5432, 8080, 8501 available

### Installation Steps
```bash
# Start all services
docker-compose up -d

# Verify all services are running
docker-compose ps
```

### Access Points
- **🎯 Streamlit Dashboard**: http://localhost:8501 (Main BI interface)
- **⚙️ Dagster UI**: http://localhost:3000 (Pipeline monitoring)
- **📚 dbt Documentation**: http://localhost:8080 (Data lineage & models)
- **🗄️ pgAdmin**: http://localhost:5050 (Database administration)

## 📊 Data Sources

### ✅ Implemented
- **Demography** (INE) - Population statistics and demographic trends
- **Employment** (SEPE) - Unemployment rates and employment contracts

### 🎯 Planned Implementation
**High Priority:**
- **Income** (Hacienda) - Municipality income levels and tax revenue
- **Municipal Debt** (Hacienda) - Debt levels and financial obligations

**Medium Priority:**
- **Business Activity** (INE) - Number of businesses and commercial data
- **Budget Allocation** (Hacienda) - Municipal spending and budget distribution
- **Crime Statistics** (Interior) - Crime rates and public safety metrics
- **Real Estate** (INE) - Property values and housing market data

**Future:**
- **Weather** (AEMET) - Climate and meteorological information

---

*Spanish Municipality Local Data Stack - Comprehensive analytics for all Spanish municipalities*
