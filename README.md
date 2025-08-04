# Spanish Municipality Local Data Stack

This project tries to get all the possible data for each Spanish municipality or city from multiple sources of data (multiple ministeries, local entities, agencies...), parse and clean the data to then model it, and display the information on a dashboard.


## 🏗️ Architecture

```mermaid
graph TB
    subgraph "Docker Environment"
        subgraph "Data Sources"
            A[Raw Excel Files<br/>INE, SEPE, etc.]
        end
        
        subgraph "Dagster Container"
            B[Python ETL Scripts<br/>Data Transformation]
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

### Project Structure
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

### Installation Steps
```bash
# Step 1

# Step 2

# Step 3
```

### Access Points
- **Service 1**: Description
- **Service 2**: Description


## 🔮 Future Plans

### Planned Features

### Enhancement Roadmap

## 📚 Documentation

- **[Documentation Link](./path)**: Description
- **[Documentation Link](./path)**: Description
- **[Documentation Link](./path)**: Description

