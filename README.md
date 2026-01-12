# Dakota Analytics - Energy Data Pipeline

A production-ready end-to-end data pipeline for US energy analytics, built with modern data engineering tools and best practices.

## Overview

This project demonstrates a complete data engineering solution that:

- **Ingests** real electricity consumption and pricing data from the US Energy Information Administration (EIA) API
- **Enriches** data with synthetic metrics (carbon intensity, grid stability, renewable share) via a custom FastAPI/GraphQL service
- **Orchestrates** the entire workflow using Dagster with retry policies and data quality checks
- **Transforms** data using dbt with staging, intermediate, and final models
- **Stores** everything in DuckDB for fast analytical queries
- **Reports** insights through auto-generated Jupyter notebooks with Plotly visualizations

## Quick Start

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- [EIA API Key](https://www.eia.gov/opendata/register.php) (free registration)

### One-Command Setup

**Windows:**
```batch
run.bat
```

**Linux/Mac:** // Not tested since I do not have a linux setup available
```bash
chmod +x run.sh
./run.sh
```

The script will:
1. Check Docker is running
2. Prompt for your EIA API key (if first run)
3. Build and start all containers
4. Automatically run the complete pipeline
5. Display URLs for all services

### Access Points

| Service | URL | Description |
|---------|-----|-------------|
| Dagster UI | http://localhost:3000 | Pipeline orchestration & monitoring |
| GraphQL Playground | http://localhost:8000/graphql | Interactive GraphQL interface |

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          DAGSTER ORCHESTRATION                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐              │
│  │   EIA API    │    │  Synthetic   │    │   DuckDB     │              │
│  │  (External)  │───▶│     API      │───▶│  Database    │              │
│  └──────────────┘    │  (FastAPI)   │    └──────┬───────┘              │
│                      └──────────────┘           │                       │
│                                                 ▼                       │
│                                        ┌──────────────┐                 │
│                                        │     dbt      │                 │
│                                        │ Transforms   │                 │
│                                        └──────┬───────┘                 │
│                                               │                         │
│                                               ▼                         │
│                                        ┌──────────────┐                 │
│                                        │   Reports    │                 │
│                                        │  (Jupyter)   │                 │
│                                        └──────────────┘                 │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

## Project Structure

```
dakota-technical-assessment/
├── api/                          # Synthetic Energy Enrichment API
│   ├── app/
│   │   ├── graphql/              # GraphQL schema and resolvers
│   │   ├── services/             # Synthetic data generators
│   │   └── main.py               # FastAPI application
│   ├── Dockerfile
│   └── pyproject.toml
│
├── ingestion/                    # Data Ingestion Layer
│   ├── clients/                  # API clients (EIA, Synthetic)
│   ├── tasks/                    # Ingestion task modules
│   ├── schemas/                  # Pydantic data models
│   ├── staging/                  # JSON staging files (gitignored)
│   └── main_pipeline.py          # CLI entry point
│
├── orchestration/                # Dagster Orchestration
│   ├── dagster_project/
│   │   ├── assets/               # Asset definitions
│   │   │   ├── energy_assets.py  # Ingestion assets
│   │   │   ├── database_assets.py# DuckDB loading assets
│   │   │   ├── dbt_assets.py     # dbt model assets
│   │   │   └── reports_assets.py # Notebook report assets
│   │   ├── definitions.py        # Dagster definitions
│   │   ├── schedules.py          # Cron schedules
│   │   └── sensors.py            # Event sensors
│   ├── Dockerfile
│   ├── startup.sh                # Auto-materialization script
│   └── requirements.txt
│
├── dbt/                          # dbt Transformations
│   ├── models/
│   │   ├── staging/              # Raw data cleaning
│   │   ├── intermediate/         # Business logic joins
│   │   └── final/                # Analytics-ready tables
│   ├── dbt_project.yml
│   └── profiles.yml
│
├── database/                     # DuckDB Database
│   ├── init/                     # Schema initialization
│   └── energy_data.duckdb        # Database file (gitignored)
│
├── reports/                      # Jupyter Report Templates
│   └── energy_analysis_report.ipynb
│
├── docker-compose.yml            # Container orchestration
├── run.bat                       # Windows setup script
├── run.sh                        # Linux/Mac setup script
├── .env.example                  # Environment template
└── README.md
```

## Data Pipeline

### 1. Ingestion Layer

**EIA Data (Real)**
- Source: US Energy Information Administration API v2
- Endpoint: `/v2/electricity/retail-sales/data/`
- Data: Monthly electricity prices, sales, revenue by state
- Coverage: All 50 US states, 10+ years of history

**Synthetic Enrichment Data**
- Source: Custom FastAPI service with GraphQL
- Metrics generated:
  - `carbon_intensity` - CO2 emissions per MWh
  - `weather_index` - Temperature impact factor
  - `grid_stability_index` - Grid reliability score
  - `renewable_share` - % renewable generation
  - `volatility_score` - Price volatility metric

### 2. Database Schema

```sql
-- Dimension Table
analytics.dim_states (
    stateid VARCHAR(2) PRIMARY KEY,
    state_name TEXT,
    census_region TEXT,      -- Northeast, Southeast, Midwest, South, West
    market_type TEXT         -- regulated, deregulated
)

-- Fact Tables
analytics.eia_energy (
    period TEXT,             -- YYYY-MM format
    stateid VARCHAR(2),
    price DOUBLE,            -- cents per kWh
    sales DOUBLE,            -- million kWh
    sectorid TEXT,           -- RES (residential)
    ingested_at TIMESTAMP
)

analytics.synth_metrics (
    period TEXT,
    stateid VARCHAR(2),
    carbon_intensity DOUBLE,
    weather_index DOUBLE,
    grid_stability_index DOUBLE,
    renewable_share DOUBLE,
    volatility_score DOUBLE,
    ingested_at TIMESTAMP
)
```

### 3. dbt Transformations

| Model | Type | Description |
|-------|------|-------------|
| `stg_eia_energy` | Staging | Clean and type-cast EIA data |
| `stg_synth_metrics` | Staging | Clean synthetic metrics |
| `int_energy_enriched` | Intermediate | Join EIA + synthetic + state dimensions |
| `monthly_state_trends` | Final | Aggregated analytics with YoY comparisons |

### 4. Dagster Assets

```
raw_energy_data          → Runs ingestion pipeline
    ↓
database_schema          → Creates DuckDB tables
    ↓
eia_energy              → Loads EIA staging data
synth_metrics           → Loads synthetic staging data
    ↓
stg_eia_energy          → dbt staging model
stg_synth_metrics       → dbt staging model
    ↓
int_energy_enriched     → dbt intermediate model
    ↓
monthly_state_trends    → dbt final model
    ↓
interactive_plotly_report → Jupyter notebook with visualizations
```

## Configuration

### Environment Variables

Copy `.env.example` to `.env` and configure:

```bash
# Required
EIA_API_KEY=your_api_key_here

# Pre-configured for Docker (don't change unless needed)
API_URL=http://synthetic_api:8000
DATABASE_PATH=/database/energy_data.duckdb
DAGSTER_HOME=/app/dagster_home
DBT_PROJECT_DIR=/dbt
DBT_PROFILES_DIR=/dbt
DBT_TARGET=dev
```

### Ingestion Parameters

Edit `orchestration/dagster_project/assets/energy_assets.py`:

```python
class IngestionConfig(Config):
    states: list[str] = ["AL", "AK", ...]  # States to ingest
    months: int = 120                        # Months of history (default: 10 years)
```

## Development

### Running Locally (Without Docker)

1. **API Service:**
```bash
cd api
pip install -e .
uvicorn app.main:app --reload --port 8000
```

2. **Ingestion:**
```bash
cd ingestion
pip install -r requirements.txt
python main_pipeline.py --states TX CA NY --months 24
```

3. **Dagster:**
```bash
cd orchestration
pip install -r requirements.txt
dagster dev -m dagster_project
```

4. **dbt:**
```bash
cd dbt
dbt deps
dbt run
```

### Rebuilding Containers

```bash
docker compose down
docker compose up --build
```

### Viewing Logs

```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f dagster_app
docker compose logs -f synthetic_api
```

## API Documentation

### REST Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/consumption/{state}` | GET | Get consumption data for state |
| `/pricing/{state}` | GET | Get pricing data for state |

### GraphQL Queries

```graphql
# Get consumption metrics
query {
  consumption(state: "TX", monthsBack: 12) {
    state
    period
    peakShare
    weatherIndex
    renewableShare
    gridStabilityIndex
  }
}

# Get pricing metrics
query {
  pricing(state: "CA", monthsBack: 6) {
    state
    period
    pricingType
    carbonIntensity
    volatilityScore
    congestionPremium
  }
}
```

## Error Handling

The pipeline includes multiple layers of error handling:

1. **Retry Policies** - Dagster assets retry 3x with 60s delay on failure
2. **Data Validation** - Asset checks verify state coverage after ingestion
3. **Graceful Degradation** - Missing API keys log warnings but don't crash
4. **Timeout Protection** - API calls timeout after 30s, pipeline after 20min

## Troubleshooting

### Common Issues

**Docker not running:**
```
[ERROR] Docker is not running. Please start Docker Desktop and try again.
```
→ Start Docker Desktop and wait for it to fully initialize

**Port already in use:**
```
Error: Port 3000 is already in use
```
→ Run `docker compose down` or stop conflicting services

**API key invalid:**
```
EIA API returned 401 Unauthorized
```
→ Check your API key at https://www.eia.gov/opendata/register.php

**dbt packages missing:**
```
dbt found 1 package(s) specified but only 0 installed
```
→ This is handled automatically in Docker; if running locally, run `dbt deps`

### Reset Everything

```bash
docker compose down -v
rm -rf database/energy_data.duckdb
rm -rf ingestion/staging/*.json
./run.sh  # or run.bat on Windows
```

## Tech Stack

| Component | Technology |
|-----------|------------|
| Orchestration | Dagster |
| API Framework | FastAPI + Strawberry GraphQL |
| Database | DuckDB |
| Transformations | dbt-core + dbt-duckdb |
| Containerization | Docker + Docker Compose |
| Data Validation | Pydantic |
| Visualization | Plotly + Jupyter |
| Language | Python 3.11 |

## License

This project was created as a technical assessment for Dakota Analytics.

## Author

Vardaan Bhatia - vardaanbhatia06@gmail.com