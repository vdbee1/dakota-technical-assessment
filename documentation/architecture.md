# System Architecture

This document provides a comprehensive overview of the Dakota Analytics Energy Data Pipeline architecture, including system design, technology choices, data flow, and scalability considerations.

Note: Detailed design documents will be available in the architecture folder
---

## System Design Overview

The pipeline follows a modern **ELT (Extract, Load, Transform)** architecture with clear separation of concerns across five main layers:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              PRESENTATION LAYER                              │
│                     Dagster UI │ GraphQL Playground │ Reports               │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
┌─────────────────────────────────────────────────────────────────────────────┐
│                            ORCHESTRATION LAYER                               │
│                                  Dagster                                     │
│              Assets │ Schedules │ Sensors │ Retry Policies                  │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
┌─────────────────────────────────────────────────────────────────────────────┐
│                           TRANSFORMATION LAYER                               │
│                                   dbt                                        │
│                    Staging │ Intermediate │ Final Models                    │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
┌─────────────────────────────────────────────────────────────────────────────┐
│                              STORAGE LAYER                                   │
│                                 DuckDB                                       │
│                  analytics.eia_energy │ analytics.synth_metrics             │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
┌─────────────────────────────────────────────────────────────────────────────┐
│                             INGESTION LAYER                                  │
│                    EIA API Client │ Synthetic API Client                    │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
┌─────────────────────────────────────────────────────────────────────────────┐
│                              SOURCE LAYER                                    │
│                      EIA API (External) │ Synthetic API (Internal)          │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Technology Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| **Orchestration** | Dagster | Pipeline scheduling, monitoring, lineage |
| **API** | FastAPI + Strawberry | Synthetic data service with REST/GraphQL |
| **Database** | DuckDB | Analytical storage (columnar, embedded) |
| **Transformations** | dbt-core + dbt-duckdb | SQL-based data modeling |
| **Containerization** | Docker + Docker Compose | Reproducible deployment |
| **Reporting** | Papermill + Plotly + Jupyter | Automated visualizations |
| **Data Validation** | Pydantic | Schema enforcement |
| **Language** | Python 3.11 | Primary development language |

### Why These Technologies?

**Dagster** - Chosen for its asset-centric paradigm which naturally models data dependencies. Unlike task-based orchestrators, Dagster understands *what data exists* not just *what code runs*.

**DuckDB** - An embedded analytical database that provides warehouse-level performance without infrastructure overhead. Perfect for batch analytical workloads.

**FastAPI** - Modern async Python framework with automatic OpenAPI docs. Strawberry adds type-safe GraphQL with minimal boilerplate.

**dbt** - Industry-standard transformation tool that brings software engineering practices (version control, testing, documentation) to SQL.

---

## Data Flow

### High-Level Flow

```
EIA API ─────┐
             ├──► Ingestion ──► Staging Files ──► DuckDB ──► dbt ──► Reports
Synthetic ───┘         │              │              │          │
   API                 │              │              │          │
                       ▼              ▼              ▼          ▼
                   Dagster        JSON Files    Raw Tables   Models
                   Assets        (staging/)    (analytics)  (final)
```

### Detailed Data Flow

#### 1. Extraction Phase
```
┌──────────────┐     HTTP/JSON      ┌──────────────┐
│   EIA API    │ ─────────────────► │  EIA Client  │
│  (External)  │                    │  (Python)    │
└──────────────┘                    └──────┬───────┘
                                          │
┌──────────────┐     HTTP/GraphQL   ┌──────────────┐
│  Synthetic   │ ─────────────────► │ Synth Client │
│     API      │                    │  (Python)    │
└──────────────┘                    └──────┬───────┘
                                          │
                                          ▼
                                   ┌──────────────┐
                                   │   Staging    │
                                   │    Files     │
                                   │   (JSON)     │
                                   └──────────────┘
```

#### 2. Loading Phase
```
┌──────────────┐                    ┌──────────────┐
│   Staging    │     DuckDB         │  Raw Tables  │
│    Files     │ ─────────────────► │  eia_energy  │
│   (JSON)     │   read_json_auto   │synth_metrics │
└──────────────┘                    └──────────────┘
```

#### 3. Transformation Phase (dbt)
```
┌─────────────────────────────────────────────────────────────┐
│                         dbt Models                          │
│                                                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────┐ │
│  │   staging   │    │intermediate │    │     final       │ │
│  │             │    │             │    │                 │ │
│  │stg_eia_     │───►│int_energy_  │───►│monthly_state_   │ │
│  │  energy     │    │  enriched   │    │    trends       │ │
│  │             │    │             │    │                 │ │
│  │stg_synth_   │───►│  (joins +   │    │ (aggregations + │ │
│  │  metrics    │    │   logic)    │    │   YoY calcs)    │ │
│  └─────────────┘    └─────────────┘    └─────────────────┘ │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## Component Architecture

### Container Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Docker Compose Network                    │
│                      (energy-network)                        │
│                                                              │
│  ┌─────────────────────┐    ┌─────────────────────────────┐ │
│  │   synthetic_api     │    │       dagster_app           │ │
│  │                     │    │                             │ │
│  │  ┌───────────────┐  │    │  ┌───────────────────────┐  │ │
│  │  │   FastAPI     │  │◄───│  │      Dagster          │  │ │
│  │  │   + GraphQL   │  │    │  │    Webserver          │  │ │
│  │  └───────────────┘  │    │  └───────────────────────┘  │ │
│  │                     │    │                             │ │
│  │  Port: 8000         │    │  ┌───────────────────────┐  │ │
│  │                     │    │  │   Ingestion Module    │  │ │
│  └─────────────────────┘    │  └───────────────────────┘  │ │
│                              │                             │ │
│                              │  ┌───────────────────────┐  │ │
│                              │  │        dbt            │  │ │
│                              │  └───────────────────────┘  │ │
│                              │                             │ │
│                              │  ┌───────────────────────┐  │ │
│                              │  │      DuckDB           │  │ │
│                              │  │  (embedded database)  │  │ │
│                              │  └───────────────────────┘  │ │
│                              │                             │ │
│                              │  Port: 3000                 │ │
│                              └─────────────────────────────┘ │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### Dagster Asset Graph

```
                    ┌─────────────────────┐
                    │   raw_energy_data   │
                    │    (Ingestion)      │
                    └──────────┬──────────┘
                               │
                    ┌──────────▼──────────┐
                    │   database_schema   │
                    │  (Schema Creation)  │
                    └──────────┬──────────┘
                               │
              ┌────────────────┼────────────────┐
              │                │                │
    ┌─────────▼─────┐  ┌───────▼───────┐       │
    │   eia_energy  │  │ synth_metrics │       │
    │   (Loading)   │  │   (Loading)   │       │
    └───────┬───────┘  └───────┬───────┘       │
            │                  │                │
    ┌───────▼───────┐  ┌───────▼───────┐       │
    │stg_eia_energy │  │stg_synth_     │       │
    │    (dbt)      │  │  metrics(dbt) │       │
    └───────┬───────┘  └───────┬───────┘       │
            │                  │                │
            └────────┬─────────┘                │
                     │                          │
           ┌─────────▼─────────┐    ┌──────────▼──────────┐
           │ int_energy_       │    │    dim_states       │
           │   enriched (dbt)  │◄───│      (dbt)          │
           └─────────┬─────────┘    └─────────────────────┘
                     │
           ┌─────────▼─────────┐
           │ monthly_state_    │
           │   trends (dbt)    │
           └─────────┬─────────┘
                     │
           ┌─────────▼─────────┐
           │interactive_plotly_│
           │   report (Jupyter)│
           └───────────────────┘
```

---

## Database Schema

### Entity Relationship Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                        DuckDB Schema                         │
│                         (analytics)                          │
└─────────────────────────────────────────────────────────────┘

┌─────────────────┐       ┌─────────────────┐
│   dim_states    │       │   eia_energy    │
├─────────────────┤       ├─────────────────┤
│ PK stateid      │◄──────│ FK stateid      │
│    state_name   │       │ PK period       │
│    census_region│       │    price        │
│    market_type  │       │    sales        │
└─────────────────┘       │    sectorid     │
        │                 │    ingested_at  │
        │                 └─────────────────┘
        │
        │                 ┌─────────────────┐
        │                 │  synth_metrics  │
        │                 ├─────────────────┤
        └────────────────►│ FK stateid      │
                          │ PK period       │
                          │  carbon_intensity│
                          │  weather_index  │
                          │  grid_stability │
                          │  renewable_share│
                          │  volatility_score│
                          │  ingested_at    │
                          └─────────────────┘
```

---

## Scalability Considerations

### Current Design (Assessment Scope)

| Aspect | Current Implementation | Limitation |
|--------|----------------------|------------|
| Data Volume | ~100K records | Single file DuckDB |
| Concurrency | Single user | Embedded database |
| Compute | Single container | No parallelism |
| Storage | Local filesystem | Container restart loses data |

### Production Scaling Path

#### Short-term (10x scale)
```
Current                          Improved
────────                         ────────
DuckDB (embedded)        →       DuckDB (file on volume mount)
Single Dagster container →       Dagster + separate workers
JSON staging files       →       Parquet staging files
```

#### Medium-term (100x scale)
```
Improved                         Scaled
────────                         ──────
DuckDB                   →       PostgreSQL / Snowflake
Docker Compose           →       Kubernetes
Monthly batch            →       Daily incremental
Local storage            →       S3 / GCS
```

#### Long-term (1000x+ scale)
```
Scaled                           Enterprise
──────                           ──────────
Single region            →       Multi-region
Batch processing         →       Streaming (Kafka + Flink)
Dagster Cloud            →       Dagster Cloud + Hybrid
Snowflake                →       Lakehouse (Databricks/Iceberg)
```

### Scaling Strategies by Component

| Component | Strategy |
|-----------|----------|
| **Ingestion** | Parallelize by state, use async HTTP |
| **Storage** | Move to cloud data warehouse |
| **Transforms** | dbt Cloud with Snowflake/BigQuery |
| **Orchestration** | Dagster Cloud with auto-scaling |
| **API** | Kubernetes HPA, read replicas |

---

## Security Considerations

### Current Implementation
- API keys stored in `.env` (gitignored)
- No authentication on internal APIs
- Network isolation via Docker network

### Production Recommendations
```
┌─────────────────────────────────────────────────────────────┐
│                    Security Improvements                     │
├─────────────────────────────────────────────────────────────┤
│ • Secrets management (Vault, AWS Secrets Manager)           │
│ • API authentication (OAuth2, API keys with rotation)       │
│ • Network policies (ingress/egress rules)                   │
│ • Data encryption at rest and in transit                    │
│ • Audit logging for data access                             │
│ • Row-level security in database                            │
└─────────────────────────────────────────────────────────────┘
```

---

## Monitoring & Observability

### Current Implementation
- Dagster UI for pipeline monitoring
- Container logs via `docker compose logs`
- Asset materialization history

### Production Recommendations
```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Metrics    │     │    Logs      │     │   Traces     │
│  (Prometheus)│     │ (CloudWatch) │     │   (Jaeger)   │
└──────┬───────┘     └──────┬───────┘     └──────┬───────┘
       │                    │                    │
       └────────────────────┼────────────────────┘
                            │
                    ┌───────▼───────┐
                    │   Grafana     │
                    │  Dashboards   │
                    └───────────────┘
```

---

## Failure Modes & Recovery

| Failure | Detection | Recovery |
|---------|-----------|----------|
| EIA API down | Dagster retry timeout | Retry 3x, then alert |
| Synthetic API down | Health check fails | Container restart |
| DuckDB corruption | dbt test failures | Restore from backup |
| Container crash | Docker health check | Auto-restart policy |
| Bad data | Asset checks | Block downstream, alert |

### Recovery Procedures

```bash
# Full pipeline re-run
docker compose down
rm database/energy_data.duckdb
./run.sh

# Single asset re-materialization
docker compose exec dagster_app \
  dagster asset materialize -m dagster_project \
  --select "raw_energy_data"
```

---

## Performance Benchmarks

| Operation | Time | Records |
|-----------|------|---------|
| Full 50-state ingestion | ~5 min | 72,000 EIA + 78,000 synth |
| DuckDB load | ~10 sec | 150,000 records |
| dbt full run | ~15 sec | All models |
| Report generation | ~30 sec | Including plots |
| **Total pipeline** | **~6 min** | **End-to-end** |

---

## Future Enhancements

1. **Data Quality Framework** - Great Expectations integration
2. **Incremental Loading** - Only process new/changed records
3. **CDC Pipeline** - Real-time updates when sources support it
4. **ML Features** - Price forecasting models
5. **Data Catalog** - Automated documentation with OpenMetadata
6. **Cost Allocation** - Track compute costs per asset