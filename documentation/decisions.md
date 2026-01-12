# Technical Decisions

This document outlines the key technical decisions made during the implementation of the Dakota Analytics Energy Data Pipeline, including the rationale behind each choice and alternatives considered.

---

## 1. Orchestration: Dagster

### Decision
Selected **Dagster** as the orchestration framework instead of Apache Airflow.

### Rationale
- **Asset-centric approach**: Dagster's software-defined assets align perfectly with data pipeline thinking - we define *what* data should exist rather than *how* to run tasks
- **Native dbt integration**: `dagster-dbt` provides first-class support for dbt models as assets
- **Learning**: As suggested by Jordan, dagster is a great open source solution for data architecture so wanted to explore that myself

---

## 2. Database: DuckDB over any over Cloud Db

### Decision
Used **DuckDB** as the analytical database instead of PostgreSQL or a cloud data warehouse.

### Rationale
- **Local-first development**: Works identically in development and production containers
- **dbt compatibility**: `dbt-duckdb` adapter works seamlessly
- **Cost efficiency**: No cloud costs for a technical assessment

---

## 3. API Design: FastAPI with GraphQL

### Decision
Built the synthetic enrichment API using **FastAPI** with **Strawberry GraphQL**, offering GraphQL interfaces.

### Rationale
- **FastAPI performance**: Async support and automatic OpenAPI documentation
- **GraphQL flexibility**: Clients can request exactly the fields they need
- **Type safety**: Pydantic models + Strawberry provide end-to-end type checking
- **Learning**: While having a discussion with Jordan, the topic of utility of graphql came up so thought building something with it would be something interesting.

---

## 4. Transformation Layer: dbt

### Decision
Used **dbt (data build tool)** for SQL transformations with a staging → intermediate → final model structure.

### Rationale
- **SQL-first**: Transformations in SQL are readable and maintainable
- **Modularity**: Clear separation between cleaning (staging), business logic (intermediate), and analytics (final)

### Model Structure
```
staging/          → 1:1 with source tables, type casting, renaming
intermediate/     → Business logic, joins, calculations
final/            → Analytics-ready, optimized for BI tools
```
---

## 5. Containerization Strategy

### Decision
Used **Docker Compose** with separate containers for the API and Dagster orchestration, sharing a network.

### Rationale
- **Isolation**: Each service has its own dependencies
- **Reproducibility**: `docker compose up` works identically everywhere
- **Service discovery**: Containers communicate via DNS names (`synthetic_api:8000`)
- **Single-command startup**: `run.bat`/`run.sh` handles everything

### Architecture
```
Look into architecture.md for more information
```

---

## 6. Error Handling Strategy

### Decision
Implemented **three layers of error handling**: retries, validation checks, and graceful degradation.

### Implementation
1. **Dagster Retry Policies**: Assets retry 3x with 60-second delays
2. **Asset Checks**: Validate data after ingestion (e.g., state coverage)
3. **Graceful Degradation**: Missing API keys log warnings, don't crash
```

