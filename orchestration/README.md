# ⚡ Energy Data Orchestration Layer

This directory contains the **Dagster** orchestration logic for the 50-state, 10-year energy data ingestion pipeline. It serves as the "brain" of the project, managing execution flow, data quality, and system health.

## 🏗️ Architecture Overview

The orchestration is built using a **Medallion-lite Architecture**, focusing on the automated transition from Raw API sources to validated Staging files.



### Key Components:
1.  **Assets (`energy_assets.py`)**: Defines the data entities.
    * `raw_energy_data`: A Python-based asset that executes the bulk ingestion via a subprocess call to the main pipeline.
2.  **Asset Checks**: A data quality layer that validates the output of the ingestion (e.g., ensuring 50 states are present) before downstream processing.
3.  **Reporting (`report_assets.py`)**: An automated post-ingestion consumer that generates a human-readable `ingestion_report.txt` summarizing the data coverage.
4.  **Sensors (`sensors.py`)**: An "API Health Guard" that pings the local Synthetic GraphQL API to ensure services are online before launching a run.
5.  **Schedules (`schedules.py`)**: Defines the automated daily cadence for data refreshes.

---

##  Error Handling Strategy

This project implements a **Triple-Layer Defense** to ensure data integrity:

| Layer | Component | Purpose |
| :--- | :--- | :--- |
| **Layer 1: Pre-Run** | `api_health_sensor` | Prevents execution if the Synthetic API (Docker) is unreachable. |
| **Layer 2: Execution** | `RetryPolicy` | Automatically retries the Python ingestion 3x with a 60s delay on network blips. |
| **Layer 3: Post-Run** | `Asset Checks` | Validates that at least 45/50 states were successfully captured in the JSON. |

---

## 🚀 Getting Started

### 1. Environment Setup
Ensure your `.env` file in the root directory contains your `EIA_API_KEY`.

### 2. Launching the UI
From the `orchestration/` directory, run:

dagster dev -m dagster_project

### 3. Running the Pipeline
Navigate to localhost:3000.

Go to the Assets tab.

Click Materialize All.

Observe the real-time logs and the successful Asset Check status.

### Outputs
Staging Data: ingestion/staging/stg_eia_primary.json

Staging Data: ingestion/staging/stg_synth_enrichment.json

Automated Report: ingestion/staging/ingestion_report.txt
```bash