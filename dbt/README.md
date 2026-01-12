dbt Transformation Layer
Overview
This directory contains the Medallion Architecture implementation for the Energy Data Pipeline. We transform raw, source-specific data into high-performance analytical tables using DuckDB as the compute engine.

Architecture: Medallion Pattern
I chose the Medallion pattern to ensure data integrity and clear lineage:

Staging:

Models: stg_eia_energy, stg_synth_metrics

Logic: Performs "Schema-on-Read." We cast raw strings (e.g., price) to numeric types and handle date normalization (converting YYYY-MM to YYYY-MM-01).

Intermediate:

Models: int_energy_enriched

Logic: This is an Incremental model that joins EIA commercial data with Synthetic environmental data on period and state_code.

Why Incremental?: With 10 years of historical data for 50 states, rebuilding the entire table is inefficient. This model only processes new data records since the last run.

Final :

Models: monthly_state_trends

Logic: A "Wide Table" designed for BI tools. It joins dimensions (dim_states) with the enriched facts to provide a human-readable view including state names and regions.

Data Quality & Testing
Data integrity is enforced via dbt_utils and standard dbt tests:

Unique/Not Null: Applied to state_code and period to ensure time-series integrity.

Accepted Range: Ensures price_cents_kwh and carbon_intensity are non-negative.

Relationship Tests: Validates that all state_code entries in fact tables exist in the dim_states dimension table.

How to Run
Install dependencies:

Bash

dbt deps
Execute transformations:

Bash

dbt run
Run quality assertions:

Bash

dbt test