# Database Design: 3-Table Normalized Star Schema

## Rationale
I chose a **3-table Normalized Star Schema** to provide strict isolation between data sources. This ensures that the EIA commercial data and the Synthetic environmental metrics can be ingested and updated independently without cross-contaminating the datasets.

## ER Diagram (Source-Separated)
```mermaid
erDiagram
    dim_states ||--o{ fct_eia_energy : "links to"
    dim_states ||--o{ fct_synth_metrics : "links to"
    
    dim_states {
        string stateid PK "Primary Key (e.g., WY)"
        string state_name "Full State Name"
        string census_region "Geographic Region"
        string market_type "Deregulated vs Regulated"
    }

    fct_eia_energy {
        string period PK "Year-Month (YYYY-MM)"
        string stateid PK, FK "State Identifier"
        double price "Residential Price (cents/kWh)"
        double sales "Total Sales (MWh)"
        string sectorid "Sector Code (RES)"
        timestamp ingested_at "Audit: Ingestion Timestamp"
    }

    fct_synth_metrics {
        string period PK "Year-Month (YYYY-MM)"
        string stateid PK, FK "State Identifier"
        double peak_share "Peak Load Share"
        double weather_index "Normalized Weather Severity"
        double grid_stability_index "Grid Reliability Score"
        double renewable_share "Renewable Energy Mix %"
        double congestion_premium "Market Congestion Cost"
        double fuel_cost_index "Fuel Price Sensitivity"
        double volatility_score "Price Volatility Index"
        double carbon_intensity "CO2 Intensity (lb/MWh)"
        timestamp ingested_at "Audit: Ingestion Timestamp"
    }