import duckdb
import os

def initialize_db():
    db_path = os.path.join(os.path.dirname(__file__), '..', 'energy_data.duckdb')
    con = duckdb.connect(db_path)

    # 1. Create Analytics Schema
    con.execute("CREATE SCHEMA IF NOT EXISTS analytics;")

    # 2. Table 1: Dimension - States Metadata
    con.execute("""
        CREATE TABLE IF NOT EXISTS analytics.dim_states (
            stateid VARCHAR(2) PRIMARY KEY,
            state_name TEXT,
            census_region TEXT,
            market_type TEXT   -- 'deregulated' vs 'regulated'
        );
    """)

    # 3. Table 2: Fact - EIA Historical Data
    con.execute("""
        CREATE TABLE IF NOT EXISTS analytics.eia_energy (
            period TEXT,
            stateid VARCHAR(2),
            price DOUBLE,
            sales DOUBLE,
            sectorid TEXT,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (period, stateid),
            FOREIGN KEY (stateid) REFERENCES dim_states(stateid)
        );
    """)

    # 4. Table 3: Fact - Synthetic Enrichment Data
    con.execute("""
        CREATE TABLE IF NOT EXISTS analytics.synth_metrics (
            period TEXT,
            stateid VARCHAR(2),
            carbon_intensity DOUBLE,
            weather_index DOUBLE,
            grid_stability_index DOUBLE,
            renewable_share DOUBLE,
            volatility_score DOUBLE,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (period, stateid),
            FOREIGN KEY (stateid) REFERENCES dim_states(stateid)
        );
    """)

    print(f"3-Table Normalized Schema initialized at {db_path}")
    con.close()

if __name__ == "__main__":
    initialize_db()