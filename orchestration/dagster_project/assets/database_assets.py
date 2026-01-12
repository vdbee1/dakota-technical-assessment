import duckdb
import os
import json
import time
from dagster import AssetExecutionContext, AssetOut, multi_asset, asset

# Support both local development and Docker paths
def get_db_path():
    # Docker path
    if os.path.exists("/database"):
        return "/database/energy_data.duckdb"
    # Local development path
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "database", "energy_data.duckdb"))

def get_staging_dir():
    # Docker path
    if os.path.exists("/ingestion/staging"):
        return "/ingestion/staging"
    # Local development path
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "ingestion", "staging"))

def connect_with_retry(db_path, max_retries=10, delay=2):
    """Connect to DuckDB with retry logic for lock conflicts."""
    for attempt in range(max_retries):
        try:
            con = duckdb.connect(db_path)
            return con
        except duckdb.IOException as e:
            if "lock" in str(e).lower() and attempt < max_retries - 1:
                time.sleep(delay)
                continue
            raise
    raise Exception(f"Could not connect to database after {max_retries} attempts")

@asset(group_name="database", compute_kind="duckdb")
def database_schema(context: AssetExecutionContext):
    """Initializes schema and seeds all 50 states for dbt joins."""
    db_path = get_db_path()
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    con = connect_with_retry(db_path)
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS analytics;")
        
        # 1. Dimension Table
        con.execute("""
            CREATE TABLE IF NOT EXISTS analytics.dim_states (
                stateid VARCHAR(2) PRIMARY KEY,
                state_name TEXT,
                census_region TEXT,
                market_type TEXT
            );
        """)

        # 2. 50-State Seed Data
        states_to_seed = [
            ('AL', 'Alabama', 'South', 'regulated'), ('AK', 'Alaska', 'West', 'regulated'),
            ('AZ', 'Arizona', 'West', 'regulated'), ('AR', 'Arkansas', 'South', 'regulated'),
            ('CA', 'California', 'West', 'deregulated'), ('CO', 'Colorado', 'West', 'regulated'),
            ('CT', 'Connecticut', 'Northeast', 'deregulated'), ('DE', 'Delaware', 'Northeast', 'deregulated'),
            ('DC', 'District of Columbia', 'Northeast', 'deregulated'), ('FL', 'Florida', 'Southeast', 'regulated'),
            ('GA', 'Georgia', 'Southeast', 'regulated'), ('HI', 'Hawaii', 'West', 'regulated'),
            ('ID', 'Idaho', 'West', 'regulated'), ('IL', 'Illinois', 'Midwest', 'deregulated'),
            ('IN', 'Indiana', 'Midwest', 'regulated'), ('IA', 'Iowa', 'Midwest', 'regulated'),
            ('KS', 'Kansas', 'Midwest', 'regulated'), ('KY', 'Kentucky', 'South', 'regulated'),
            ('LA', 'Louisiana', 'South', 'regulated'), ('ME', 'Maine', 'Northeast', 'deregulated'),
            ('MD', 'Maryland', 'Northeast', 'deregulated'), ('MA', 'Massachusetts', 'Northeast', 'deregulated'),
            ('MI', 'Michigan', 'Midwest', 'deregulated'), ('MN', 'Minnesota', 'Midwest', 'regulated'),
            ('MS', 'Mississippi', 'South', 'regulated'), ('MO', 'Missouri', 'Midwest', 'regulated'),
            ('MT', 'Montana', 'West', 'regulated'), ('NE', 'Nebraska', 'Midwest', 'regulated'),
            ('NV', 'Nevada', 'West', 'regulated'), ('NH', 'New Hampshire', 'Northeast', 'deregulated'),
            ('NJ', 'New Jersey', 'Northeast', 'deregulated'), ('NM', 'New Mexico', 'West', 'regulated'),
            ('NY', 'New York', 'Northeast', 'deregulated'), ('NC', 'North Carolina', 'Southeast', 'regulated'),
            ('ND', 'North Dakota', 'Midwest', 'regulated'), ('OH', 'Ohio', 'Midwest', 'deregulated'),
            ('OK', 'Oklahoma', 'South', 'regulated'), ('OR', 'Oregon', 'West', 'regulated'),
            ('PA', 'Pennsylvania', 'Northeast', 'deregulated'), ('RI', 'Rhode Island', 'Northeast', 'deregulated'),
            ('SC', 'South Carolina', 'Southeast', 'regulated'), ('SD', 'South Dakota', 'Midwest', 'regulated'),
            ('TN', 'Tennessee', 'South', 'regulated'), ('TX', 'Texas', 'South', 'deregulated'),
            ('UT', 'Utah', 'West', 'regulated'), ('VT', 'Vermont', 'Northeast', 'regulated'),
            ('VA', 'Virginia', 'South', 'regulated'), ('WA', 'Washington', 'West', 'regulated'),
            ('WV', 'West Virginia', 'South', 'regulated'), ('WI', 'Wisconsin', 'Midwest', 'regulated'),
            ('WY', 'Wyoming', 'West', 'regulated')
        ]
        
        for state in states_to_seed:
            con.execute("INSERT OR IGNORE INTO analytics.dim_states VALUES (?, ?, ?, ?)", state)

        # 3. Fact Tables
        con.execute("CREATE TABLE IF NOT EXISTS analytics.eia_energy (period TEXT, stateid VARCHAR(2), price DOUBLE, sales DOUBLE, sectorid TEXT, ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (period, stateid));")
        con.execute("CREATE TABLE IF NOT EXISTS analytics.synth_metrics (period TEXT, stateid VARCHAR(2), carbon_intensity DOUBLE, weather_index DOUBLE, grid_stability_index DOUBLE, renewable_share DOUBLE, volatility_score DOUBLE, ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (period, stateid));")
        
        context.log.info(f"Database initialized at: {db_path}")
    finally:
        con.close()
    
    return db_path

@multi_asset(
    outs={"eia_energy": AssetOut(), "synth_metrics": AssetOut()},
    deps=["database_schema", "raw_energy_data"],
    group_name="database", compute_kind="duckdb"
)
def loaded_energy_data(context: AssetExecutionContext):
    db_path = get_db_path()
    staging_dir = get_staging_dir()
    
    eia_file = os.path.join(staging_dir, 'stg_eia_primary.json')
    synth_file = os.path.join(staging_dir, 'stg_synth_enrichment.json')
    
    context.log.info(f"DB path: {db_path}")
    context.log.info(f"Staging dir: {staging_dir}")
    context.log.info(f"EIA file path: {eia_file}")
    context.log.info(f"Synth file path: {synth_file}")
    context.log.info(f"EIA file exists: {os.path.exists(eia_file)}")
    context.log.info(f"Synth file exists: {os.path.exists(synth_file)}")
    
    # Check if files exist and have content
    if not os.path.exists(eia_file):
        raise Exception(f"EIA staging file not found at: {eia_file}. Available files in staging: {os.listdir(staging_dir) if os.path.exists(staging_dir) else 'staging dir not found'}")
    
    if not os.path.exists(synth_file):
        raise Exception(f"Synth staging file not found at: {synth_file}")
    
    # Check file sizes
    eia_size = os.path.getsize(eia_file)
    synth_size = os.path.getsize(synth_file)
    context.log.info(f"EIA file size: {eia_size} bytes")
    context.log.info(f"Synth file size: {synth_size} bytes")
    
    if eia_size == 0:
        raise Exception("EIA staging file is empty!")
    if synth_size == 0:
        raise Exception("Synth staging file is empty!")
    
    # Load and check JSON structure
    with open(eia_file, 'r') as f:
        eia_data = json.load(f)
        context.log.info(f"EIA records count: {len(eia_data)}")
        if eia_data:
            context.log.info(f"EIA first record keys: {list(eia_data[0].keys())}")
    
    with open(synth_file, 'r') as f:
        synth_data = json.load(f)
        context.log.info(f"Synth records count: {len(synth_data)}")
        if synth_data:
            context.log.info(f"Synth first record keys: {list(synth_data[0].keys())}")
    
    # Connect with retry to handle lock conflicts
    context.log.info("Connecting to database (with retry for locks)...")
    con = connect_with_retry(db_path)
    
    try:
        # Load EIA data using Python data directly instead of read_json_auto
        context.log.info("Loading EIA data...")
        for record in eia_data:
            try:
                con.execute("""
                    INSERT OR REPLACE INTO analytics.eia_energy (period, stateid, price, sales, sectorid, ingested_at)
                    VALUES (?, ?, ?, ?, ?, now())
                """, [
                    record.get('period'),
                    record.get('stateid'),
                    float(record.get('price', 0)),
                    float(record.get('sales', 0)),
                    record.get('sectorid')
                ])
            except Exception as e:
                context.log.warning(f"Error inserting EIA record: {e}, record: {record}")
        
        # Load synthetic data
        context.log.info("Loading synth data...")
        for record in synth_data:
            try:
                con.execute("""
                    INSERT OR REPLACE INTO analytics.synth_metrics (period, stateid, carbon_intensity, weather_index, grid_stability_index, renewable_share, volatility_score, ingested_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, now())
                """, [
                    record.get('period'),
                    record.get('stateid'),
                    float(record.get('carbon_intensity', 0)),
                    float(record.get('weather_index', 0)),
                    float(record.get('grid_stability_index', 0)),
                    float(record.get('renewable_share', 0)),
                    float(record.get('volatility_score', 0))
                ])
            except Exception as e:
                context.log.warning(f"Error inserting synth record: {e}, record: {record}")
        
        context.log.info("Data loaded successfully")
    finally:
        con.close()
    
    return None, None