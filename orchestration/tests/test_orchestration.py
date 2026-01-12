"""
Test suite for Dagster orchestration assets.
These tests validate the pipeline components without requiring external APIs.
"""
import sys
import os
import pytest
import json
import tempfile
import shutil

# Add paths for both Docker and local environments
# Docker: /app/dagster_project/
# Local: ../dagster_project/
sys.path.insert(0, '/app')
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# --- UNIT TESTS (No external dependencies) ---

class TestPathHelpers:
    """Test the path helper functions used for Docker/local compatibility."""
    
    def test_get_db_path(self):
        """Test that db path is returned correctly."""
        from dagster_project.assets.database_assets import get_db_path
        
        db_path = get_db_path()
        # Should return a path ending with energy_data.duckdb
        assert db_path.endswith("energy_data.duckdb")
    
    def test_get_staging_dir(self):
        """Test that staging path is returned."""
        from dagster_project.assets.database_assets import get_staging_dir
        
        staging_dir = get_staging_dir()
        assert "staging" in staging_dir


class TestDatabaseSchema:
    """Test database schema creation and state seeding."""
    
    @pytest.fixture
    def temp_db_path(self):
        """Create a temporary database for testing."""
        temp_dir = tempfile.mkdtemp()
        db_path = os.path.join(temp_dir, "test_energy.duckdb")
        yield db_path
        shutil.rmtree(temp_dir)
    
    def test_schema_creation(self, temp_db_path):
        """Test that schema and tables can be created."""
        import duckdb
        
        con = duckdb.connect(temp_db_path)
        con.execute("CREATE SCHEMA IF NOT EXISTS analytics;")
        
        # Create dimension table
        con.execute("""
            CREATE TABLE IF NOT EXISTS analytics.dim_states (
                stateid VARCHAR(2) PRIMARY KEY,
                state_name TEXT,
                census_region TEXT,
                market_type TEXT
            );
        """)
        
        # Verify table exists
        result = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'analytics'").fetchall()
        table_names = [r[0] for r in result]
        assert 'dim_states' in table_names
        con.close()
    
    def test_state_seeding(self, temp_db_path):
        """Test that states are seeded correctly."""
        import duckdb
        
        con = duckdb.connect(temp_db_path)
        con.execute("CREATE SCHEMA IF NOT EXISTS analytics;")
        con.execute("""
            CREATE TABLE IF NOT EXISTS analytics.dim_states (
                stateid VARCHAR(2) PRIMARY KEY,
                state_name TEXT,
                census_region TEXT,
                market_type TEXT
            );
        """)
        
        # Seed test states
        test_states = [
            ('TX', 'Texas', 'South', 'deregulated'),
            ('CA', 'California', 'West', 'deregulated'),
            ('NY', 'New York', 'Northeast', 'deregulated'),
        ]
        
        for state in test_states:
            con.execute("INSERT OR IGNORE INTO analytics.dim_states VALUES (?, ?, ?, ?)", state)
        
        # Verify states exist
        result = con.execute("SELECT COUNT(*) FROM analytics.dim_states").fetchone()[0]
        assert result == 3
        
        # Verify specific state
        tx = con.execute("SELECT state_name, market_type FROM analytics.dim_states WHERE stateid = 'TX'").fetchone()
        assert tx[0] == 'Texas'
        assert tx[1] == 'deregulated'
        con.close()
    
    def test_fact_tables_creation(self, temp_db_path):
        """Test that fact tables are created with correct schema."""
        import duckdb
        
        con = duckdb.connect(temp_db_path)
        con.execute("CREATE SCHEMA IF NOT EXISTS analytics;")
        
        # Create EIA energy table
        con.execute("""
            CREATE TABLE IF NOT EXISTS analytics.eia_energy (
                period TEXT, 
                stateid VARCHAR(2), 
                price DOUBLE, 
                sales DOUBLE, 
                sectorid TEXT, 
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
                PRIMARY KEY (period, stateid)
            );
        """)
        
        # Create synth metrics table
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
                PRIMARY KEY (period, stateid)
            );
        """)
        
        # Verify tables exist
        result = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'analytics'").fetchall()
        table_names = [r[0] for r in result]
        assert 'eia_energy' in table_names
        assert 'synth_metrics' in table_names
        con.close()


class TestDataLoading:
    """Test data loading from staging files."""
    
    @pytest.fixture
    def temp_staging_dir(self):
        """Create temporary staging directory with test data."""
        temp_dir = tempfile.mkdtemp()
        
        # Create test EIA data
        eia_data = [
            {"period": "2024-01", "stateid": "TX", "price": "12.5", "sales": "1000.0", "sectorid": "RES"},
            {"period": "2024-01", "stateid": "CA", "price": "18.3", "sales": "800.0", "sectorid": "RES"},
        ]
        with open(os.path.join(temp_dir, "stg_eia_primary.json"), "w") as f:
            json.dump(eia_data, f)
        
        # Create test synth data
        synth_data = [
            {"period": "2024-01", "stateid": "TX", "carbon_intensity": 450.0, "weather_index": 1.1, 
             "grid_stability_index": 0.95, "renewable_share": 0.25, "volatility_score": 35.0},
            {"period": "2024-01", "stateid": "CA", "carbon_intensity": 300.0, "weather_index": 1.0, 
             "grid_stability_index": 0.92, "renewable_share": 0.45, "volatility_score": 28.0},
        ]
        with open(os.path.join(temp_dir, "stg_synth_enrichment.json"), "w") as f:
            json.dump(synth_data, f)
        
        yield temp_dir
        shutil.rmtree(temp_dir)
    
    def test_json_file_validation(self, temp_staging_dir):
        """Test that staging JSON files can be read and validated."""
        eia_file = os.path.join(temp_staging_dir, "stg_eia_primary.json")
        synth_file = os.path.join(temp_staging_dir, "stg_synth_enrichment.json")
        
        # Verify files exist
        assert os.path.exists(eia_file)
        assert os.path.exists(synth_file)
        
        # Verify files are not empty
        assert os.path.getsize(eia_file) > 0
        assert os.path.getsize(synth_file) > 0
        
        # Verify JSON structure
        with open(eia_file, 'r') as f:
            eia_data = json.load(f)
            assert len(eia_data) == 2
            assert 'period' in eia_data[0]
            assert 'stateid' in eia_data[0]
        
        with open(synth_file, 'r') as f:
            synth_data = json.load(f)
            assert len(synth_data) == 2
            assert 'carbon_intensity' in synth_data[0]
    
    def test_data_insertion(self, temp_staging_dir):
        """Test that data can be inserted from staging files."""
        import duckdb
        
        # Create temp database
        temp_db = tempfile.mktemp(suffix=".duckdb")
        con = duckdb.connect(temp_db)
        
        # Create schema and tables
        con.execute("CREATE SCHEMA IF NOT EXISTS analytics;")
        con.execute("""
            CREATE TABLE analytics.eia_energy (
                period TEXT, stateid VARCHAR(2), price DOUBLE, sales DOUBLE, 
                sectorid TEXT, ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (period, stateid)
            );
        """)
        
        # Load data from staging files
        eia_file = os.path.join(temp_staging_dir, "stg_eia_primary.json")
        with open(eia_file, 'r') as f:
            eia_data = json.load(f)
        
        for record in eia_data:
            con.execute("""
                INSERT OR REPLACE INTO analytics.eia_energy 
                (period, stateid, price, sales, sectorid, ingested_at)
                VALUES (?, ?, ?, ?, ?, now())
            """, [
                record.get('period'),
                record.get('stateid'),
                float(record.get('price', 0)),
                float(record.get('sales', 0)),
                record.get('sectorid')
            ])
        
        # Verify data was inserted
        result = con.execute("SELECT COUNT(*) FROM analytics.eia_energy").fetchone()[0]
        assert result == 2
        
        # Verify specific record
        tx_price = con.execute("SELECT price FROM analytics.eia_energy WHERE stateid = 'TX'").fetchone()[0]
        assert tx_price == 12.5
        
        con.close()
        os.remove(temp_db)


class TestAssetImports:
    """Test that asset modules can be imported."""
    
    def test_database_assets_import(self):
        """Test database_assets can be imported."""
        from dagster_project.assets import database_assets
        assert hasattr(database_assets, 'database_schema')
        assert hasattr(database_assets, 'loaded_energy_data')
        assert hasattr(database_assets, 'get_db_path')
        assert hasattr(database_assets, 'get_staging_dir')
    
    def test_energy_assets_import(self):
        """Test energy_assets can be imported."""
        from dagster_project.assets import energy_assets
        assert hasattr(energy_assets, 'raw_energy_data')
    
    def test_dbt_assets_import(self):
        """Test dbt_assets can be imported."""
        from dagster_project.assets import dbt_assets
        assert dbt_assets is not None


class TestDefinitionsLoad:
    """Test that Dagster definitions load correctly."""
    
    def test_definitions_import(self):
        """Test that definitions module can be imported."""
        from dagster_project.definitions import defs
        assert defs is not None
    
    def test_assets_registered(self):
        """Test that assets are registered in definitions."""
        from dagster_project.definitions import defs
        
        assets = list(defs.get_asset_graph().all_asset_keys)
        asset_names = [str(a) for a in assets]
        
        # Should have core assets
        assert any('database_schema' in name for name in asset_names)


class TestConfigModels:
    """Test configuration models."""
    
    def test_ingestion_config_defaults(self):
        """Test that IngestionConfig has correct defaults."""
        from dagster_project.assets.energy_assets import IngestionConfig
        
        config = IngestionConfig()
        
        # Should have 50 states
        assert len(config.states) == 50
        assert 'TX' in config.states
        assert 'CA' in config.states
        
        # Should have default months
        assert config.months == 120


class TestSchedules:
    """Test schedule definitions."""
    
    def test_schedule_exists(self):
        """Test that monthly schedule is defined."""
        from dagster_project.schedules import daily_ingestion_schedule
        
        assert daily_ingestion_schedule is not None
        # Cron should be monthly (1st of month)
        assert daily_ingestion_schedule.cron_schedule == "0 0 1 * *"


class TestSensors:
    """Test sensor definitions."""
    
    def test_sensors_import(self):
        """Test that sensors can be imported."""
        from dagster_project.sensors import ingestion_job
        assert ingestion_job is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
