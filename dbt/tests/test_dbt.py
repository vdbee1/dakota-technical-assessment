"""
Test suite for dbt transformations.
These tests validate dbt model structure and SQL logic without running dbt.
"""
import sys
import os
import pytest
import tempfile
import shutil

# --- UNIT TESTS FOR DBT SQL LOGIC ---

class TestStagingModels:
    """Test staging model SQL logic."""
    
    def test_stg_eia_energy_sql_syntax(self):
        """Test that stg_eia_energy.sql has valid structure."""
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        model_path = os.path.join(project_root, 'models', 'staging', 'stg_eia_energy.sql')
        
        with open(model_path, 'r') as f:
            sql = f.read()
        
        # Verify key elements exist
        assert 'source' in sql.lower()
        assert 'select' in sql.lower()
        assert 'from' in sql.lower()
        assert 'period' in sql
        assert 'stateid' in sql or 'state_code' in sql
        assert 'price' in sql
        assert 'sales' in sql
    
    def test_stg_synth_metrics_sql_syntax(self):
        """Test that stg_synth_metrics.sql has valid structure."""
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        model_path = os.path.join(project_root, 'models', 'staging', 'stg_synth_metrics.sql')
        
        with open(model_path, 'r') as f:
            sql = f.read()
        
        # Verify key elements exist
        assert 'source' in sql.lower()
        assert 'select' in sql.lower()
        assert 'carbon_intensity' in sql
        assert 'weather_index' in sql
        assert 'renewable_share' in sql


class TestIntermediateModels:
    """Test intermediate model SQL logic."""
    
    def test_int_energy_enriched_joins(self):
        """Test that int_energy_enriched.sql has proper joins."""
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        model_path = os.path.join(project_root, 'models', 'intermediate', 'int_energy_enriched.sql')
        
        with open(model_path, 'r') as f:
            sql = f.read()
        
        # Verify join structure
        assert 'join' in sql.lower()
        assert 'stg_eia_energy' in sql
        assert 'stg_synth_metrics' in sql
        assert 'period' in sql
        assert 'state_code' in sql
    
    def test_int_energy_enriched_incremental(self):
        """Test that int_energy_enriched uses incremental strategy."""
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        model_path = os.path.join(project_root, 'models', 'intermediate', 'int_energy_enriched.sql')
        
        with open(model_path, 'r') as f:
            sql = f.read()
        
        # Verify incremental config
        assert 'incremental' in sql
        assert 'unique_key' in sql
        assert 'is_incremental()' in sql


class TestFinalModels:
    """Test final model SQL logic."""
    
    def test_monthly_state_trends_structure(self):
        """Test that monthly_state_trends.sql has correct structure."""
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        model_path = os.path.join(project_root, 'models', 'final', 'monthly_state_trends.sql')
        
        with open(model_path, 'r') as f:
            sql = f.read()
        
        # Verify structure
        assert 'int_energy_enriched' in sql
        assert 'dim_states' in sql
        assert 'state_name' in sql
        assert 'census_region' in sql
        assert 'market_type' in sql
    
    def test_monthly_state_trends_calculations(self):
        """Test that monthly_state_trends has calculated fields."""
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        model_path = os.path.join(project_root, 'models', 'final', 'monthly_state_trends.sql')
        
        with open(model_path, 'r') as f:
            sql = f.read()
        
        # Verify calculated fields
        assert 'price_to_carbon_ratio' in sql or 'case' in sql.lower()


class TestSourcesConfig:
    """Test dbt sources configuration."""
    
    def test_sources_yml_exists(self):
        """Test that sources.yml exists and is valid."""
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        sources_path = os.path.join(project_root, 'models', 'staging', 'sources.yml')
        
        assert os.path.exists(sources_path)
        
        import yaml
        with open(sources_path, 'r') as f:
            sources = yaml.safe_load(f)
        
        # Verify structure
        assert 'sources' in sources
        assert len(sources['sources']) > 0
        
        # Find our source
        source = sources['sources'][0]
        assert 'name' in source
        assert 'tables' in source


class TestSchemaConfig:
    """Test dbt schema configuration."""
    
    def test_final_schema_yml_exists(self):
        """Test that final schema.yml exists."""
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        schema_path = os.path.join(project_root, 'models', 'final', 'schema.yml')
        
        assert os.path.exists(schema_path)
        
        import yaml
        with open(schema_path, 'r') as f:
            schema = yaml.safe_load(f)
        
        # Verify models are defined
        assert 'models' in schema


class TestDbtProjectConfig:
    """Test dbt_project.yml configuration."""
    
    def test_dbt_project_yml_exists(self):
        """Test that dbt_project.yml exists and is valid."""
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        config_path = os.path.join(project_root, 'dbt_project.yml')
        
        assert os.path.exists(config_path)
        
        import yaml
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        # Verify required fields
        assert 'name' in config
        assert 'version' in config
        assert 'profile' in config
    
    def test_profiles_yml_exists(self):
        """Test that profiles.yml exists and is valid."""
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        profiles_path = os.path.join(project_root, 'profiles.yml')
        
        assert os.path.exists(profiles_path)
        
        import yaml
        with open(profiles_path, 'r') as f:
            profiles = yaml.safe_load(f)
        
        # Should have a profile for DuckDB
        assert len(profiles) > 0


class TestDbtPackages:
    """Test dbt package configuration."""
    
    def test_packages_yml_exists(self):
        """Test that packages.yml exists."""
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        packages_path = os.path.join(project_root, 'packages.yml')
        
        assert os.path.exists(packages_path)
        
        import yaml
        with open(packages_path, 'r') as f:
            packages = yaml.safe_load(f)
        
        # Should have packages defined
        assert 'packages' in packages


# --- SQL LOGIC TESTS WITH DUCKDB ---

class TestSQLLogic:
    """Test SQL transformation logic using DuckDB."""
    
    @pytest.fixture
    def test_db(self):
        """Create a test database with sample data."""
        import duckdb
        
        temp_file = tempfile.mktemp(suffix=".duckdb")
        con = duckdb.connect(temp_file)
        
        # Create schema
        con.execute("CREATE SCHEMA IF NOT EXISTS analytics;")
        
        # Create tables matching staging model output
        con.execute("""
            CREATE TABLE analytics.stg_eia_energy (
                period DATE,
                state_code VARCHAR(2),
                price_cents_kwh DOUBLE,
                sales_mwh DOUBLE,
                sectorid TEXT,
                ingested_at TIMESTAMP
            );
        """)
        
        con.execute("""
            CREATE TABLE analytics.stg_synth_metrics (
                period DATE,
                state_code VARCHAR(2),
                carbon_intensity DOUBLE,
                weather_index DOUBLE,
                grid_stability_index DOUBLE,
                renewable_share DOUBLE,
                volatility_score DOUBLE,
                ingested_at TIMESTAMP
            );
        """)
        
        con.execute("""
            CREATE TABLE analytics.dim_states (
                stateid VARCHAR(2),
                state_name TEXT,
                census_region TEXT,
                market_type TEXT
            );
        """)
        
        # Insert test data
        con.execute("""
            INSERT INTO analytics.stg_eia_energy VALUES
            ('2024-01-01', 'TX', 12.5, 1000.0, 'RES', NOW()),
            ('2024-01-01', 'CA', 18.3, 800.0, 'RES', NOW()),
            ('2024-02-01', 'TX', 13.0, 1100.0, 'RES', NOW());
        """)
        
        con.execute("""
            INSERT INTO analytics.stg_synth_metrics VALUES
            ('2024-01-01', 'TX', 450.0, 1.1, 0.95, 0.25, 35.0, NOW()),
            ('2024-01-01', 'CA', 300.0, 1.0, 0.92, 0.45, 28.0, NOW()),
            ('2024-02-01', 'TX', 460.0, 1.2, 0.94, 0.26, 38.0, NOW());
        """)
        
        con.execute("""
            INSERT INTO analytics.dim_states VALUES
            ('TX', 'Texas', 'South', 'deregulated'),
            ('CA', 'California', 'West', 'deregulated');
        """)
        
        yield con
        
        con.close()
        os.remove(temp_file)
    
    def test_join_logic(self, test_db):
        """Test that EIA and synth data join correctly."""
        result = test_db.execute("""
            SELECT 
                e.period,
                e.state_code,
                e.price_cents_kwh,
                s.carbon_intensity,
                s.renewable_share
            FROM analytics.stg_eia_energy e
            LEFT JOIN analytics.stg_synth_metrics s
                ON e.period = s.period 
                AND e.state_code = s.state_code
            ORDER BY e.period, e.state_code
        """).fetchall()
        
        # Should have 3 rows
        assert len(result) == 3
        
        # TX Jan should have carbon_intensity
        tx_jan = [r for r in result if r[1] == 'TX' and str(r[0]) == '2024-01-01'][0]
        assert tx_jan[3] == 450.0  # carbon_intensity
        assert tx_jan[4] == 0.25   # renewable_share
    
    def test_state_metadata_join(self, test_db):
        """Test that state metadata joins correctly."""
        result = test_db.execute("""
            SELECT 
                e.state_code,
                s.state_name,
                s.census_region,
                s.market_type
            FROM analytics.stg_eia_energy e
            LEFT JOIN analytics.dim_states s ON e.state_code = s.stateid
            GROUP BY e.state_code, s.state_name, s.census_region, s.market_type
        """).fetchall()
        
        # Should have TX and CA
        states = {r[0]: r for r in result}
        assert 'TX' in states
        assert 'CA' in states
        assert states['TX'][1] == 'Texas'
        assert states['CA'][2] == 'West'
    
    def test_calculated_fields(self, test_db):
        """Test calculated field logic."""
        result = test_db.execute("""
            SELECT 
                state_code,
                price_cents_kwh,
                carbon_intensity,
                CASE 
                    WHEN carbon_intensity > 0 THEN (price_cents_kwh / carbon_intensity) 
                    ELSE NULL 
                END as price_to_carbon_ratio
            FROM (
                SELECT 
                    e.state_code,
                    e.price_cents_kwh,
                    s.carbon_intensity
                FROM analytics.stg_eia_energy e
                LEFT JOIN analytics.stg_synth_metrics s
                    ON e.period = s.period AND e.state_code = s.state_code
            )
            WHERE carbon_intensity IS NOT NULL
        """).fetchall()
        
        # All rows should have calculated ratio
        for row in result:
            assert row[3] is not None
            assert row[3] > 0
            # Verify calculation: price / carbon_intensity
            expected = row[1] / row[2]
            assert abs(row[3] - expected) < 0.0001


class TestDataQuality:
    """Test data quality assumptions."""
    
    @pytest.fixture
    def sample_data(self):
        """Generate sample test data."""
        return {
            'eia': [
                {"period": "2024-01", "stateid": "TX", "price": 12.5, "sales": 1000.0},
                {"period": "2024-01", "stateid": "CA", "price": 18.3, "sales": 800.0},
            ],
            'synth': [
                {"period": "2024-01", "stateid": "TX", "carbon_intensity": 450.0, 
                 "weather_index": 1.1, "renewable_share": 0.25},
                {"period": "2024-01", "stateid": "CA", "carbon_intensity": 300.0, 
                 "weather_index": 1.0, "renewable_share": 0.45},
            ]
        }
    
    def test_period_format(self, sample_data):
        """Test that period format is consistent."""
        import re
        for record in sample_data['eia']:
            assert re.match(r'^\d{4}-\d{2}$', record['period'])
    
    def test_state_codes_uppercase(self, sample_data):
        """Test that state codes are uppercase."""
        for record in sample_data['eia']:
            assert record['stateid'].isupper()
            assert len(record['stateid']) == 2
    
    def test_numeric_ranges(self, sample_data):
        """Test that numeric values are in reasonable ranges."""
        for record in sample_data['synth']:
            assert 0 <= record['renewable_share'] <= 1.0
            assert 0.5 <= record['weather_index'] <= 2.0
            assert record['carbon_intensity'] >= 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
