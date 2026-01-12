import subprocess
import os
import json
from dagster import (
    asset, 
    Config,
    AssetCheckResult, 
    asset_check, 
    AssetCheckExecutionContext, 
    AssetExecutionContext, 
    RetryPolicy
)

# Support both local development and Docker paths
def get_ingestion_path():
    # Docker path
    if os.path.exists("/ingestion/main_pipeline.py"):
        return "/ingestion/main_pipeline.py"
    # Local development path
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "ingestion", "main_pipeline.py"))

def get_staging_dir():
    # Docker path
    if os.path.exists("/ingestion/staging"):
        return "/ingestion/staging"
    # Local development path
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "ingestion", "staging"))

class IngestionConfig(Config):
    states: list[str] = [
        "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", 
        "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", 
        "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", 
        "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", 
        "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
    ]
    months: int = 120

@asset(
    group_name="ingestion", 
    compute_kind="python",
    # Layer 2 Error Handling: Retry 3 times with a 1-minute delay if the script crashes
    retry_policy=RetryPolicy(max_retries=3, delay=60)
)
def raw_energy_data(context: AssetExecutionContext, config: IngestionConfig):
    """Triggers the bulk ingestion for all 50 states."""
    pipeline_path = get_ingestion_path()

    context.log.info(f"Looking for pipeline at: {pipeline_path}")
    
    if not os.path.exists(pipeline_path):
        raise Exception(f"File not found at: {pipeline_path}")

    cmd = ["python", pipeline_path, "--months", str(config.months), "--states"] + config.states
    
    context.log.info(f"Running command: {' '.join(cmd)}")
    
    # timeout=1200 ensures we don't hang forever on a slow API
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=1200)
    
    if result.returncode != 0:
        context.log.error(f"Pipeline StdErr: {result.stderr}")
        context.log.error(f"Pipeline StdOut: {result.stdout}")
        raise Exception(f"Pipeline Failed: {result.stderr}")

    context.log.info(f"Pipeline output: {result.stdout}")
    return {"total_states": len(config.states), "months": config.months}

@asset_check(asset=raw_energy_data)
def check_state_coverage(context: AssetCheckExecutionContext): 
    """Layer 3 Error Handling: Validates the actual data content."""
    staging_dir = get_staging_dir()
    path = os.path.join(staging_dir, "stg_eia_primary.json")
    
    context.log.info(f"Checking staging file at: {path}")
    
    if not os.path.exists(path):
        return AssetCheckResult(passed=False, description=f"Staging file missing at {path}")

    with open(path, 'r') as f:
        data = json.load(f)
    
    unique_states = len(set(d.get('stateid') for d in data if 'stateid' in d))
    
    # We pass if we get at least 1 out of 50 states
    passed = unique_states >= 1
    
    return AssetCheckResult(
        passed=passed,
        metadata={
            "states_found": unique_states,
            "expected_minimum": 1,
            "total_records": len(data)
        }
    )
