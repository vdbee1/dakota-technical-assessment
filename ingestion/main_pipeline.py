import os
import json
import argparse
from dotenv import load_dotenv

# Importing your specific task modules
from tasks.fetch_eia import ingest_eia_for_state
from tasks.fetch_synth import ingest_synth_for_state
from utils import logger

load_dotenv()

def save_to_staging(data, filename):
    """
    Saves records to the ingestion/staging directory using absolute paths
    to ensure Dagster doesn't save them in the wrong folder.
    """
    # Force the path to be inside the 'ingestion/staging' folder
    base_dir = os.path.dirname(os.path.abspath(__file__))
    staging_dir = os.path.join(base_dir, "staging")
    
    os.makedirs(staging_dir, exist_ok=True)
    path = os.path.join(staging_dir, filename)
    
    is_update = os.path.exists(path)
    action_str = "UPDATED" if is_update else "CREATED"
    
    try:
        with open(path, "w") as f:
            json.dump(data, f, indent=4)
        logger.info(f"FILE {action_str}: {path} | Total Records: {len(data)}")
    except Exception as e:
        logger.error(f"Failed to write to {path}: {e}")

def run_ingestion_pipeline(state: str, months: int, api_key: str = None):
    """
    Orchestrates the ingestion for a single state.
    """
    eia_key = api_key or os.getenv("EIA_API_KEY")
    
    if not eia_key:
        logger.error(f"EIA_API_KEY missing for {state}. Skipping.")
        return {"eia": [], "synth": []}

    logger.info(f">>> Processing State: {state} | Window: {months} months")

    # 1. Fetch EIA Data
    eia_data = ingest_eia_for_state(eia_key, state, length=months)
    
    # 2. Fetch Synthetic Data 
    # Logic: Match the month count to exactly what was requested
    synth_data = ingest_synth_for_state(state, months=months)

    return {
        "eia": eia_data or [],
        "synth": synth_data or []
    }

def main():
    parser = argparse.ArgumentParser(description="Energy Data Ingestion Pipeline")
    
    parser.add_argument("--states", nargs="+", default=["TX"], help="List of state codes")
    parser.add_argument("--months", type=int, default=12, help="Months of history")
    parser.add_argument("--api-key", type=str, help="Optional EIA API Key override")

    args = parser.parse_args()

    all_aggregated_eia = []
    all_aggregated_synth = []

    logger.info(f"--- Starting Bulk Ingestion for {len(args.states)} States ---")

    for state in args.states:
        try:
            result = run_ingestion_pipeline(state, args.months, args.api_key)
            
            if result["eia"]:
                all_aggregated_eia.extend(result["eia"])
            else:
                logger.warning(f"No EIA data found for {state}")

            if result["synth"]:
                all_aggregated_synth.extend(result["synth"])
            else:
                logger.warning(f"No Synthetic data found for {state}")
            
        except Exception as e:
            logger.error(f"Critical failure during state {state} ingestion: {e}")
            continue 

    # Save final aggregated files
    save_to_staging(all_aggregated_eia, "stg_eia_primary.json")
    save_to_staging(all_aggregated_synth, "stg_synth_enrichment.json")

    logger.info(f"--- Ingestion Complete | Total EIA: {len(all_aggregated_eia)} | Total Synth: {len(all_aggregated_synth)} ---")

if __name__ == "__main__":
    main()