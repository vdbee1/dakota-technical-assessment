import json
import os
import pandas as pd
from datetime import datetime
from dagster import asset, AssetExecutionContext

@asset(
    deps=["raw_energy_data"], 
    group_name="reporting",
    compute_kind="pandas"
)
def ingestion_summary_report(context: AssetExecutionContext):
    """
    Generates a coverage report based on the EIA 'stateid' schema.
    """
    # 1. Path Setup
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Go up 3 levels to reach project root, then into ingestion/staging
    staging_dir = os.path.abspath(os.path.join(current_dir, "..", "..", "..", "ingestion", "staging"))
    
    eia_path = os.path.join(staging_dir, "stg_eia_primary.json")
    report_path = os.path.join(staging_dir, "ingestion_report.txt")

    # 2. Load Data
    if not os.path.exists(eia_path):
        raise Exception(f"EIA Staging file not found at {eia_path}")

    with open(eia_path, 'r') as f:
        eia_data = json.load(f)

    if not eia_data:
        context.log.error("EIA JSON file is empty!")
        return

    df = pd.DataFrame(eia_data)

    # 3. Analyze based on your specific 'stateid' key
    # We use .get() or check columns to prevent KeyErrors
    state_col = 'stateid' if 'stateid' in df.columns else None
    
    if state_col:
        unique_states = sorted(df[state_col].unique().tolist())
        state_count = len(unique_states)
    else:
        unique_states = []
        state_count = 0
        context.log.warning(f"Could not find 'stateid' column. Available: {df.columns.tolist()}")

    # 4. Check Period/Date Coverage
    date_range = f"{df['period'].min()} to {df['period'].max()}" if 'period' in df.columns else "Unknown"

    # 5. Build and Save the Text Report
    report_lines = [
        "==========================================",
        "      PRODUCTION INGESTION SUMMARY        ",
        f"      Run Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "==========================================",
        f"Total Records Ingested:   {len(df)}",
        f"Temporal Coverage:        {date_range}",
        f"Unique States Found:      {state_count} / 50",
        "------------------------------------------",
        "States List:",
        ", ".join(unique_states),
        "=========================================="
    ]
    
    report_text = "\n".join(report_lines)
    with open(report_path, "w") as f:
        f.write(report_text)

    # 6. Push Metadata to Dagster UI for easy viewing
    context.add_output_metadata(
        metadata={
            "total_records": len(df),
            "states_found": state_count,
            "missing_states": 50 - state_count,
            "report_location": report_path
        }
    )
    
    context.log.info(f"Report generated: {state_count} states captured.")