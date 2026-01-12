import requests
from dagster import sensor, RunRequest, SkipReason, AssetSelection, define_asset_job
from .assets.energy_assets import raw_energy_data

# Define the Job (The 'Verb' of the pipeline)
ingestion_job = define_asset_job(
    "ingestion_job", 
    selection=AssetSelection.assets(raw_energy_data)
)

@sensor(job=ingestion_job)
def api_health_sensor():
    """Checks API availability before triggering a run."""
    try:
        resp = requests.get("http://localhost:8000/graphql", timeout=2)
        if resp.status_code in [200, 400]:
            yield RunRequest(run_key=None)
    except Exception:
        yield SkipReason("Synthetic API is offline. Skipping run to avoid connection errors.")