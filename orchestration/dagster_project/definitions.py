from dagster import Definitions, load_assets_from_modules
from .assets import energy_assets
from .sensors import api_health_sensor, ingestion_job

all_assets = load_assets_from_modules([energy_assets])

defs = Definitions(
    assets=all_assets,
    jobs=[ingestion_job],
    sensors=[api_health_sensor],
)