import os
from pathlib import Path
from dagster import Definitions, load_assets_from_modules, AssetExecutionContext
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets, DagsterDbtTranslator
from dagstermill import ConfigurableLocalOutputNotebookIOManager

# 1. Setup the dbt Project
DBT_PROJECT_DIR = Path(os.getenv("DBT_PROJECT_DIR", Path(__file__).joinpath("..", "..", "..", "dbt").resolve()))
dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR)
dbt_project.prepare_if_dev()

# 2. Define a Translator
class EnergyDbtTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props):
        return "energy_analysis"

# 3. Define the dbt Assets (This automatically loads stg_eia_energy, etc.)
@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=EnergyDbtTranslator()
)
def energy_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

# 4. Import modules (Removing the conflicting dbt_assets module)
from .assets import (
    energy_assets, 
    report_assets, 
    reports_assets,
    database_assets
)
from . import sensors, schedules

# 5. Load assets
all_assets = [
    energy_dbt_assets,
    *load_assets_from_modules([energy_assets]),
    *load_assets_from_modules([report_assets]),
    *load_assets_from_modules([reports_assets]),
    *load_assets_from_modules([database_assets]),
    # We REMOVED load_assets_from_modules([dbt_python_assets]) to fix the duplicate error
]

# 6. Final Definitions
defs = Definitions(
    assets=all_assets,
    asset_checks=[energy_assets.check_state_coverage],
    jobs=[sensors.ingestion_job],
    sensors=[sensors.api_health_sensor],
    schedules=[schedules.daily_ingestion_schedule],
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(DBT_PROJECT_DIR)),
        "output_notebook_io_manager": ConfigurableLocalOutputNotebookIOManager(),
    },
)