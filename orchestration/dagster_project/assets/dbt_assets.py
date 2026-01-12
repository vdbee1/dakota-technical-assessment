from dagster_dbt import DbtCliResource, dbt_assets
from dagster import AssetExecutionContext, file_relative_path
import os

# Support both local development and Docker paths
def get_dbt_project_dir():
    # Docker path
    if os.path.exists("/dbt/dbt_project.yml"):
        return "/dbt"
    # Local development path
    return os.path.abspath(file_relative_path(__file__, "../../../dbt"))

DBT_PROJECT_DIR = get_dbt_project_dir()

dbt_resource = DbtCliResource(project_dir=DBT_PROJECT_DIR)

# This single function loads ALL your dbt models (staging, intermediate, final) 
@dbt_assets(manifest=os.path.join(DBT_PROJECT_DIR, "target", "manifest.json"))
def energy_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).get_artifacts()
