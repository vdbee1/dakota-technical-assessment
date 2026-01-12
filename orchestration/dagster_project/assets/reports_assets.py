import os
from dagstermill import define_dagstermill_asset
from dagster import AssetKey, Config

# Support both local development and Docker paths
def get_reports_dir():
    # Docker path
    if os.path.exists("/reports"):
        return "/reports"
    # Local development path
    CURRENT_FILE_PATH = os.path.abspath(__file__)
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_FILE_PATH))))
    return os.path.join(BASE_DIR, "reports")

REPORTS_DIR = get_reports_dir()
TEMPLATE_PATH = os.path.join(REPORTS_DIR, "energy_analysis_report.ipynb")

class NotebookConfig(Config):
    database_path: str = "/database/energy_data.duckdb" if os.path.exists("/database") else "database/energy_data.duckdb"

interactive_plotly_report = define_dagstermill_asset(
    name="interactive_plotly_report",
    notebook_path=TEMPLATE_PATH,
    group_name="reports",
    deps=[AssetKey(["monthly_state_trends"])], # Ensures dbt runs first
    config_schema=NotebookConfig 
)
