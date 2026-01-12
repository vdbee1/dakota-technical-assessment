from dagster import ScheduleDefinition
from .sensors import ingestion_job

daily_ingestion_schedule = ScheduleDefinition(
    job=ingestion_job,
    cron_schedule="0 0 1 * *", 
)