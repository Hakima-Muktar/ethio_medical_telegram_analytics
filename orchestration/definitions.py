
"""
Dagster Definitions for Medical Telegram Data Pipeline
This module defines the Dagster repository with all assets, jobs, and schedules.
"""

from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    AssetSelection,
)

from orchestration.assets import (
    telegram_scraper,
    data_loader,
    dbt_transform,
    yolo_enrichment,
    load_detections,
)

# Define the full pipeline job that runs all assets
full_pipeline_job = define_asset_job(
    name="full_pipeline",
    description="Complete data pipeline from scraping to warehouse",
    selection=AssetSelection.all(),
)

# Define a job for just the extraction and loading
extract_load_job = define_asset_job(
    name="extract_and_load",
    description="Scrape data and load to PostgreSQL",
    selection=AssetSelection.groups("extract", "load"),
)

# Define a job for transformations only
transform_job = define_asset_job(
    name="transform_only",
    description="Run dbt transformations on existing data",
    selection=AssetSelection.groups("transform"),
)

# Define a job for enrichment only
enrich_job = define_asset_job(
    name="enrich_only",
    description="Run YOLO detection and load results",
    selection=AssetSelection.groups("enrich"),
)

# Schedule to run the full pipeline daily at 2 AM
daily_pipeline_schedule = ScheduleDefinition(
    job=full_pipeline_job,
    cron_schedule="0 2 * * *",  # Every day at 2 AM
    name="daily_full_pipeline",
    description="Run the complete pipeline daily at 2 AM",
)

# Schedule to run just transformations every 6 hours
transform_schedule = ScheduleDefinition(
    job=transform_job,
    cron_schedule="0 */6 * * *",  # Every 6 hours
    name="six_hourly_transform",
    description="Run dbt transformations every 6 hours",
)

# Define the Dagster repository
defs = Definitions(
    assets=[
        telegram_scraper,
        data_loader,
        dbt_transform,
        yolo_enrichment,
        load_detections,
    ],
    jobs=[
        full_pipeline_job,
        extract_load_job,
        transform_job,
        enrich_job,
    ],
    schedules=[
        daily_pipeline_schedule,
        transform_schedule,
    ],
)
