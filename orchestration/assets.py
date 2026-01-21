
"""
Dagster Assets for Medical Telegram Data Pipeline
This module defines the data pipeline assets for orchestrating:
- Telegram data scraping
- Data loading to PostgreSQL
- dbt transformations
- YOLO object detection enrichment
"""

import os
import subprocess
from pathlib import Path
from datetime import datetime
from dagster import asset, AssetExecutionContext, Output, MetadataValue
import sys

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


@asset(
    description="Scrape messages and images from Telegram channels",
    group_name="extract"
)
def telegram_scraper(context: AssetExecutionContext) -> Output[dict]:
    """
    Execute the Telegram scraper to collect raw messages and images.
    """
    from src.scraper import main as scraper_main
    
    context.log.info("Starting Telegram data scraping...")
    
    try:
        # Run the scraper
        # Note: scraper.py needs to be modified to return stats
        # For now, we'll call it as a subprocess
        result = subprocess.run(
            [sys.executable, str(project_root / "src" / "scraper.py")],
            capture_output=True,
            text=True,
            timeout=600  # 10 minutes timeout
        )
        
        if result.returncode != 0:
            context.log.error(f"Scraper failed: {result.stderr}")
            raise Exception(f"Scraper failed with return code {result.returncode}")
        
        context.log.info("Telegram scraping completed successfully")
        
        # Count scraped files
        raw_data_path = project_root / "data" / "raw" / "telegram_messages"
        image_path = project_root / "data" / "raw" / "images"
        
        message_files = list(raw_data_path.rglob("*.json")) if raw_data_path.exists() else []
        image_files = list(image_path.rglob("*.jpg")) if image_path.exists() else []
        
        metadata = {
            "message_files": len(message_files),
            "image_files": len(image_files),
            "timestamp": datetime.now().isoformat(),
        }
        
        return Output(
            value=metadata,
            metadata={
                "message_files": MetadataValue.int(len(message_files)),
                "image_files": MetadataValue.int(len(image_files)),
                "timestamp": MetadataValue.text(metadata["timestamp"]),
            }
        )
        
    except Exception as e:
        context.log.error(f"Error during scraping: {str(e)}")
        raise


@asset(
    description="Load raw JSON data into PostgreSQL staging tables",
    group_name="load",
    deps=[telegram_scraper]
)
def data_loader(context: AssetExecutionContext) -> Output[dict]:
    """
    Load scraped data from JSON files into PostgreSQL raw tables.
    """
    context.log.info("Starting data load to PostgreSQL...")
    
    try:
        result = subprocess.run(
            [sys.executable, str(project_root / "src" / "loader.py")],
            capture_output=True,
            text=True,
            timeout=300  # 5 minutes timeout
        )
        
        if result.returncode != 0:
            context.log.error(f"Loader failed: {result.stderr}")
            raise Exception(f"Loader failed with return code {result.returncode}")
        
        context.log.info("Data loading completed successfully")
        
        metadata = {
            "status": "success",
            "timestamp": datetime.now().isoformat(),
        }
        
        return Output(
            value=metadata,
            metadata={
                "status": MetadataValue.text("success"),
                "timestamp": MetadataValue.text(metadata["timestamp"]),
            }
        )
        
    except Exception as e:
        context.log.error(f"Error during data loading: {str(e)}")
        raise


@asset(
    description="Run dbt transformations to build the data warehouse",
    group_name="transform",
    deps=[data_loader]
)
def dbt_transform(context: AssetExecutionContext) -> Output[dict]:
    """
    Execute dbt models to transform raw data into dimensional model.
    """
    context.log.info("Starting dbt transformations...")
    
    dbt_project_dir = project_root / "medical_warehouse"
    
    try:
        # Run dbt deps first
        context.log.info("Running dbt deps...")
        subprocess.run(
            ["dbt", "deps"],
            cwd=dbt_project_dir,
            check=True,
            capture_output=True,
            text=True
        )
        
        # Run dbt run
        context.log.info("Running dbt run...")
        result = subprocess.run(
            ["dbt", "run"],
            cwd=dbt_project_dir,
            capture_output=True,
            text=True,
            timeout=600  # 10 minutes timeout
        )
        
        if result.returncode != 0:
            context.log.error(f"dbt run failed: {result.stderr}")
            raise Exception(f"dbt run failed with return code {result.returncode}")
        
        # Run dbt test
        context.log.info("Running dbt test...")
        test_result = subprocess.run(
            ["dbt", "test"],
            cwd=dbt_project_dir,
            capture_output=True,
            text=True,
            timeout=300
        )
        
        context.log.info("dbt transformations completed successfully")
        
        metadata = {
            "status": "success",
            "timestamp": datetime.now().isoformat(),
            "tests_passed": test_result.returncode == 0,
        }
        
        return Output(
            value=metadata,
            metadata={
                "status": MetadataValue.text("success"),
                "timestamp": MetadataValue.text(metadata["timestamp"]),
                "tests_passed": MetadataValue.bool(metadata["tests_passed"]),
            }
        )
        
    except Exception as e:
        context.log.error(f"Error during dbt transformations: {str(e)}")
        raise


@asset(
    description="Run YOLO object detection on downloaded images",
    group_name="enrich",
    deps=[telegram_scraper]
)
def yolo_enrichment(context: AssetExecutionContext) -> Output[dict]:
    """
    Execute YOLO object detection on scraped images.
    """
    context.log.info("Starting YOLO object detection...")
    
    try:
        result = subprocess.run(
            [sys.executable, str(project_root / "src" / "yolo_detect.py")],
            capture_output=True,
            text=True,
            timeout=1800  # 30 minutes timeout for image processing
        )
        
        if result.returncode != 0:
            context.log.error(f"YOLO detection failed: {result.stderr}")
            raise Exception(f"YOLO detection failed with return code {result.returncode}")
        
        context.log.info("YOLO object detection completed successfully")
        
        # Check for detection results
        detections_csv = project_root / "data" / "processed" / "image_detections.csv"
        
        metadata = {
            "status": "success",
            "timestamp": datetime.now().isoformat(),
            "detections_file_exists": detections_csv.exists(),
        }
        
        return Output(
            value=metadata,
            metadata={
                "status": MetadataValue.text("success"),
                "timestamp": MetadataValue.text(metadata["timestamp"]),
                "detections_file_exists": MetadataValue.bool(metadata["detections_file_exists"]),
            }
        )
        
    except Exception as e:
        context.log.error(f"Error during YOLO enrichment: {str(e)}")
        raise


@asset(
    description="Load YOLO detection results into data warehouse",
    group_name="enrich",
    deps=[yolo_enrichment, dbt_transform]
)
def load_detections(context: AssetExecutionContext) -> Output[dict]:
    """
    Load YOLO detection results into the data warehouse.
    """
    context.log.info("Loading detection results to warehouse...")
    
    try:
        result = subprocess.run(
            [sys.executable, str(project_root / "src" / "load_detections.py")],
            capture_output=True,
            text=True,
            timeout=300
        )
        
        if result.returncode != 0:
            context.log.error(f"Detection loading failed: {result.stderr}")
            raise Exception(f"Detection loading failed with return code {result.returncode}")
        
        context.log.info("Detection results loaded successfully")
        
        metadata = {
            "status": "success",
            "timestamp": datetime.now().isoformat(),
        }
        
        return Output(
            value=metadata,
            metadata={
                "status": MetadataValue.text("success"),
                "timestamp": MetadataValue.text(metadata["timestamp"]),
            }
        )
        
    except Exception as e:
        context.log.error(f"Error loading detection results: {str(e)}")
        raise
