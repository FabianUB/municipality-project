"""
SEPE Unemployment Data Assets
Dagster assets for extracting raw SEPE unemployment XLS files
"""

import os
import pandas as pd
from dagster import asset, AssetExecutionContext, get_dagster_logger
from typing import List

from ..utils.sepe_scraper import SepeScraper


@asset(
    description="Extract raw XLS files from SEPE website",
    group_name="sepe_etl"
)
def sepe_raw_xls_files(context: AssetExecutionContext) -> List[str]:
    """
    Extract raw unemployment XLS files from SEPE website
    Downloads the "Libro completo" files without processing them
    """
    logger = get_dagster_logger()
    
    download_dir = "/opt/dagster/raw/sepe"
    
    # Check if files already exist
    import glob
    existing_files = glob.glob(f"{download_dir}/*.xls*")
    
    if existing_files:
        logger.info(f"SEPE files already exist: {len(existing_files)} files found")
        downloaded_files = existing_files
    else:
        # Initialize scraper with mapped volume path
        scraper = SepeScraper(download_dir=download_dir)
        
        # Get the latest available data first
        logger.info("Attempting to download latest SEPE unemployment XLS file")
        latest_file = scraper.get_latest_data()
        
        downloaded_files = []
        if latest_file:
            downloaded_files.append(latest_file)
            logger.info(f"Downloaded latest file: {latest_file}")
        
        # Download all available historical data
        logger.info("Downloading all available XLS files from SEPE")
        historical_files = scraper.scrape_all_available_data(
            years=None,  # Download all available years
            max_files=None  # No limit on files
        )
        
        downloaded_files.extend(historical_files)
    
    # Remove duplicates
    downloaded_files = list(set(downloaded_files))
    
    logger.info(f"Total XLS files downloaded: {len(downloaded_files)}")
    return downloaded_files


@asset(
    description="SEPE raw files inventory",
    group_name="sepe_etl",
    deps=[sepe_raw_xls_files]
)
def sepe_files_inventory(context: AssetExecutionContext) -> dict:
    """
    Create an inventory of downloaded SEPE XLS files
    """
    logger = get_dagster_logger()
    
    import glob
    
    # Get all SEPE files from mapped volume path
    xls_files = glob.glob("/opt/dagster/raw/sepe/*.xls") + glob.glob("/opt/dagster/raw/sepe/*.xlsx")
    
    inventory = {
        'total_files': len(xls_files),
        'files': []
    }
    
    for file_path in xls_files:
        file_info = {
            'filename': os.path.basename(file_path),
            'file_path': file_path,
            'size_mb': round(os.path.getsize(file_path) / (1024 * 1024), 2),
            'modified_date': os.path.getmtime(file_path)
        }
        inventory['files'].append(file_info)
    
    logger.info(f"SEPE files inventory: {inventory['total_files']} files found")
    return inventory