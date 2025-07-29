"""
SEPE Unemployment Data Assets
Dagster assets for extracting raw SEPE unemployment XLS files
"""

import os
import pandas as pd
from dagster import asset, AssetExecutionContext, get_dagster_logger, Output
from typing import List, Dict

from ..utils.sepe_scraper import SepeScraper
from ..utils.sepe_data_cleaner import SepeDataCleaner


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


@asset(
    description="Clean SEPE XLS files and convert to organized CSV files",
    group_name="sepe_etl",
    deps=[sepe_raw_xls_files]
)
def sepe_clean_data(context: AssetExecutionContext) -> Output[Dict]:
    """
    Clean and process SEPE XLS files into organized CSV files
    
    This asset processes all SEPE XLS files and creates separate CSV files
    for each province and date combination:
    - unemployment/[PROVINCE]_[YEAR]_[MONTH]_unemployment.csv
    - contracts/[PROVINCE]_[YEAR]_[MONTH]_contracts.csv
    
    Returns:
        Output containing processing statistics and file paths
    """
    logger = get_dagster_logger()
    
    # Check for force reprocess configuration and performance settings
    config = context.op_config or {}
    force_reprocess = config.get("force_reprocess", False)
    max_workers = config.get("max_workers", 4)  # Allow configurable parallelism
    
    # Initialize the optimized data cleaner
    cleaner = SepeDataCleaner(
        input_dir="/opt/dagster/raw/sepe",
        output_dir="/opt/dagster/clean/sepe",
        force_reprocess=force_reprocess,
        max_workers=max_workers
    )
    
    logger.info(f"Starting optimized SEPE processing:")
    logger.info(f"  Force reprocess: {force_reprocess}")
    logger.info(f"  Max workers: {max_workers}")
    logger.info(f"  Processing mode: {'Overwrite existing files' if force_reprocess else 'Skip existing files'}")
    
    logger.info("Starting SEPE data cleaning and CSV generation")
    
    try:
        # Process all files and create organized CSV files
        saved_files = cleaner.clean_all_files()
        
        # Calculate statistics
        total_files = sum(len(files) for files in saved_files.values())
        
        # Calculate statistics
        xls_files = list(cleaner.input_dir.glob("*.xls"))
        
        # Count how many files were actually processed vs skipped
        processed_count = 0
        skipped_count = 0
        
        for file_path in xls_files:
            if not force_reprocess and cleaner.check_file_already_processed(file_path):
                skipped_count += 1
            else:
                processed_count += 1
        
        # Enhanced summary statistics with performance metrics
        processing_rate = processed_count / max(1, len(xls_files)) * 100
        
        summary = {
            'total_xls_files': len(xls_files),
            'files_processed': processed_count,
            'files_skipped': skipped_count,
            'processing_rate_percent': round(processing_rate, 1),
            'csv_files_managed': total_files,
            'unemployment_files': len(saved_files.get('unemployment', [])),
            'contracts_files': len(saved_files.get('contracts', [])),
            'output_directory': str(cleaner.output_dir),
            'file_organization': 'Consolidated monthly files: [YEAR]_[MONTH]_[DATA_TYPE].csv',
            'optimization_features': [
                'Parallel file processing',
                'Optimized Excel reading engines', 
                'Cached format detection',
                'Vectorized data operations',
                'Memory-efficient batch processing'
            ],
            'max_workers': max_workers,
            'force_reprocess': force_reprocess
        }
        
        logger.info(f"\n=== Optimized SEPE Processing Completed ===")
        logger.info(f"XLS files processed: {summary['files_processed']}/{summary['total_xls_files']} ({summary['processing_rate_percent']}%)")
        logger.info(f"Files skipped (already processed): {summary['files_skipped']}")
        logger.info(f"Consolidated CSV files created: {summary['csv_files_managed']}")
        logger.info(f"  → Unemployment files: {summary['unemployment_files']}")
        logger.info(f"  → Contracts files: {summary['contracts_files']}")
        logger.info(f"Parallel workers used: {summary['max_workers']}")
        logger.info(f"Performance optimizations active: {len(summary['optimization_features'])}")
        
        return Output(
            summary,
            metadata={
                "total_csv_files": summary['csv_files_managed'],
                "xls_files_processed": summary['files_processed'],
                "xls_files_skipped": summary['files_skipped'],
                "unemployment_files": summary['unemployment_files'],
                "contracts_files": summary['contracts_files'],
                "processing_status": "success",
                "output_organization": "consolidated_monthly_files", 
                "processing_rate_percent": summary['processing_rate_percent'],
                "max_workers": summary['max_workers'],
                "optimizations_enabled": len(summary['optimization_features']),
                "force_reprocess": force_reprocess
            }
        )
        
    except Exception as e:
        logger.error(f"Failed to clean SEPE data: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise


@asset(
    description="Generate SEPE data processing summary report",
    group_name="sepe_etl", 
    deps=[sepe_clean_data]
)
def sepe_data_summary(context: AssetExecutionContext) -> Output[Dict]:
    """
    Generate a summary report of all cleaned SEPE data
    
    Creates a comprehensive overview of all processed SEPE data including:
    - File counts by province and data type
    - Date coverage analysis
    - Data quality metrics
    
    Returns:
        Output containing comprehensive data summary
    """
    logger = get_dagster_logger()
    
    import glob
    from pathlib import Path
    
    clean_dir = Path("/opt/dagster/clean/sepe")
    
    logger.info("Generating SEPE data summary report")
    
    try:
        # Find all consolidated CSV files
        unemployment_files = list(clean_dir.glob("*_unemployment.csv"))
        contracts_files = list(clean_dir.glob("*_contracts.csv"))
        
        # Analyze file patterns
        provinces = set()
        date_coverage = set()
        
        for file_path in unemployment_files + contracts_files:
            # Parse filename: YEAR_MONTH_TYPE.csv
            parts = file_path.stem.split('_')
            if len(parts) >= 3:
                year = parts[0]
                month = parts[1] 
                date_coverage.add(f"{year}-{month}")
                
                # Count provinces from actual file content
                if file_path.suffix == '.csv':
                    try:
                        sample_df = pd.read_csv(file_path, nrows=100)  # Just sample to count provinces
                        if 'province' in sample_df.columns:
                            file_provinces = sample_df['province'].unique()
                            provinces.update(file_provinces)
                    except Exception:
                        pass  # Skip if can't read file
        
        # Sample data analysis from a few files
        sample_analysis = {}
        if unemployment_files:
            sample_file = unemployment_files[0]
            sample_df = pd.read_csv(sample_file)
            sample_analysis = {
                'sample_file': sample_file.name,
                'sample_records': len(sample_df),
                'sample_columns': list(sample_df.columns),
                'municipalities_in_sample': sample_df['municipality_code'].nunique()
            }
        
        summary_report = {
            'total_provinces': len(provinces),
            'provinces_list': sorted(list(provinces)),
            'date_coverage_months': len(date_coverage),
            'date_range': sorted(list(date_coverage)),
            'unemployment_files_count': len(unemployment_files),
            'contracts_files_count': len(contracts_files),
            'total_files': len(unemployment_files) + len(contracts_files),
            'sample_analysis': sample_analysis,
            'data_organization': {
                'output_dir': str(clean_dir),
                'filename_pattern': "[YEAR]_[MONTH]_[DATA_TYPE].csv",
                'format': 'Consolidated monthly files with province as column'
            }
        }
        
        logger.info(f"SEPE Summary Report:")
        logger.info(f"  Total provinces: {summary_report['total_provinces']}")
        logger.info(f"  Date coverage: {summary_report['date_coverage_months']} months")
        logger.info(f"  Total CSV files: {summary_report['total_files']}")
        logger.info(f"  Date range: {summary_report['date_range'][0] if summary_report['date_range'] else 'N/A'} to {summary_report['date_range'][-1] if summary_report['date_range'] else 'N/A'}")
        
        return Output(
            summary_report,
            metadata={
                "provinces_processed": summary_report['total_provinces'],
                "months_covered": summary_report['date_coverage_months'], 
                "total_csv_files": summary_report['total_files'],
                "processing_quality": "organized_by_province_and_date"
            }
        )
        
    except Exception as e:
        logger.error(f"Failed to generate SEPE summary: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise