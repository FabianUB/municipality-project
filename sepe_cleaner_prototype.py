#!/usr/bin/env python3
"""
SEPE XLS File Cleaner - Prototype
Extracts unemployment and contract data from complex multi-sheet SEPE Excel files
"""

import pandas as pd
import os
import re
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SepeDataCleaner:
    """
    Cleans and processes SEPE unemployment and contract data from Excel files
    """
    
    def __init__(self, input_dir: str = "raw/sepe", output_dir: str = "clean/sepe"):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Spanish province name mappings (for consistency)
        self.province_mappings = {
            'A CORUÑA': 'CORUÑA',
            'CORUÑA A': 'CORUÑA',
            'STA CRUZ TENER.': 'SANTA CRUZ TENERIFE',
            'STA. CRUZ TENER.': 'SANTA CRUZ TENERIFE',
            'CONTRTOS LUGO': 'CONTRATOS LUGO'  # Fix typo in sheet name
        }
    
    def extract_date_from_filename(self, filename: str) -> Tuple[int, int]:
        """Extract year and month from filename like '2021_05_employment.xls'"""
        parts = filename.replace('.xls', '').split('_')
        if len(parts) >= 2:
            year = int(parts[0])
            month = int(parts[1])
            return year, month
        raise ValueError(f"Cannot extract date from filename: {filename}")
    
    def clean_sheet_name(self, sheet_name: str) -> Tuple[str, str, str]:
        """
        Extract data type, province from sheet name
        Returns: (data_type, province_name, clean_province)
        """
        sheet_name = sheet_name.strip()
        
        # Apply mappings for known inconsistencies
        for old_name, new_name in self.province_mappings.items():
            if old_name in sheet_name:
                sheet_name = sheet_name.replace(old_name, new_name)
        
        if sheet_name.startswith('PARO '):
            return 'unemployment', sheet_name[5:], sheet_name[5:].strip()
        elif sheet_name.startswith('CONTRATOS '):
            return 'contracts', sheet_name[10:], sheet_name[10:].strip()
        else:
            return 'unknown', sheet_name, sheet_name
    
    def parse_unemployment_sheet(self, df: pd.DataFrame, province: str, year: int, month: int) -> pd.DataFrame:
        """
        Parse unemployment data sheet structure
        """
        logger.info(f"Parsing unemployment sheet for {province}")
        
        # Find data start row (first row with municipality code)
        data_start_row = None
        for i, row in df.iterrows():
            if pd.notna(row.iloc[0]) and isinstance(row.iloc[0], (int, float)):
                if row.iloc[0] > 1000:  # Municipality codes are 5-digit numbers
                    data_start_row = i
                    break
        
        if data_start_row is None:
            logger.warning(f"No data found in unemployment sheet for {province}")
            return pd.DataFrame()
        
        # Extract data rows
        data_df = df.iloc[data_start_row:].copy()
        
        # Remove rows with NaN municipality codes
        data_df = data_df[pd.notna(data_df.iloc[:, 0])]
        
        # Define standardized column names based on analysis
        unemployment_columns = [
            'municipality_code',
            'municipality_name', 
            'total_unemployment',
            'men_under_25',
            'men_25_44',
            'men_45_plus',
            'women_under_25',
            'women_25_44', 
            'women_45_plus',
            'agriculture_sector',
            'industry_sector',
            'construction_sector',
            'services_sector',
            'no_previous_employment'
        ]
        
        # Ensure we have the right number of columns
        if len(data_df.columns) >= len(unemployment_columns):
            data_df = data_df.iloc[:, :len(unemployment_columns)]
            data_df.columns = unemployment_columns
        else:
            logger.warning(f"Unexpected column count in {province} unemployment data")
            return pd.DataFrame()
        
        # Add metadata columns
        data_df['province'] = province
        data_df['year'] = year
        data_df['month'] = month
        data_df['data_type'] = 'unemployment'
        
        # Clean data types
        numeric_columns = [col for col in unemployment_columns[2:] if col != 'municipality_name']
        for col in numeric_columns:
            data_df[col] = pd.to_numeric(data_df[col], errors='coerce').fillna(0)
        
        # Clean municipality code and name
        data_df['municipality_code'] = data_df['municipality_code'].astype(int)
        data_df['municipality_name'] = data_df['municipality_name'].astype(str).str.strip()
        
        return data_df
    
    def parse_contracts_sheet(self, df: pd.DataFrame, province: str, year: int, month: int) -> pd.DataFrame:
        """
        Parse contracts data sheet structure
        """
        logger.info(f"Parsing contracts sheet for {province}")
        
        # Find data start row (first row with municipality code)
        data_start_row = None
        for i, row in df.iterrows():
            if pd.notna(row.iloc[0]) and isinstance(row.iloc[0], (int, float)):
                if row.iloc[0] > 1000:  # Municipality codes are 5-digit numbers
                    data_start_row = i
                    break
        
        if data_start_row is None:
            logger.warning(f"No data found in contracts sheet for {province}")
            return pd.DataFrame()
        
        # Extract data rows
        data_df = df.iloc[data_start_row:].copy()
        
        # Remove rows with NaN municipality codes
        data_df = data_df[pd.notna(data_df.iloc[:, 0])]
        
        # Define standardized column names based on analysis
        contracts_columns = [
            'municipality_code',
            'municipality_name',
            'total_contracts', 
            'men_indefinite_initial',
            'men_temporary_initial',
            'men_indefinite_conversion',
            'women_indefinite_initial',
            'women_temporary_initial',
            'women_indefinite_conversion',
            'agriculture_sector',
            'industry_sector', 
            'construction_sector',
            'services_sector'
        ]
        
        # Ensure we have the right number of columns
        if len(data_df.columns) >= len(contracts_columns):
            data_df = data_df.iloc[:, :len(contracts_columns)]
            data_df.columns = contracts_columns
        else:
            logger.warning(f"Unexpected column count in {province} contracts data")
            return pd.DataFrame()
        
        # Add metadata columns
        data_df['province'] = province
        data_df['year'] = year
        data_df['month'] = month
        data_df['data_type'] = 'contracts'
        
        # Clean data types
        numeric_columns = [col for col in contracts_columns[2:] if col != 'municipality_name']
        for col in numeric_columns:
            # Handle spaces in data that should be zeros
            data_df[col] = data_df[col].replace(' ', 0)
            data_df[col] = pd.to_numeric(data_df[col], errors='coerce').fillna(0)
        
        # Clean municipality code and name
        data_df['municipality_code'] = data_df['municipality_code'].astype(int)
        data_df['municipality_name'] = data_df['municipality_name'].astype(str).str.strip()
        
        return data_df
    
    def process_file(self, file_path: Path) -> Dict[str, pd.DataFrame]:
        """
        Process a single SEPE XLS file and extract all data
        """
        logger.info(f"Processing file: {file_path.name}")
        
        try:
            year, month = self.extract_date_from_filename(file_path.name)
        except ValueError as e:
            logger.error(f"Skipping file {file_path.name}: {e}")
            return {}
        
        try:
            # Load Excel file
            xl_file = pd.ExcelFile(file_path)
            sheet_names = xl_file.sheet_names
            
            logger.info(f"Found {len(sheet_names)} sheets in {file_path.name}")
            
            results = {
                'unemployment': [],
                'contracts': []
            }
            
            for sheet_name in sheet_names:
                # Skip special sheets
                if sheet_name in ['PORTADA', 'Indice', 'NO CONSTA MUNIC.']:
                    continue
                
                data_type, province_raw, province_clean = self.clean_sheet_name(sheet_name)
                
                if data_type == 'unknown':
                    continue
                
                try:
                    # Read sheet
                    df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
                    
                    if data_type == 'unemployment':
                        parsed_df = self.parse_unemployment_sheet(df, province_clean, year, month)
                    elif data_type == 'contracts':
                        parsed_df = self.parse_contracts_sheet(df, province_clean, year, month)
                    else:
                        continue
                    
                    if not parsed_df.empty:
                        results[data_type].append(parsed_df)
                        logger.info(f"Processed {data_type} data for {province_clean}: {len(parsed_df)} municipalities")
                    
                except Exception as e:
                    logger.error(f"Error processing sheet {sheet_name}: {e}")
                    continue
            
            # Combine results
            final_results = {}
            for data_type, dfs in results.items():
                if dfs:
                    combined_df = pd.concat(dfs, ignore_index=True)
                    final_results[data_type] = combined_df
                    logger.info(f"Combined {data_type} data: {len(combined_df)} total records")
            
            return final_results
            
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
            return {}
    
    def clean_all_files(self) -> Dict[str, List[pd.DataFrame]]:
        """
        Process all SEPE XLS files in the input directory
        """
        logger.info(f"Starting SEPE data cleaning from {self.input_dir}")
        
        # Find all XLS files
        xls_files = list(self.input_dir.glob("*.xls"))
        logger.info(f"Found {len(xls_files)} XLS files to process")
        
        all_results = {
            'unemployment': [],
            'contracts': []
        }
        
        for file_path in sorted(xls_files):
            file_results = self.process_file(file_path)
            
            for data_type, df in file_results.items():
                if not df.empty:
                    all_results[data_type].append(df)
        
        # Combine all data and save
        for data_type, dfs in all_results.items():
            if dfs:
                logger.info(f"Combining {len(dfs)} files for {data_type} data")
                combined_df = pd.concat(dfs, ignore_index=True)
                
                # Save to CSV
                output_file = self.output_dir / f"sepe_{data_type}.csv"
                combined_df.to_csv(output_file, index=False, encoding='utf-8')
                logger.info(f"Saved {data_type} data: {output_file} ({len(combined_df)} records)")
        
        return all_results


def main():
    """Test the SEPE cleaner with a single file"""
    cleaner = SepeDataCleaner()
    
    # Test with one file first
    test_files = list(Path("raw/sepe").glob("*.xls"))
    if test_files:
        test_file = test_files[0]
        logger.info(f"Testing with file: {test_file}")
        
        results = cleaner.process_file(test_file)
        
        for data_type, df in results.items():
            logger.info(f"\n{data_type.upper()} DATA SAMPLE:")
            logger.info(f"Shape: {df.shape}")
            logger.info(f"Columns: {list(df.columns)}")
            logger.info(f"Sample data:\n{df.head()}")
            
            # Save sample
            sample_file = Path(f"sepe_{data_type}_sample.csv")
            df.head(100).to_csv(sample_file, index=False)
            logger.info(f"Saved sample to: {sample_file}")
    else:
        logger.error("No SEPE XLS files found for testing")


if __name__ == "__main__":
    main()