"""
SEPE Data Cleaner Utility
Extracts unemployment and contract data from complex multi-sheet SEPE Excel files
"""

import pandas as pd
import os
import re
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import logging
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import multiprocessing as mp
from functools import lru_cache
import time
import openpyxl

class SepeDataCleaner:
    """
    Cleans and processes SEPE unemployment and contract data from Excel files
    """
    
    def __init__(self, input_dir: str = "/opt/dagster/raw/sepe", output_dir: str = "/opt/dagster/clean/sepe", force_reprocess: bool = False, max_workers: int = None):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.force_reprocess = force_reprocess
        self.max_workers = max_workers or min(4, mp.cpu_count())  # Limit to avoid memory issues
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Spanish province name mappings (for consistency)
        self.province_mappings = {
            'A CORUÑA': 'CORUÑA',
            'CORUÑA A': 'CORUÑA',
            'STA CRUZ TENER.': 'SANTA CRUZ TENERIFE',
            'STA. CRUZ TENER.': 'SANTA CRUZ TENERIFE',
            'CONTRTOS LUGO': 'CONTRATOS LUGO'  # Fix typo in sheet name
        }
        
        # Pre-compiled regex for better performance
        self.header_keywords_pattern = re.compile(
            r'PARO REGISTRADO|SEGÚN SEXO|EDAD Y SECTOR|MUNICIPIOS|TOTAL|HOMBRES|MUJERES|'
            r'SECTORES|AGRI-|INDUS-|CONS-|CONTRATOS DE TRABAJO|REGISTRADOS SEGÚN|'
            r'TIPO DE CONTRATO|INIC\. INDEF\.|INIC\. TEMPORAL|CONVERT\.', 
            re.IGNORECASE
        )
        
        # Cache for format detection results
        self._format_cache = {}
        
        # Setup logger
        self.logger = logging.getLogger(__name__)
        
        # Error tracking and logging
        self.error_log_file = self.output_dir / 'processing_errors.log'
        self.processing_stats = {
            'files_processed': 0,
            'files_failed': 0,
            'sheets_processed': 0,
            'sheets_failed': 0,
            'engine_fallbacks': 0,
            'data_quality_issues': 0,
            'old_format_files': 0,
            'new_format_files': 0
        }
    
    def log_error(self, error_type: str, file_name: str, sheet_name: str = None, error_message: str = None):
        """Log errors to both console and error log file"""
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        location = f"{file_name}" + (f" -> {sheet_name}" if sheet_name else "")
        log_entry = f"[{timestamp}] {error_type}: {location}"
        if error_message:
            log_entry += f" - {error_message}"
        
        # Log to file
        with open(self.error_log_file, 'a', encoding='utf-8') as f:
            f.write(log_entry + '\n')
        
        # Log to console
        self.logger.error(log_entry)
    
    def extract_date_from_filename(self, filename: str) -> Tuple[int, int]:
        """Extract year and month from filename like '2021_05_employment.xls'"""
        try:
            parts = filename.replace('.xls', '').split('_')
            if len(parts) >= 2:
                year = int(parts[0])
                month = int(parts[1])
                return year, month
            raise ValueError(f"Invalid filename format: {filename}")
        except Exception as e:
            self.log_error('FILENAME_PARSE_ERROR', filename, error_message=str(e))
            raise ValueError(f"Cannot extract date from filename: {filename} - {e}")
    
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
    
    def normalize_province_name(self, province: str) -> str:
        """Normalize province name for consistent file naming"""
        # Replace spaces and special characters for file naming
        normalized = province.replace(' ', '_').replace('.', '').replace('/', '_')
        normalized = normalized.upper()
        return normalized
    
    def validate_parsed_data(self, df: pd.DataFrame, data_type: str, file_name: str, sheet_name: str):
        """Validate parsed data quality and log issues (excluding known OLD format limitations)"""
        issues = []
        
        # Extract year from filename to determine format
        try:
            year = int(file_name.split('_')[0])
            is_old_format = year <= 2012
        except:
            is_old_format = False
        
        # Check for missing municipality codes (skip placeholder codes in OLD format)
        if 'municipality_code' in df.columns:
            missing_codes = df['municipality_code'].isna().sum()
            if missing_codes > 0:
                issues.append(f"{missing_codes} missing municipality codes")
            
            # Only log placeholder codes as issues in NEW format (>=2013)
            if not is_old_format:
                placeholder_codes = (df['municipality_code'] == 0).sum()
                if placeholder_codes > 0:
                    issues.append(f"{placeholder_codes} unexpected placeholder municipality codes (0)")
        
        # Check for missing municipality names
        if 'municipality_name' in df.columns:
            missing_names = df['municipality_name'].isna().sum()
            if missing_names > 0:
                issues.append(f"{missing_names} missing municipality names")
        
        # Check for negative values in numeric columns
        numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
        for col in numeric_cols:
            if col not in ['municipality_code', 'year', 'month']:
                negative_count = (df[col] < 0).sum()
                if negative_count > 0:
                    issues.append(f"{negative_count} negative values in {col}")
        
        # Check for suspiciously high values (potential data errors)
        if data_type == 'unemployment':
            if 'total_unemployment' in df.columns:
                high_values = (df['total_unemployment'] > 50000).sum()  # Arbitrary threshold
                if high_values > 0:
                    issues.append(f"{high_values} suspiciously high unemployment values (>50k)")
        elif data_type == 'contracts':
            if 'total_contracts' in df.columns:
                high_values = (df['total_contracts'] > 100000).sum()  # Arbitrary threshold
                if high_values > 0:
                    issues.append(f"{high_values} suspiciously high contract values (>100k)")
        
        # Log validation issues (only real issues, not expected OLD format behavior)
        if issues:
            issue_summary = "; ".join(issues)
            self.log_error('DATA_QUALITY_ISSUE', file_name, sheet_name, issue_summary)
            self.processing_stats['data_quality_issues'] += 1
    
    @lru_cache(maxsize=128)
    def get_format_by_year(self, year: int) -> str:
        """Cache format detection by year for performance"""
        return 'OLD' if year <= 2012 else 'NEW'
    
    def detect_format_and_data_start(self, df: pd.DataFrame, province: str, year: int) -> tuple:
        """
        Optimized format detection and data start row finding
        Returns: (format_type, data_start_row, has_codes)
        """
        format_type = self.get_format_by_year(year)
        cache_key = f"{year}_{format_type}_{province}"
        
        # Check cache first
        if cache_key in self._format_cache:
            return self._format_cache[cache_key]
        
        if format_type == 'OLD':
            # Optimized search for OLD format - check first 50 rows only
            search_df = df.head(50)  # Limit search range
            
            # Vectorized operation to find municipality names
            if len(search_df.columns) > 1:
                col1_values = search_df.iloc[:, 1].dropna()
                # Filter string values with length > 3
                string_mask = col1_values.astype(str).str.len() > 3
                alpha_mask = col1_values.astype(str).str.contains(r'[a-zA-Z]', na=False)
                exclude_mask = ~col1_values.astype(str).str.upper().isin(['TOTAL', 'HOMBRES', 'MUJERES', 'MUNICIPIOS'])
                
                valid_indices = col1_values[string_mask & alpha_mask & exclude_mask].index
                
                if len(valid_indices) > 0:
                    data_start = valid_indices[0]
                    result = ('OLD', data_start, False)
                else:
                    result = ('OLD', None, False)
            else:
                result = ('OLD', None, False)
        else:
            # NEW FORMAT: Optimized search for municipality codes
            search_df = df.head(50)  # Limit search range
            
            if len(search_df.columns) > 0:
                col0_numeric = pd.to_numeric(search_df.iloc[:, 0], errors='coerce')
                valid_codes = col0_numeric[col0_numeric > 1000].dropna()
                
                if len(valid_codes) > 0:
                    data_start = valid_codes.index[0]
                    result = ('NEW', data_start, True)
                else:
                    result = ('NEW', None, True)
            else:
                result = ('NEW', None, True)
        
        # Cache the result
        self._format_cache[cache_key] = result
        return result
    
    def parse_unemployment_sheet(self, df: pd.DataFrame, province: str, year: int, month: int) -> pd.DataFrame:
        """Parse unemployment data sheet structure"""
        self.logger.info(f"Parsing unemployment sheet for {province} (year {year}) - shape: {df.shape}")
        
        # Detect format and find data start
        format_type, data_start_row, has_codes = self.detect_format_and_data_start(df, province, year)
        
        if data_start_row is None:
            self.logger.warning(f"No data found in unemployment sheet for {province} ({format_type} format)")
            return pd.DataFrame()
        
        self.logger.info(f"Using {format_type} format, data starts at row {data_start_row}")
        
        # Extract data rows
        data_df = df.iloc[data_start_row:].copy()
        
        if format_type == 'NEW':
            # NEW FORMAT: Has municipality codes in column 0
            # Remove rows with NaN municipality codes
            data_df = data_df[pd.notna(data_df.iloc[:, 0])]
            
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
                self.logger.warning(f"Unexpected column count in {province} unemployment data (NEW format)")
                return pd.DataFrame()
                
        else:  # OLD FORMAT
            # OLD FORMAT: No municipality codes, different column structure
            # Remove rows with NaN municipality names and filter out header rows
            data_df = data_df[pd.notna(data_df.iloc[:, 1])]
            data_df = data_df[data_df.iloc[:, 1].astype(str).str.len() > 3]  # Filter out short strings
            
            # Optimized header filtering using pre-compiled regex
            if len(data_df) > 0 and len(data_df.columns) > 1:
                municipality_names = data_df.iloc[:, 1].astype(str)
                # Single regex operation instead of multiple loops
                header_mask = municipality_names.str.contains(self.header_keywords_pattern, na=False)
                province_mask = municipality_names.str.contains(province, case=False, na=False)
                data_df = data_df[~(header_mask | province_mask)]
            
            if year <= 2007:
                # Very old format (2005-2007) with 17 columns
                unemployment_columns_old = [
                    'temp_col_0',  # Empty column in old format
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
                    'no_previous_employment',
                    'extra_col_1',
                    'extra_col_2',
                    'extra_col_3'
                ]
            else:
                # Intermediate old format (2008-2012) with 14 columns
                unemployment_columns_old = [
                    'temp_col_0',
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
            
            # Apply column names with flexible handling
            actual_columns = len(data_df.columns)
            expected_columns = len(unemployment_columns_old)
            
            if actual_columns < expected_columns:
                self.logger.warning(f"Not enough columns in {province} unemployment data (OLD format): "
                                  f"got {actual_columns}, expected {expected_columns}")
                # Use only the columns we have
                available_columns = unemployment_columns_old[:actual_columns]
                data_df.columns = available_columns
            else:
                # Use expected columns, truncate if more
                data_df = data_df.iloc[:, :expected_columns]
                data_df.columns = unemployment_columns_old
                
            self.logger.info(f"Processed {province} unemployment data (OLD format): "
                           f"{actual_columns} columns -> {len(data_df.columns)} columns")
            
            # Generate municipality codes (placeholder for now - could be enhanced with lookup)
            data_df['municipality_code'] = 0  # Placeholder
            
            # Reorganize columns to match NEW format - flexible handling
            final_columns = [
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
            
            # Build column list based on what's actually available
            available_columns = ['municipality_code', 'municipality_name']
            for col in unemployment_columns_old[2:]:
                if col in data_df.columns and col in final_columns:
                    available_columns.append(col)
            
            data_df = data_df[available_columns]
        
        # Add metadata columns
        data_df['province'] = province
        data_df['year'] = year
        data_df['month'] = month
        data_df['data_type'] = 'unemployment'
        
        # Optimized data type cleaning - vectorized operations
        numeric_columns = [col for col in data_df.columns 
                          if col not in ['municipality_name', 'province', 'year', 'month', 'data_type']]
        
        if numeric_columns:
            # Vectorized string cleaning for all numeric columns at once
            for col in numeric_columns:
                if col != 'municipality_code':  # Skip municipality_code for old format with placeholders
                    # Single vectorized operation for special value handling
                    data_df[col] = pd.to_numeric(
                        data_df[col].astype(str).str.replace(r'[<>]', '', regex=True),
                        errors='coerce'
                    ).fillna(0)
        
        # Clean municipality code and name
        if format_type == 'NEW':
            data_df['municipality_code'] = data_df['municipality_code'].astype(int)
        # For OLD format, municipality_code is already 0 (placeholder)
        
        data_df['municipality_name'] = data_df['municipality_name'].astype(str).str.strip()
        
        return data_df
    
    def parse_contracts_sheet(self, df: pd.DataFrame, province: str, year: int, month: int) -> pd.DataFrame:
        """Parse contracts data sheet structure"""
        self.logger.info(f"Parsing contracts sheet for {province} (year {year}) - shape: {df.shape}")
        
        # Use the same format detection logic
        format_type, data_start_row, has_codes = self.detect_format_and_data_start(df, province, year)
        
        if data_start_row is None:
            self.logger.warning(f"No data found in contracts sheet for {province} ({format_type} format)")
            return pd.DataFrame()
        
        self.logger.info(f"Using {format_type} format, data starts at row {data_start_row}")
        
        # Extract data rows
        data_df = df.iloc[data_start_row:].copy()
        
        if format_type == 'NEW':
            # NEW FORMAT: Has municipality codes in column 0
            # Remove rows with NaN municipality codes
            data_df = data_df[pd.notna(data_df.iloc[:, 0])]
            
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
                self.logger.warning(f"Unexpected column count in {province} contracts data (NEW format)")
                return pd.DataFrame()
                
        else:  # OLD FORMAT
            # OLD FORMAT: No municipality codes, different column structure
            # Remove rows with NaN municipality names and filter out header rows
            data_df = data_df[pd.notna(data_df.iloc[:, 1])]
            data_df = data_df[data_df.iloc[:, 1].astype(str).str.len() > 3]  # Filter out short strings
            
            # Optimized header filtering using pre-compiled regex
            if len(data_df) > 0 and len(data_df.columns) > 1:
                municipality_names = data_df.iloc[:, 1].astype(str)
                # Single regex operation instead of multiple loops
                header_mask = municipality_names.str.contains(self.header_keywords_pattern, na=False)
                province_mask = municipality_names.str.contains(province, case=False, na=False)
                data_df = data_df[~(header_mask | province_mask)]
            
            # OLD format contracts should have similar structure but fewer columns
            contracts_columns_old = [
                'temp_col_0',
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
            
            # Apply column names with flexible handling
            actual_columns = len(data_df.columns)
            expected_columns = len(contracts_columns_old)
            
            if actual_columns < expected_columns:
                self.logger.warning(f"Not enough columns in {province} contracts data (OLD format): "
                                  f"got {actual_columns}, expected {expected_columns}")
                # Use only the columns we have
                available_columns = contracts_columns_old[:actual_columns]
                data_df.columns = available_columns
            else:
                # Use expected columns, truncate if more
                data_df = data_df.iloc[:, :expected_columns]
                data_df.columns = contracts_columns_old
                
            self.logger.info(f"Processed {province} contracts data (OLD format): "
                           f"{actual_columns} columns -> {len(data_df.columns)} columns")
            
            # Generate municipality codes (placeholder)
            data_df['municipality_code'] = 0
            
            # Reorganize columns to match NEW format - flexible handling
            final_columns = [
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
            
            # Build column list based on what's actually available
            available_columns = ['municipality_code', 'municipality_name']
            for col in contracts_columns_old[2:]:
                if col in data_df.columns and col in final_columns:
                    available_columns.append(col)
            
            data_df = data_df[available_columns]
        
        # Add metadata columns
        data_df['province'] = province
        data_df['year'] = year
        data_df['month'] = month
        data_df['data_type'] = 'contracts'
        
        # Optimized data type cleaning - vectorized operations
        numeric_columns = [col for col in data_df.columns 
                          if col not in ['municipality_name', 'province', 'year', 'month', 'data_type']]
        
        if numeric_columns:
            # Vectorized string cleaning for all numeric columns at once
            for col in numeric_columns:
                if col != 'municipality_code':  # Skip municipality_code for old format with placeholders
                    # Single vectorized operation for space and special value handling
                    data_df[col] = pd.to_numeric(
                        data_df[col].astype(str).str.replace(r'[ <>]', '', regex=True).replace('', '0'),
                        errors='coerce'
                    ).fillna(0)
        
        # Clean municipality code and name
        if format_type == 'NEW':
            data_df['municipality_code'] = data_df['municipality_code'].astype(int)
        # For OLD format, municipality_code is already 0 (placeholder)
        
        data_df['municipality_name'] = data_df['municipality_name'].astype(str).str.strip()
        
        return data_df
    
    def get_optimal_engine(self, file_path: Path) -> str:
        """Choose optimal Excel engine based on file type and size"""
        # Try calamine first for both .xls and .xlsx - it's much faster (6-58x speedup)
        try:
            # Test if calamine can handle this file type
            if file_path.suffix.lower() in ['.xlsx', '.xls']:
                return 'calamine'  # Rust-based, extremely fast
        except Exception:
            # Fallback to traditional engines if calamine fails
            pass
        
        # Fallback engines
        if file_path.suffix.lower() == '.xlsx':
            return 'openpyxl'  # Slower but reliable for .xlsx
        else:
            return 'xlrd'  # Required for older .xls files
    
    def process_sheet_batch(self, file_path: Path, sheet_batch: List[str], year: int, month: int) -> Dict[str, Dict[str, pd.DataFrame]]:
        """Process a batch of sheets in parallel with performance monitoring"""
        batch_start_time = time.time()
        results = {'unemployment': {}, 'contracts': {}}
        engine = self.get_optimal_engine(file_path)
        
        for sheet_name in sheet_batch:
            try:
                # Skip special sheets
                if sheet_name in ['PORTADA', 'Indice', 'NO CONSTA MUNIC.']:
                    continue
                
                data_type, province_raw, province_clean = self.clean_sheet_name(sheet_name)
                if data_type == 'unknown':
                    continue
                
                # Optimized sheet reading with calamine engine for massive speed boost
                sheet_read_start = time.time()
                try:
                    df = pd.read_excel(file_path, sheet_name=sheet_name, header=None, engine=engine)
                    read_time = time.time() - sheet_read_start
                    # Only log if read takes more than 0.5s
                    if read_time > 0.5:
                        self.logger.debug(f"Sheet {sheet_name} read with {engine} in {read_time:.2f}s")
                except Exception as e:
                    # Fallback to openpyxl/xlrd if calamine fails
                    self.log_error('ENGINE_FALLBACK', file_path.name, sheet_name, f"Calamine failed: {str(e)[:100]}")
                    self.processing_stats['engine_fallbacks'] += 1
                    
                    fallback_engine = 'openpyxl' if file_path.suffix.lower() == '.xlsx' else 'xlrd'
                    fallback_start = time.time()
                    try:
                        df = pd.read_excel(file_path, sheet_name=sheet_name, header=None, engine=fallback_engine)
                        fallback_time = time.time() - fallback_start
                        self.logger.info(f"🔄 Fallback read with {fallback_engine} in {fallback_time:.2f}s")
                        engine = fallback_engine  # Update engine for logging
                    except Exception as fallback_error:
                        self.log_error('SHEET_READ_FAILED', file_path.name, sheet_name, f"Both engines failed. Calamine: {str(e)[:50]}, {fallback_engine}: {str(fallback_error)[:50]}")
                        self.processing_stats['sheets_failed'] += 1
                        continue
                
                try:
                    if data_type == 'unemployment':
                        parsed_df = self.parse_unemployment_sheet(df, province_clean, year, month)
                    elif data_type == 'contracts':
                        parsed_df = self.parse_contracts_sheet(df, province_clean, year, month)
                    else:
                        continue
                    
                    if not parsed_df.empty:
                        # Data quality validation
                        self.validate_parsed_data(parsed_df, data_type, file_path.name, sheet_name)
                        normalized_province = self.normalize_province_name(province_clean)
                        results[data_type][normalized_province] = parsed_df
                        self.processing_stats['sheets_processed'] += 1
                    else:
                        self.log_error('EMPTY_DATA', file_path.name, sheet_name, f"No data extracted from {data_type} sheet")
                        self.processing_stats['data_quality_issues'] += 1
                        
                except Exception as parse_error:
                    self.log_error('DATA_PARSE_ERROR', file_path.name, sheet_name, f"Failed to parse {data_type} data: {str(parse_error)[:100]}")
                    self.processing_stats['sheets_failed'] += 1
                    continue
                
                # Clear DataFrame to free memory
                del df
                
            except Exception as e:
                self.log_error('SHEET_PROCESSING_ERROR', file_path.name, sheet_name, str(e)[:100])
                self.processing_stats['sheets_failed'] += 1
                continue
        
        batch_time = time.time() - batch_start_time
        self.logger.info(f"Processed {len(sheet_batch)} sheets in {batch_time:.2f}s (avg {batch_time/len(sheet_batch):.2f}s/sheet) using {engine}")
        return results
    
    def process_file(self, file_path: Path) -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        Optimized processing of a single SEPE XLS file with parallel sheet processing
        Returns: Dict[data_type, Dict[province, DataFrame]]
        """
        start_time = time.time()
        file_size_mb = file_path.stat().st_size / (1024 * 1024)
        self.logger.info(f"Processing file: {file_path.name} ({file_size_mb:.1f}MB)")
        
        try:
            year, month = self.extract_date_from_filename(file_path.name)
            # Track format distribution
            if year <= 2012:
                self.processing_stats['old_format_files'] += 1
            else:
                self.processing_stats['new_format_files'] += 1
        except ValueError as e:
            self.log_error('FILE_PROCESSING_ERROR', file_path.name, error_message=str(e))
            self.processing_stats['files_failed'] += 1
            return {}
        
        try:
            # Quick check of file size and skip if too large
            file_size_mb = file_path.stat().st_size / (1024 * 1024)
            if file_size_mb > 100:  # Skip files larger than 100MB
                self.logger.warning(f"Skipping large file {file_path.name}: {file_size_mb:.1f}MB")
                return {}
            
            # Load Excel file with optimal engine (calamine for 6-58x speed boost)
            engine = self.get_optimal_engine(file_path)
            try:
                xl_file = pd.ExcelFile(file_path, engine=engine)
            except Exception as e:
                # Fallback if calamine fails
                self.logger.warning(f"Primary engine {engine} failed, falling back: {e}")
                fallback_engine = 'openpyxl' if file_path.suffix.lower() == '.xlsx' else 'xlrd'
                xl_file = pd.ExcelFile(file_path, engine=fallback_engine)
                engine = fallback_engine
            sheet_names = xl_file.sheet_names
            
            self.logger.info(f"Found {len(sheet_names)} sheets in {file_path.name}")
            
            # Filter relevant sheets early
            relevant_sheets = [s for s in sheet_names 
                             if s not in ['PORTADA', 'Indice', 'NO CONSTA MUNIC.'] 
                             and ('PARO' in s or 'CONTRATOS' in s)]
            
            self.logger.info(f"Processing {len(relevant_sheets)} relevant sheets")
            
            # Process sheets in smaller batches to manage memory
            batch_size = min(10, len(relevant_sheets))  # Process 10 sheets at a time
            
            results = {'unemployment': {}, 'contracts': {}}
            
            for i in range(0, len(relevant_sheets), batch_size):
                batch = relevant_sheets[i:i + batch_size]
                batch_results = self.process_sheet_batch(file_path, batch, year, month)
                
                # Merge results
                for data_type in ['unemployment', 'contracts']:
                    results[data_type].update(batch_results[data_type])
                
                # Log progress
                self.logger.info(f"Processed batch {i//batch_size + 1}/{(len(relevant_sheets)-1)//batch_size + 1}")
            
            processing_time = time.time() - start_time
            processing_rate = file_size_mb / processing_time if processing_time > 0 else 0
            
            self.logger.info(f"✅ PERFORMANCE: {file_path.name} ({file_size_mb:.1f}MB) processed in {processing_time:.1f}s")
            self.logger.info(f"   📊 Rate: {processing_rate:.1f} MB/s | Engine: {engine} | "
                           f"Unemployment: {len(results['unemployment'])} provinces, "
                           f"Contracts: {len(results['contracts'])} provinces")
            
            # Log significant performance improvements if using calamine
            if engine == 'calamine' and processing_time > 0:
                estimated_openpyxl_time = processing_time * 6  # Conservative estimate (6x slower)
                self.logger.info(f"   ⚡ Estimated time savings vs openpyxl: {estimated_openpyxl_time - processing_time:.1f}s ")
                self.logger.info(f"      (Would have taken ~{estimated_openpyxl_time:.1f}s with openpyxl)")
            
            return results
            
        except Exception as e:
            self.log_error('FILE_PROCESSING_ERROR', file_path.name, error_message=str(e))
            self.processing_stats['files_failed'] += 1
            return {}
        finally:
            self.processing_stats['files_processed'] += 1
    
    def save_consolidated_data(self, data_type: str, all_provinces_data: Dict[str, pd.DataFrame], year: int, month: int) -> str:
        """Save consolidated data for all provinces in a single CSV file"""
        filename = f"{year}_{month:02d}_{data_type}.csv"
        file_path = self.output_dir / filename
        
        # Check if file already exists (unless force reprocessing)
        if file_path.exists() and not self.force_reprocess:
            self.logger.info(f"Consolidated file already exists, skipping: {filename}")
            return str(file_path)
        
        # Combine all province data into a single DataFrame
        combined_dfs = []
        for province, df in all_provinces_data.items():
            if not df.empty:
                combined_dfs.append(df)
        
        if combined_dfs:
            consolidated_df = pd.concat(combined_dfs, ignore_index=True)
            consolidated_df.to_csv(file_path, index=False, encoding='utf-8')
            self.logger.info(f"Saved consolidated {data_type} data: {file_path} ({len(consolidated_df)} total records, {len(all_provinces_data)} provinces)")
            return str(file_path)
        else:
            self.logger.warning(f"No data to save for {data_type} {year}-{month:02d}")
            return ""
    
    def check_file_already_processed(self, file_path: Path) -> bool:
        """
        Check if consolidated CSV outputs for this XLS file already exist
        """
        try:
            year, month = self.extract_date_from_filename(file_path.name)
        except ValueError:
            return False
        
        # Check for consolidated files
        unemployment_file = self.output_dir / f"{year}_{month:02d}_unemployment.csv"
        contracts_file = self.output_dir / f"{year}_{month:02d}_contracts.csv"
        
        files_exist = unemployment_file.exists() and contracts_file.exists()
        
        if files_exist:
            self.logger.info(f"File {file_path.name} already processed (consolidated CSV files found)")
        
        return files_exist

    def process_file_worker(self, file_path: Path) -> Tuple[Path, Dict[str, Dict[str, pd.DataFrame]]]:
        """Worker function for parallel file processing"""
        return file_path, self.process_file(file_path)
    
    def group_files_by_format(self, xls_files: List[Path]) -> Dict[str, List[Path]]:
        """Group files by format for batch processing optimization"""
        old_format_files = []
        new_format_files = []
        
        for file_path in xls_files:
            try:
                year, month = self.extract_date_from_filename(file_path.name)
                if year <= 2012:
                    old_format_files.append(file_path)
                else:
                    new_format_files.append(file_path)
            except ValueError:
                continue
        
        return {'old': old_format_files, 'new': new_format_files}
    
    def clean_all_files(self) -> Dict[str, List[str]]:
        """
        Optimized processing of all SEPE XLS files with parallel execution
        Returns: Dict[data_type, List[file_paths]]
        """
        start_time = time.time()
        self.logger.info(f"Starting optimized SEPE data cleaning from {self.input_dir}")
        
        # Initialize error log file
        with open(self.error_log_file, 'w', encoding='utf-8') as f:
            f.write(f"SEPE Processing Error Log - {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 80 + "\n\n")
        
        # Reset processing stats
        self.processing_stats = {
            'files_processed': 0,
            'files_failed': 0,
            'sheets_processed': 0,
            'sheets_failed': 0,
            'engine_fallbacks': 0,
            'data_quality_issues': 0,
            'old_format_files': 0,
            'new_format_files': 0
        }
        
        # Find all XLS files
        xls_files = list(self.input_dir.glob("*.xls"))
        self.logger.info(f"Found {len(xls_files)} XLS files to process")
        
        if not xls_files:
            self.logger.warning("No XLS files found to process")
            return {'unemployment': [], 'contracts': []}
        
        # Group files by format for optimized processing
        file_groups = self.group_files_by_format(xls_files)
        self.logger.info(f"File distribution: OLD format: {len(file_groups['old'])}, NEW format: {len(file_groups['new'])}")
        
        saved_files = {'unemployment': [], 'contracts': []}
        
        # Filter files that need processing
        files_to_process = []
        for file_path in sorted(xls_files):
            if not self.force_reprocess and self.check_file_already_processed(file_path):
                # Add existing files to the result
                try:
                    year, month = self.extract_date_from_filename(file_path.name)
                    for data_type in ['unemployment', 'contracts']:
                        consolidated_file = self.output_dir / f"{year}_{month:02d}_{data_type}.csv"
                        if consolidated_file.exists():
                            saved_files[data_type].append(str(consolidated_file))
                except ValueError:
                    pass
            else:
                files_to_process.append(file_path)
        
        self.logger.info(f"Processing {len(files_to_process)} files (skipped {len(xls_files) - len(files_to_process)} already processed)")
        
        if not files_to_process:
            self.logger.info("All files already processed")
            return saved_files
        
        # Process files with controlled parallelism
        processed_count = 0
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all jobs
            future_to_file = {executor.submit(self.process_file_worker, file_path): file_path 
                            for file_path in files_to_process}
            
            # Process results as they complete
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    file_path, file_results = future.result()
                    processed_count += 1
                    
                    if file_results:
                        year, month = self.extract_date_from_filename(file_path.name)
                        
                        # Save consolidated files for each data type
                        for data_type, provinces_data in file_results.items():
                            if provinces_data:  # Only if we have data
                                saved_file = self.save_consolidated_data(data_type, provinces_data, year, month)
                                if saved_file:  # Only add if file was actually saved
                                    saved_files[data_type].append(saved_file)
                    
                    # Log progress
                    if processed_count % 5 == 0 or processed_count == len(files_to_process):
                        elapsed = time.time() - start_time
                        rate = processed_count / elapsed if elapsed > 0 else 0
                        self.logger.info(f"Progress: {processed_count}/{len(files_to_process)} files "
                                       f"({elapsed:.1f}s, {rate:.1f} files/sec)")
                        
                except Exception as e:
                    self.log_error('FILE_PROCESSING_ERROR', file_path.name, error_message=str(e))
                    self.processing_stats['files_failed'] += 1
                    continue
        
        total_time = time.time() - start_time
        total_files_mb = sum(f.stat().st_size for f in files_to_process) / (1024 * 1024) if files_to_process else 0
        
        # Log comprehensive performance summary
        total_saved = sum(len(files) for files in saved_files.values())
        avg_rate = total_files_mb / total_time if total_time > 0 else 0
        
        self.logger.info(f"\n🎯 === OPTIMIZED SEPE PROCESSING SUMMARY ===")
        self.logger.info(f"⏱️  Total processing time: {total_time:.1f}s")
        self.logger.info(f"📁 Files processed: {processed_count} ({total_files_mb:.1f}MB total)")
        self.logger.info(f"📈 Overall processing rate: {avg_rate:.1f} MB/s")
        self.logger.info(f"⚡ Average time per file: {total_time/max(processed_count, 1):.1f}s")
        self.logger.info(f"📊 Output: {len(saved_files['unemployment'])} unemployment + {len(saved_files['contracts'])} contracts CSV files")
        self.logger.info(f"🚀 Performance engine: python-calamine (6-58x faster than openpyxl)")
        
        # Estimate time savings if using calamine for most files
        if processed_count > 0:
            estimated_openpyxl_time = total_time * 6  # Conservative 6x improvement
            time_saved = estimated_openpyxl_time - total_time
            self.logger.info(f"💰 Estimated time saved vs openpyxl: {time_saved:.1f}s ({time_saved/60:.1f} minutes)")
        
        # Generate comprehensive error and processing report
        self.generate_processing_report()
        
        return saved_files
    
    def generate_processing_report(self):
        """Generate comprehensive processing and error report"""
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        
        # Create summary report
        report_file = self.output_dir / 'processing_summary.txt'
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(f"SEPE Data Processing Report - {timestamp}\n")
            f.write("=" * 60 + "\n\n")
            
            # Processing Statistics
            f.write("PROCESSING STATISTICS:\n")
            f.write(f"• Total files processed: {self.processing_stats['files_processed']}\n")
            f.write(f"  - OLD format (2005-2012): {self.processing_stats['old_format_files']} files\n")
            f.write(f"  - NEW format (2013+): {self.processing_stats['new_format_files']} files\n")
            f.write(f"• Files failed: {self.processing_stats['files_failed']}\n")
            f.write(f"• Success rate: {((self.processing_stats['files_processed'] - self.processing_stats['files_failed']) / max(self.processing_stats['files_processed'], 1) * 100):.1f}%\n\n")
            
            f.write(f"• Total sheets processed: {self.processing_stats['sheets_processed']}\n")
            f.write(f"• Sheets failed: {self.processing_stats['sheets_failed']}\n")
            f.write(f"• Sheet success rate: {(self.processing_stats['sheets_processed'] / max(self.processing_stats['sheets_processed'] + self.processing_stats['sheets_failed'], 1) * 100):.1f}%\n\n")
            
            # Engine Performance
            f.write("ENGINE PERFORMANCE:\n")
            f.write(f"• Engine fallbacks (calamine → openpyxl/xlrd): {self.processing_stats['engine_fallbacks']}\n")
            f.write(f"• Data quality issues detected: {self.processing_stats['data_quality_issues']}\n\n")
            
            # Data Quality Context
            f.write("DATA QUALITY NOTES:\n")
            f.write(f"• OLD format files (2005-2012) use placeholder municipality codes (0) - this is expected\n")
            f.write(f"• NEW format files (2013+) have real municipality codes from INE\n")
            if self.processing_stats['data_quality_issues'] > 0:
                f.write(f"• {self.processing_stats['data_quality_issues']} sheets flagged for actual data quality issues\n")
            else:
                f.write(f"• No unexpected data quality issues detected\n")
            f.write("\n")
            
            # Recommendations
            f.write("RECOMMENDATIONS:\n")
            if self.processing_stats['engine_fallbacks'] > 0:
                f.write(f"• {self.processing_stats['engine_fallbacks']} files required engine fallback - consider investigating calamine compatibility\n")
            if self.processing_stats['data_quality_issues'] > 0:
                f.write(f"• Review error log for {self.processing_stats['data_quality_issues']} genuine data quality issues\n")
            if self.processing_stats['files_failed'] > 0:
                f.write(f"• {self.processing_stats['files_failed']} files failed completely - check error log for details\n")
            if self.processing_stats['engine_fallbacks'] == 0 and self.processing_stats['data_quality_issues'] == 0 and self.processing_stats['files_failed'] == 0:
                f.write(f"• No issues detected - processing completed successfully!\n")
            
            f.write(f"\nDetailed errors logged in: {self.error_log_file}\n")
        
        self.logger.info(f"📋 Processing report generated: {report_file}")
        self.logger.info(f"📋 Error log available: {self.error_log_file}")
        
        # Log key statistics to console
        if self.processing_stats['files_failed'] > 0 or self.processing_stats['data_quality_issues'] > 0:
            self.logger.warning(f"⚠️  PROCESSING ISSUES DETECTED:")
            self.logger.warning(f"   Files failed: {self.processing_stats['files_failed']}")
            self.logger.warning(f"   Sheets with quality issues: {self.processing_stats['data_quality_issues']}")
            self.logger.warning(f"   Engine fallbacks: {self.processing_stats['engine_fallbacks']}")
        else:
            self.logger.info("✅ No processing errors detected!")