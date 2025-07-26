"""
Data processing utilities for municipality analytics pipeline.

Contains functions for:
- Excel header detection and parsing
- Column name standardization across years  
- DataFrame cleaning and validation
"""

import pandas as pd
import re
from typing import Optional


def detect_header_row(df: pd.DataFrame, max_rows_to_check: int = 10) -> int:
    """
    Detect which row contains the actual headers by analyzing text vs numeric patterns.
    
    Excel files from INE have variable header positions due to title rows and metadata.
    This function identifies the row that contains actual column headers.
    
    Args:
        df: Raw DataFrame read from Excel
        max_rows_to_check: Maximum number of rows to analyze for headers
        
    Returns:
        Index of the row containing headers (0-based)
    """
    for row_idx in range(min(max_rows_to_check, len(df))):
        row_data = df.iloc[row_idx]
        
        # Skip completely empty rows
        if row_data.isna().all():
            continue
            
        # Check if this row looks like headers (mostly text, not numbers)
        non_null_values = row_data.dropna()
        if len(non_null_values) < 2:  # Need at least 2 non-null values
            continue
            
        # Count text vs numeric values
        text_count = sum(
            1 for val in non_null_values 
            if isinstance(val, str) and not str(val).replace('.', '').replace(',', '').isdigit()
        )
        numeric_count = len(non_null_values) - text_count
        
        # If mostly text, this is likely the header row
        if text_count >= numeric_count and len(non_null_values) >= 3:
            return row_idx
    
    # Default to row 1 (index 1) if no clear header found
    return 1


def standardize_demography_columns(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """
    Standardize column names across different years of demography data.
    
    INE demographic data has inconsistent column naming across years:
    - Different formats: 'pob98', 'pob99', etc.
    - Various spellings: 'varon'/'hombre', 'mujer'/'mujeres'
    - Code variations: 'cpro'/'cod_prov'/'codigo_prov'
    
    This function creates consistent column names for reliable processing.
    
    Args:
        df: DataFrame with original INE column names
        year: Year of the data (for context)
        
    Returns:
        DataFrame with standardized column names
    """
    new_columns = []
    for col in df.columns:
        col_str = str(col).strip().lower()
        
        # Standardize population columns (pob98, pob99, etc.)
        if re.match(r'pob\d{2}', col_str):
            new_columns.append('population_total')
        # Municipality code columns
        elif any(term in col_str for term in ['codigo', 'cod', 'cmun']) and 'municipio' in col_str:
            new_columns.append('municipality_code')
        # Municipality name columns
        elif any(term in col_str for term in ['municipio', 'nombre']) and 'codigo' not in col_str:
            new_columns.append('municipality_name')
        # Province CODE columns (cpro, codigo_provincia, etc.)
        elif any(term in col_str for term in ['cpro', 'cod_prov', 'codigo_prov']):
            new_columns.append('province_code')
        # Province NAME columns (provincia, nombre_provincia, etc.)
        elif 'provincia' in col_str and 'codigo' not in col_str and 'cpro' not in col_str:
            new_columns.append('province_name')
        # Handle ambiguous "province" column
        elif col_str == 'province':
            # Determine if this is code or name based on data inspection
            if len(df) > 0:
                sample_values = df[col].dropna().head(10)
                if (sample_values.dtype in ['int64', 'float64'] or 
                    all(str(val).isdigit() for val in sample_values if pd.notna(val))):
                    new_columns.append('province_code')
                else:
                    new_columns.append('province_name')
            else:
                new_columns.append('province_code')  # Default assumption
        # Male population columns
        elif any(term in col_str for term in ['varon', 'hombre', 'varones', 'hombres', 'masculino']):
            new_columns.append('population_male')
        # Female population columns
        elif any(term in col_str for term in ['mujer', 'mujeres', 'femenino', 'femenina']):
            new_columns.append('population_female')
        # Total/both sexes columns
        elif any(term in col_str for term in ['total', 'ambos_sexos', 'ambos', 'suma', 'totales']):
            new_columns.append('population_total')
        # Autonomous community columns
        elif any(term in col_str for term in ['comunidad', 'autonoma', 'ccaa']):
            new_columns.append('autonomous_community')
        # Island columns (for Canarias/Baleares)
        elif any(term in col_str for term in ['isla', 'island']):
            new_columns.append('island')
        else:
            # Clean other column names
            clean_name = _clean_column_name(col_str)
            new_columns.append(clean_name or f'unnamed_column_{len(new_columns)}')
    
    # Handle duplicate column names by adding suffixes
    final_columns = _handle_duplicate_columns(new_columns)
    df.columns = final_columns
    
    # Standardize data types for consistency
    df = _standardize_data_types(df)
    
    return df


def clean_dataframe(df: pd.DataFrame, year: Optional[int] = None) -> pd.DataFrame:
    """
    Clean DataFrame by removing empty rows/columns and standardizing names.
    
    Args:
        df: DataFrame to clean
        year: Optional year for demography-specific cleaning
        
    Returns:
        Cleaned DataFrame
    """
    # Remove completely empty rows and columns
    df = df.dropna(how='all').dropna(how='all', axis=1)
    
    # Apply demography-specific cleaning if year provided
    if year:
        df = standardize_demography_columns(df, year)
    else:
        # Generic column cleaning
        new_columns = []
        for col in df.columns:
            if pd.isna(col) or str(col).strip() == '':
                new_columns.append(f'unnamed_column_{len(new_columns)}')
            else:
                clean_name = _clean_column_name(str(col).strip().lower())
                new_columns.append(clean_name or f'unnamed_column_{len(new_columns)}')
        df.columns = new_columns
    
    # Remove rows that appear to be subtotals or aggregates
    if len(df.columns) > 0:
        first_col = df.columns[0]
        df = df[~df[first_col].astype(str).str.lower().str.contains('total|suma|agregado', na=False)]
    
    return df


def _clean_column_name(name: str) -> str:
    """Clean individual column name by removing special characters."""
    clean_name = name.replace(' ', '_').replace('/', '_').replace('-', '_')
    clean_name = clean_name.replace('(', '').replace(')', '').replace('.', '').replace(',', '')
    # Remove any remaining special characters
    clean_name = ''.join(c for c in clean_name if c.isalnum() or c == '_')
    # Ensure it doesn't start with a number
    if clean_name and clean_name[0].isdigit():
        clean_name = 'col_' + clean_name
    return clean_name


def _handle_duplicate_columns(columns: list) -> list:
    """Handle duplicate column names by adding numeric suffixes."""
    seen = {}
    final_columns = []
    for col in columns:
        if col in seen:
            seen[col] += 1
            final_columns.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 0
            final_columns.append(col)
    return final_columns


def _standardize_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize data types for consistency across years."""
    # Convert province codes to strings (zero-padded to 2 digits)
    if 'province_code' in df.columns:
        df['province_code'] = df['province_code'].astype(str).str.zfill(2)
    
    # Convert municipality codes to strings (zero-padded to 5 digits)
    if 'municipality_code' in df.columns:
        df['municipality_code'] = df['municipality_code'].astype(str).str.zfill(5)
    
    # Ensure population columns are numeric
    for col in df.columns:
        if 'population' in col:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df