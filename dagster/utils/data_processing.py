"""
Data processing utilities for municipality analytics pipeline
"""
import pandas as pd
import re
from typing import Optional


def detect_header_row(df: pd.DataFrame, max_rows_to_check: int = 10) -> int:
    """Detect which row contains the actual headers by looking for text vs numeric patterns"""
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
        text_count = sum(1 for val in non_null_values if isinstance(val, str) and not str(val).replace('.', '').replace(',', '').isdigit())
        numeric_count = len(non_null_values) - text_count
        
        # If mostly text, this is likely the header row
        if text_count >= numeric_count and len(non_null_values) >= 3:
            return row_idx
    
    # Default to row 1 (index 1) if no clear header found
    return 1


def standardize_demography_columns(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Standardize column names across different years of demography data"""
    new_columns = []
    for col in df.columns:
        col_str = str(col).strip().lower()
        
        # Standardize population columns (pob98, pob99, etc.)
        if re.match(r'pob\d{2}', col_str):
            new_columns.append('population_total')
        # Standardize municipality code columns
        elif any(term in col_str for term in ['codigo', 'cod', 'cmun']) and 'municipio' in col_str:
            new_columns.append('municipality_code')
        # Standardize municipality name columns
        elif any(term in col_str for term in ['municipio', 'nombre']) and 'codigo' not in col_str:
            new_columns.append('municipality_name')
        # Standardize province CODE columns (cpro, codigo_provincia, etc.)
        elif any(term in col_str for term in ['cpro', 'cod_prov', 'codigo_prov']):
            new_columns.append('province_code')
        # Standardize province NAME columns (provincia, nombre_provincia, etc.)
        elif 'provincia' in col_str and 'codigo' not in col_str and 'cpro' not in col_str:
            new_columns.append('province_name')
        # Handle the specific case where "province" appears as a standalone column
        elif col_str == 'province':
            # Determine if this is code or name based on data inspection
            # If it contains numeric data, it's likely a code
            if len(df) > 0:
                sample_values = df[col].dropna().head(10)
                if sample_values.dtype in ['int64', 'float64'] or all(str(val).isdigit() for val in sample_values if pd.notna(val)):
                    new_columns.append('province_code')
                else:
                    new_columns.append('province_name')
            else:
                new_columns.append('province_code')  # Default assumption
        # Standardize male population columns
        elif any(term in col_str for term in ['varon', 'hombre', 'varones', 'hombres', 'masculino']):
            new_columns.append('population_male')
        # Standardize female population columns
        elif any(term in col_str for term in ['mujer', 'mujeres', 'femenino', 'femenina']):
            new_columns.append('population_female')
        # Standardize total/both sexes columns
        elif any(term in col_str for term in ['total', 'ambos_sexos', 'ambos', 'suma', 'totales']):
            new_columns.append('population_total')
        # Standardize community/autonomy columns
        elif any(term in col_str for term in ['comunidad', 'autonoma', 'ccaa']):
            new_columns.append('autonomous_community')
        # Standardize island columns (for Canarias/Baleares)
        elif any(term in col_str for term in ['isla', 'island']):
            new_columns.append('island')
        else:
            # Keep other columns as cleaned names
            clean_name = col_str
            clean_name = clean_name.replace(' ', '_')
            clean_name = clean_name.replace('/', '_')
            clean_name = clean_name.replace('-', '_')
            clean_name = clean_name.replace('(', '')
            clean_name = clean_name.replace(')', '')
            clean_name = clean_name.replace('.', '')
            clean_name = clean_name.replace(',', '')
            # Remove any remaining special characters
            clean_name = ''.join(c for c in clean_name if c.isalnum() or c == '_')
            # Ensure it doesn't start with a number
            if clean_name and clean_name[0].isdigit():
                clean_name = 'col_' + clean_name
            new_columns.append(clean_name or f'unnamed_column_{len(new_columns)}')
    
    # Handle duplicate column names by adding suffixes
    seen = {}
    final_columns = []
    for col in new_columns:
        if col in seen:
            seen[col] += 1
            final_columns.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 0
            final_columns.append(col)
    
    df.columns = final_columns
    
    # Standardize data types to ensure consistency across years
    if 'province_code' in df.columns:
        # Convert province codes to strings (zero-padded to 2 digits)
        df['province_code'] = df['province_code'].astype(str).str.zfill(2)
    
    if 'municipality_code' in df.columns:
        # Convert municipality codes to strings (zero-padded to 5 digits)
        df['municipality_code'] = df['municipality_code'].astype(str).str.zfill(5)
    
    # Ensure population columns are numeric
    for col in df.columns:
        if 'population' in col:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df


def clean_dataframe(df: pd.DataFrame, year: Optional[int] = None) -> pd.DataFrame:
    """Clean the dataframe by removing empty rows/columns and fixing column names"""
    # Remove completely empty rows
    df = df.dropna(how='all')
    
    # Remove completely empty columns
    df = df.dropna(how='all', axis=1)
    
    # Standardize column names for demography data
    if year:
        df = standardize_demography_columns(df, year)
    else:
        # Generic column cleaning
        new_columns = []
        for col in df.columns:
            if pd.isna(col) or str(col).strip() == '':
                # Generate a name for unnamed columns
                new_columns.append(f'unnamed_column_{len(new_columns)}')
            else:
                # Clean the column name
                clean_name = str(col).strip()
                clean_name = clean_name.lower()
                clean_name = clean_name.replace(' ', '_')
                clean_name = clean_name.replace('/', '_')
                clean_name = clean_name.replace('-', '_')
                clean_name = clean_name.replace('(', '')
                clean_name = clean_name.replace(')', '')
                clean_name = clean_name.replace('.', '')
                clean_name = clean_name.replace(',', '')
                # Remove any remaining special characters
                clean_name = ''.join(c for c in clean_name if c.isalnum() or c == '_')
                # Ensure it doesn't start with a number
                if clean_name and clean_name[0].isdigit():
                    clean_name = 'col_' + clean_name
                new_columns.append(clean_name or f'unnamed_column_{len(new_columns)}')
        
        df.columns = new_columns
    
    # Remove rows that appear to be subtotals or totals (often contain aggregate data)
    # Look for rows where the first column contains words like 'total', 'suma', etc.
    if len(df.columns) > 0:
        first_col = df.columns[0]
        if first_col in df.columns:
            df = df[~df[first_col].astype(str).str.lower().str.contains('total|suma|agregado', na=False)]
    
    return df