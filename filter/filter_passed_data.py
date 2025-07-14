import pandas as pd
import numpy as np
from datetime import datetime
import re
import ast

def safe_literal_eval(val):
    """Safely evaluate string containing Python literals"""
    if pd.isna(val) or val == '':
        return []
    try:
        return ast.literal_eval(val)
    except (ValueError, SyntaxError):
        return []

def load_data(companium_path: str, main_data_path: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load both datasets with INN as strings"""
    # Load companium data
    companium_df = pd.read_csv(
        companium_path,
        dtype={'ИНН': str},
        converters={
            'Дата последней отчетности': lambda x: float(x) if str(x).replace('.','').isdigit() else np.nan
        }
    )
    
    # Load main data with safe literal evaluation
    main_df = pd.read_csv(
        main_data_path,
        dtype={'debtor_inn': str},
        converters={
            'Телефоны': safe_literal_eval,
            'Электронные почты': safe_literal_eval,
            'Веб сайты': safe_literal_eval
        }
    )
    
    return companium_df, main_df

def filter_bankrupt(df: pd.DataFrame) -> pd.DataFrame:
    """Remove bankrupt companies (optional)"""
    return df[~df['Статус'].str.contains('банкрот', case=False, na=False)]

def filter_liquidated(df: pd.DataFrame, min_years: float = 2.83) -> pd.DataFrame:
    """
    Enhanced to handle:
    - Russian date formats ("31 января 2025")
    - Multiple status lines
    - 'Исключение из ЕГРЮЛ' cases
    - Uses integer years and months for reliable date math
    """
    def extract_liquidation_date(status):
        if pd.isna(status):
            return pd.NaT
        
        # Handle both date formats
        date_patterns = [
            r'ликвидировано (\d{1,2} \w+ \d{4})',  # Russian dates
            r'ликвидировано (\d{4}-\d{2}-\d{2})',  # ISO dates
            r'Исключение из ЕГРЮЛ.*?(\d{4})'       # Exclusion year
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, status)
            if match:
                date_str = match.group(1)
                try:
                    # Parse Russian dates
                    if any(month in date_str for month in 
                          ['января','февраля','марта','апреля','мая','июня',
                           'июля','августа','сентября','октября','ноября','декабря']):
                        return pd.to_datetime(date_str, format='%d %B %Y')
                    # Parse year-only
                    elif len(date_str) == 4:
                        return pd.to_datetime(date_str + '-12-31')
                    # Parse ISO dates
                    else:
                        return pd.to_datetime(date_str)
                except:
                    continue
        return pd.NaT
    
    df['liquidation_date'] = df['Статус'].apply(extract_liquidation_date)
    
    # Convert fractional years to whole months (2.83 years ≈ 34 months)
    months_threshold = int(min_years * 12)
    threshold_date = datetime.now() - pd.DateOffset(months=months_threshold)
    
    # Only filter if liquidation date is older than threshold
    liquid_mask = df['liquidation_date'].notna()
    old_liquid_mask = df['liquidation_date'] < threshold_date
    
    return df[~(liquid_mask & old_liquid_mask)].drop(columns=['liquidation_date'])

def filter_old_reports(df: pd.DataFrame, max_years: int = 5) -> pd.DataFrame:
    """
    Modified to:
    - Always keep rows with NaN/empty reporting dates
    - Only filter out rows with dates older than threshold
    - Handle float-formatted years like 2022.0
    """
    # Convert float years to datetime (2022.0 → 2022-12-31)
    df['report_date'] = pd.to_datetime(
        df['Дата последней отчетности'].dropna().astype(int).astype(str) + '-12-31',
        errors='coerce'
    )
    
    threshold = datetime.now() - pd.DateOffset(years=max_years)
    
    # Keep rows where:
    # 1. Report date is NaN (missing) OR
    # 2. Report date is within threshold
    keep_mask = df['report_date'].isna() | (df['report_date'] >= threshold)
    return df[keep_mask].drop(columns=['report_date'])


def merge_and_enrich(main_df: pd.DataFrame, filtered_companium: pd.DataFrame) -> pd.DataFrame:
    """Merge data and add debtor info columns"""
    debtor_info = filtered_companium[['ИНН', 'Короткое название', 'Статус', 'Дата последней отчетности']]
    debtor_info.columns = ['debtor_inn', 'Название должника', 'Статус должника', 'Дата отчетности должника']
    return pd.merge(main_df, debtor_info, on='debtor_inn', how='left')

def propagate_debtor_info(df: pd.DataFrame) -> pd.DataFrame:
    """Fill debtor info for duplicate INNs (optional)"""
    cols = ['Название должника', 'Статус должника', 'Дата отчетности должника']
    for col in cols:
        df[col] = df.groupby('debtor_inn')[col].transform(lambda x: x.ffill().bfill())
    return df

def clean_empty_debtors(df: pd.DataFrame, require_all: bool = False) -> pd.DataFrame:
    """
    Remove rows with empty debtor info (optional)
    require_all=True: only remove if ALL debtor fields are empty
    require_all=False: remove if ANY debtor field is empty
    """
    debtor_cols = ['Название должника', 'Статус должника', 'Дата отчетности должника']
    if require_all:
        return df[df[debtor_cols].notna().any(axis=1)]
    return df[df[debtor_cols].notna().all(axis=1)]


def sort_by_empty_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sorts the DataFrame so that rows with more empty (NaN or empty string) columns are at the bottom.
    """
    # Count NaN or empty string per row
    empty_count = df.isna().sum(axis=1) + (df == '').sum(axis=1)
    return df.assign(_empty_count=empty_count).sort_values('_empty_count').drop(columns=['_empty_count']).reset_index(drop=True)

def save_result(df: pd.DataFrame, output_path: str):
    """Save final dataframe"""
    df.to_csv(output_path, index=False)

def main():
    # Configuration
    # COMPANIUM_PATH = "data/res250714_300_dropped_cols.csv"
    COMPANIUM_PATH = "data/res250714_300_dropped_cols.csv"
    MAIN_DATA_PATH = "data/cleaned___debt_creditors_add0.csv"
    OUTPUT_PATH = "data/res250714_400_filtered.csv"
    
    try:
        # Load data
        companium_df, main_df = load_data(COMPANIUM_PATH, MAIN_DATA_PATH)
        print("Data loaded successfully")
        
        # Process companium data
        filtered = companium_df.copy()
        filtered = filter_bankrupt(filtered)
        filtered = filter_liquidated(filtered)
        filtered = filter_old_reports(filtered)
        
        # Merge and process main data
        result = merge_and_enrich(main_df, filtered)
        result = propagate_debtor_info(result)
        result = clean_empty_debtors(result, require_all=True)
        result = sort_by_empty_columns(result)
        
        # Save result
        save_result(result, OUTPUT_PATH)
        print(f"Processing complete. Output saved to {OUTPUT_PATH}")
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    main()