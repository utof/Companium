import pandas as pd

def inspect_debtor_inn_column(csv_path):
    """
    Inspects the debtor_inn column for potential issues that could cause NoneType errors.
    Returns the full column and prints diagnostic information.
    """
    # Read CSV with debtor_inn as string
    df = pd.read_csv(csv_path, dtype={'debtor_inn': str}, keep_default_na=False)
    
    # Get the column
    inn_column = df['debtor_inn']
    
    # Print diagnostic info
    print("=== Column Overview ===")
    print(f"Total rows: {len(inn_column)}")
    print(f"Non-empty values: {inn_column.notna().sum()}")
    print(f"Empty strings: {(inn_column == '').sum()}")
    print(f"'None' strings: {(inn_column == 'None').sum()}")
    print(f"'nan' strings: {(inn_column == 'nan').sum()}")
    print("\n=== Sample Values ===")
    print(inn_column.head(20))
    
    # Find problematic rows
    problematic = df[df['debtor_inn'].isna() | (df['debtor_inn'] == '')]
    if not problematic.empty:
        print("\n=== Problematic Rows ===")
        print(problematic)
    
    return inn_column

# Usage:
inn_data = inspect_debtor_inn_column("data/cleaned___debt_creditors_add0.csv")