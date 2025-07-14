import pandas as pd

# Change these as needed
# CSV_PATH = "data/res250714_400_filtered.csv"
CSV_PATH = "data/res250714_300_dropped_cols.csv"
# REPORT_DATE_COL = "Дата отчетности должника"
REPORT_DATE_COL = "Дата последней отчетности"

df = pd.read_csv(CSV_PATH)

# Find rows with missing or empty report date
missing_mask = ~df[REPORT_DATE_COL].apply(lambda x: isinstance(x, (int, float)) and not pd.isna(x))
missing_rows = df[missing_mask]

count = len(missing_rows)
print(f"Rows with missing '{REPORT_DATE_COL}': {count}")

if count > 0:
    print("Random example row:")
    def save_missing_date_rows(df, col_name, output_csv):
        missing_mask = ~df[col_name].apply(lambda x: isinstance(x, (int, float)) and not pd.isna(x))
        missing_rows = df[missing_mask]
        missing_rows.to_csv(output_csv, index=False)
        print(f"Saved {len(missing_rows)} rows with missing '{col_name}' to '{output_csv}'.")

    # Example usage:
    save_missing_date_rows(df, REPORT_DATE_COL, "data/res250714_350_only_missing_dates.csv")