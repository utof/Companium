import pandas as pd

# Global variables
CSV_PATH = "data/res250714_200_parsed.csv"
COLUMNS_TO_DROP = [
    'ОРГН', 'ИНН', 'КПП'  
]

def drop_columns_from_csv(csv_path, columns_to_drop):
    df = pd.read_csv(csv_path, encoding='utf-8')
    df = df.drop(columns=columns_to_drop, errors='ignore')
    return df

def keep_only_columns_from_csv(csv_path, columns_to_keep):
    df = pd.read_csv(csv_path, encoding='utf-8')
    df = df[columns_to_keep]
    return df

if __name__ == '__main__':
    # df_filtered = drop_columns_from_csv(CSV_PATH, COLUMNS_TO_DROP)
    df_filtered = keep_only_columns_from_csv(CSV_PATH, ['ИНН', 'Короткое название', 'Статус', 'Система налогообложения', 'Дата последней отчетности'])
    # Save to new file or print
    df_filtered.to_csv('res250714_300_dropped_cols.csv', index=False, encoding='utf-8')