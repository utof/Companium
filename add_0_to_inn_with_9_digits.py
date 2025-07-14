import pandas as pd

# Загрузка данных из CSV-файла
df = pd.read_csv('data/cleaned_filtered_merged_debt_creditors.csv', encoding='utf-8')

# Функция для добавления нуля к ИНН длиной 9 символов
def add_zero_to_inn(inn):
    if pd.notna(inn) and len(str(int(inn))) == 9:  # Проверка длины ИНН (исключая возможные NaN)
        return '0' + str(int(inn))
    return inn

# print("Количество ИНН, к которым будет добавлен ноль:", df['debtor_inn'].apply(lambda x: len(str(int(x))) == 9).sum())



# Применение функции к столбцу 'debtor_inn'
df['debtor_inn'] = df['debtor_inn'].apply(add_zero_to_inn)


# Сохранение изменений обратно в CSV-файл
df.to_csv('data/cleaned___debt_creditors_add0.csv', index=False, encoding='utf-8')