import pandas as pd

# Загрузка данных из CSV-файла
df = pd.read_csv('data_more_25.csv')

# Функция для добавления нуля к ИНН длиной 9 символов
def add_zero_to_inn(inn):
    if pd.notna(inn) and len(str(int(inn))) == 9:  # Проверка длины ИНН (исключая возможные NaN)
        return '0' + str(int(inn))
    return inn

# Применение функции к столбцу 'ИНН'
df['ИНН'] = df['ИНН'].apply(add_zero_to_inn)

# Сохранение изменений обратно в CSV-файл
df.to_csv('data_more_25_updated.csv', index=False, encoding='utf-8')