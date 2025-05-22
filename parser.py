import requests
from bs4 import BeautifulSoup
import time
import random
import json
from typing import List, Dict, Optional, Any
import pandas as pd
import os


# 1. Загружаем CSV и получаем уникальные ИНН кредиторов
def load_unique_inn_list(filepath: str) -> List[str]:
    df = pd.read_csv(filepath)
    inn_list = df['creditor_inn'].dropna().astype(str).unique().tolist()
    return inn_list


# 2. Загружаем полные ИНН (включая повторы) — если нужно для пост-обработки
def load_full_inn_list(filepath: str) -> List[str]:
    df = pd.read_csv(filepath)
    return df['creditor_inn'].dropna().astype(str).tolist()


# Используем уникальные ИНН для парсинга
INN_LIST = load_unique_inn_list("result1.csv")

# Конфигурация
# INN_LIST = ["7447211759", "2308119595", "7744000912"]  # Ваш список ИНН
BASE_URL = "https://companium.ru/search/tips?query="
DETAILS_URL = "https://companium.ru"
DELAY_RANGE = (1, 3)  # Случайная задержка между запросами
MAX_RETRIES = 3  # Максимальное количество попыток
TIMEOUT = 10  # Таймаут запроса
COOKIES = {
    '_ym_uid': '1747066760757332821',
    '_ym_isad': '2',
    '_companium_ru_session': 'xc%2F4BDXj6uc62w%2FzDNZNeG%2B1JkJYcD4Mt15Napzw5xdZ9RS1ZRMqAm4G1UZgkAdjvp5Fy0cTtKtcCiqCqN19BZpoDgc0lHye4dFAzjJLd9DEr5lYEpCvI4tJElzX5DrQHb8vwWoffDZxkAflbHJuaxeJ%2BLeKW%2FJ%2B6Y%2Fxw9AEwlIyS7nWRVyAHo%2FCSRDyvBUYvyPZaK%2B3R5RHYvCJbyRowR%2FMXbO6YUh%2F8lZeKVZdPCfO37FhjYc1SeC7tb0N40s9DggseZ3cBFcyDS79kgnT5CeGXTn7k9i88ZSs6%2BM%3D--rDEsdVTSYE%2FkEQVa--N8LadXM8e7WFcRFXlln98g%3D%3D',
    '_ym_d': '1747591126'
}
HEADERS = {
    'authority': 'companium.ru',
    'accept': '*/*',
    'accept-language': 'ru,en;q=0.9,en-GB;q=0.8,en-US;q=0.7',
    'referer': 'https://companium.ru/',
    'sec-ch-ua': '"Chromium";v="136", "Microsoft Edge";v="136", "Not.A/Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0'
}


def create_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    session.cookies.update(COOKIES)
    return session


def random_delay():
    time.sleep(random.uniform(*DELAY_RANGE))


def extract_link(content: str) -> Optional[str]:
    try:
        href_start = content.find('href="') + 6
        href_end = content.find('"', href_start)
        return content[href_start:href_end]
    except (IndexError, ValueError):
        return None


def fetch_company_link(session: requests.Session, inn: str) -> Optional[str]:
    for attempt in range(MAX_RETRIES):
        try:
            random_delay()
            response = session.get(f"{BASE_URL}{inn}", timeout=TIMEOUT)

            if response.status_code == 200:
                data = response.json()
                if data and isinstance(data, list):
                    result = data[0]
                    link = extract_link(result.get('content', ''))
                    return f"{DETAILS_URL}{link}" if link else None

            elif response.status_code == 429:
                wait_time = 5 * (attempt + 1)
                print(f"Ошибка 429 для ИНН {inn}. Жду {wait_time} сек...")
                time.sleep(wait_time)
                continue

        except (requests.RequestException, ValueError) as e:
            print(f"Ошибка для ИНН {inn} (попытка {attempt + 1}): {str(e)}")
            time.sleep(2)

    return None


def parse_company_page(html):
    data = {}
    try:
        soup = BeautifulSoup(html, 'html.parser')

        def get_copy_value(id_):
            el = soup.find(id=id_)
            return el.get_text(strip=True) if el else None

        def get_block_value(label):
            block = soup.find('div', string=label)
            if not block:
                block = soup.find('div', class_='fw-bold', string=label)
            if block:
                sibling = block.find_next_sibling('div')
                if sibling:
                    return sibling.get_text(strip=True)
            return None

        # Основные реквизиты
        data['ОРГН'] = get_copy_value('copy-ogrn')
        data['ИНН'] = get_copy_value('copy-inn')
        data['КПП'] = get_copy_value('copy-kpp')
        data['ОКПО'] = get_copy_value('copy-okpo')
        data['Адрес'] = get_copy_value('copy-address')

        # Названия
        data['Короткое название'] = soup.find('h1', class_="mb-2").text
        data['Полное название'] = soup.find('div', class_="fw-bold mb-2").text

        # Статус и форма
        data['Статус'] = None
        if soup.find('div', class_="text-success fw-bold"):
            data['Статус'] = soup.find('div', class_="text-success fw-bold").text
        elif soup.find('div', class_="text-danger fw-bold"):
            data['Статус'] = soup.find('div', class_="text-danger fw-bold").text
        else:
            data['Статус'] = soup.find('div', class_="fw-bold special-status").text

        data['Организационно-правовая форма'] = get_block_value('Организационно-правовая форма')
        data['Форма собственности'] = get_block_value('Форма собственности')

        block_sn = soup.find('div', class_="fw-bold", string='Система налогообложения')
        if block_sn:
            parent_div = block_sn.parent
            value_div = block_sn.find_next_sibling('div')
            comment_div = block_sn.find_next_sibling('div', class_="text-secondary")

            data['Система налогообложения'] = (
                    (value_div.get_text(strip=True) if value_div else '') +
                    " " +
                    (comment_div.get_text(strip=True) if comment_div else '')
            )
        else:
            data['Система налогообложения'] = None

        # Финансовая отчетность
        # Создаем словарь для хранения данных
        financial_data = {
            'Период': '',
            'Значения': []
        }

        # Получаем период отчетности
        period_header = soup.find('div', class_="fw-bold",
                                  string=lambda text: 'Финансовая отчетность' in text if text else False)
        if period_header:
            financial_data['Период'] = period_header.get_text(strip=True)

        # Парсим все элементы финансовой отчетности
        try:
            for item in period_header.find_next_siblings('div'):
                # Получаем название показателя
                name = item.find('a', class_='link-pseudo')
                if not name:
                    continue

                # Получаем значение показателя
                value = ''.join([text for text in item.stripped_strings][1:]).split('&nbsp;')[0].strip()

                # Получаем изменение (если есть)
                change = item.find('span', class_='financial-statement-change')
                change_data = {
                    'value': change.get_text(strip=True) if change else None,
                    'tooltip': change.get('data-bs-title') if change else None
                } if change else None

                financial_data['Значения'].append({
                    'name': name.get_text(strip=True),
                    'value': value,
                    'change': change_data
                })

            data['Финансовая отчетность'] = financial_data
        except Exception:
            pass

        # Генеральный директор

        ceo_block = soup.find('div', class_='flex-grow-1 ms-3')
        org_blocks = soup.find_all('div', class_='mb-3')
        org_block = None
        for block in org_blocks:
            if block.find('div', class_='fw-bold', string='Управляющая организация'):
                org_block = block
                break
        if ceo_block:
            # Извлекаем данные
            ceo_data = {
                'Должность': ceo_block.find('strong', class_='fw-bold').get_text(strip=True),
                'Имя': ceo_block.find('a').get_text(strip=True),
                'Ссылка': ceo_block.find('a')['href'],
                'ИНН': ceo_block.find('span', class_='copy').get_text(strip=True)
            }
            data['Генеральный директор'] = ceo_data
        elif org_block:
            org_data = {
                'type': org_block.find('div', class_='fw-bold').get_text(strip=True),
                'name': org_block.find('a').get_text(strip=True),
                'link': org_block.find('a')['href'],
                'since': org_block.find_next('div', class_='text-secondary').get_text(strip=True)
            }
            data['Управляющая компания'] = org_data

        # Учредители
        # Находим блок учредителей
        founders_blocks = soup.find_all('div', class_='mb-3')
        founders_block = None

        for block in founders_blocks:
            title = block.find('strong', class_=['fw-bold', 'fu-bold'],
                               string=lambda t: t and 'Учредител' in t)
            if title:
                founders_block = block
                break

        if founders_block:
            # Ищем только основную ссылку на учредителя (не history)
            founder_link = founders_block.find('a', href=True, class_=lambda x: x != 'history')

            if founder_link:
                f_data = {
                    'Тип': 'Учредитель',
                    'Имя': founder_link.get_text(strip=True),
                    'Ссылка': founder_link['href'],
                    'С какого момента': (founders_block.find('div', class_='text-secondary').get_text(strip=True)
                                         if founders_block.find('div', class_='text-secondary') else None)
                }
            else:
                # Обработка случая "Нет сведений"
                no_data = founders_block.find(string=lambda t: t and "Нет сведений" in t)
                f_data = {
                    'Информация': no_data.strip() if no_data else 'Нет данных'
                }
        else:
            f_data = {
                'Ошибка': 'Блок не найден'
            }

        data['Учредители'] = f_data

        # Санкции
        sanctions_block = soup.find('div', string='Санкционные списки')
        if sanctions_block:
            sanctions_info = sanctions_block.find_next('div')
            if sanctions_info:
                data['Санкционные списки'] = sanctions_info.get_text(strip=True)

        # Контактные данные
        # Находим все номера телефонов
        phone_numbers = [a.get_text(strip=True) for a in soup.select('a.link-black[href^="tel:"]')]
        data['Телефоны'] = phone_numbers

        emails = [a.get_text(strip=True) for a in soup.select('a[href^="mailto:"]')]
        data['Электронные почты'] = emails

        # 1. Находим тег strong с названием компании
        company_tag = soup.find('strong', class_='fw-bold d-block mt-3 mb-1')
        websites = []
        if company_tag:
            # 2. Находим все последующие теги 'a' с веб-сайтами
            for sibling in company_tag.find_next_siblings():
                if sibling.name == 'a' and sibling.get('href', '').startswith('http'):
                    websites.append({
                        'name': sibling.get_text(strip=True),
                        'url': sibling['href']
                    })
                # Прерываем цикл, если встречаем другой strong тег
                elif sibling.name == 'strong':
                    break

        data['Веб сайты'] = websites

        # Виды деятельности
        table = soup.find_all("table", class_="table table-md table-striped")[-1]
        d = []

        if table:
            rows = table.find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 2:
                    code = cols[0].get_text(strip=True)
                    link = cols[1].find('a')
                    if link:
                        href = link.get('href', '')
                        text = link.get_text(strip=True)
                    else:
                        href = ''
                        text = cols[1].get_text(strip=True)

                    extra_tip = cols[1].find('span', class_='extra-tip')
                    tip = extra_tip.get_text(strip=True) if extra_tip else ''

                    d.append({
                        'code': code,
                        'text': text,
                        'href': href,
                        'extra_tip': tip
                    })
        data['Виды деятельности'] = d

        # Контракты по госзакупкам
        section = soup.find_all('section', class_='x-section')[9]
        text = section.get_text(" ", strip=True)

        d = {}

        # Проверка на отсутствие данных
        if 'Нет сведений об участии компании' in text:
            d['Наличие контрактов по госзакупкам'] = False
        else:
            # Извлекаем количество контрактов и общую сумму
            try:
                contract_text = section.find('div', class_='mb-2').text.strip()
                contract_count = int(contract_text.split()[0])
                amount_tag = section.find('a', class_='link-black')
                amount_value = amount_tag.text.strip().split()[0].replace(',', '.')
                amount_unit = amount_tag.find('span').text.strip()
                total_amount = float(amount_value)

                # Заказчик и Поставщик суммы
                buttons = section.find_all('button', class_='nav-link')
                customer_amount = None
                customer_unit = None
                supplier_amount = None
                supplier_unit = None
                for button in buttons:
                    btn_text = button.text
                    span = button.find('span', class_='text-muted fw-400')
                    if not span:
                        continue
                    amount_parts = span.text.strip().split()
                    if len(amount_parts) >= 2:
                        amount_val = float(amount_parts[0].replace(',', '.'))
                        unit = ' '.join(amount_parts[1:])
                        if 'Заказчик' in btn_text:
                            customer_amount = amount_val
                            customer_unit = unit
                        elif 'Поставщик' in btn_text:
                            supplier_amount = amount_val
                            supplier_unit = unit

                d['Наличие данных'] = True
                d['Контракт'] = str(contract_count)
                d['Сумма'] = str(total_amount) + " " + amount_unit  # в млрд руб.
                d['Заказчик'] = str(customer_amount) + " " + customer_unit
                d['Поставщик'] = str(supplier_amount) + " " + supplier_unit
            except Exception as e:
                d['Наличие контрактов по госзакупкам'] = False

        data['Контракты по госзакупкам'] = d

        return data
    except Exception as e:
        print(f"Ошибка {e}")
        return data


def fetch_company_details(session: requests.Session, url: str) -> Optional[Dict[str, Any]]:
    for attempt in range(MAX_RETRIES):
        try:
            random_delay()
            response = session.get(url, timeout=TIMEOUT)

            if response.status_code == 200:
                return parse_company_page(response.text)

            elif response.status_code == 429:
                wait_time = 5 * (attempt + 1)
                print(f"Ошибка 429 при запросе {url}. Жду {wait_time} сек...")
                time.sleep(wait_time)
                continue

        except (requests.RequestException, ValueError) as e:
            print(f"Ошибка при запросе {url} (попытка {attempt + 1}): {str(e)}")
            time.sleep(2)

    return None


CACHE_FILE = "inn_cache.json"


# Загрузка кэша
def load_cache() -> Dict[str, Any]:
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


# Сохранение кэша
def save_cache(cache: Dict[str, Any]):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def process_inn_list(inn_list: List[str]) -> List[Dict[str, Any]]:
    session = create_session()
    results = []

    cache = load_cache()

    for i, inn in enumerate(inn_list, 1):
        print(f"Обрабатываю ИНН {i}/{len(inn_list)}: {inn}")

        if inn in cache:
            print(f"[КЭШ] Используется сохранённый результат для ИНН: {inn}")
            results.append(cache[inn])
            continue

        link = fetch_company_link(session, inn)

        if link:
            print(f"Найдена ссылка: {link}")
            company_data = fetch_company_details(session, link)
            if company_data:
                cache[inn] = company_data
                results.append(company_data)
                save_cache(cache)  # можно делать реже, но так надёжнее
        else:
            print(f"Не удалось получить ссылку для ИНН: {inn}")

        if i % 50 == 0:
            session.close()
            session = create_session()

    session.close()
    return results


def save_results_to_json(results: List[Dict[str, Any]], filename: str = 'companium_data.json'):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"Результаты сохранены в {filename}")


def save_results_to_csv(data: List[Dict[str, Any]], filename: str):
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)


if __name__ == "__main__":
    links = process_inn_list(INN_LIST)
    save_results_to_csv(links, "data_more_25.csv")
    print(f"\nОбработка завершена. Получено {len(links)} карточек компаний из {len(INN_LIST)} ИНН.")
