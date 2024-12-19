import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime
import calendar
import sqlite3
import time

# Путь к базе данных SQLite
DB_PATH = 'sales_data.db'

username = "admin@bayzak1"
password = "Pospro2023!"
headers = {
    "Content-Type": "application/json"
}


# Функция для удаления данных из базы начиная с указанной даты
def clear_stock_data(start_date):
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Удаление всех записей, где start_date_str >= start_date
        cursor.execute('DELETE FROM stock_data WHERE start_date_str >= ?', (start_date,))
        conn.commit()
        conn.close()
        print(f"Удалены записи, начиная с даты {start_date}.")
    except sqlite3.OperationalError as e:
        print(f"Ошибка при очистке данных: {e}")
    except Exception as e:
        print(f"Неожиданная ошибка при очистке данных: {e}")


# Функция для сохранения данных в базу
def save_to_db(product_name, product_code, stock_quantity, start_date_str):
    while True:  # Бесконечный цикл для повторных попыток записи
        try:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()

            cursor.execute('''
                    CREATE TABLE IF NOT EXISTS stock_data (
                        product_name TEXT,
                        product_code TEXT,
                        stock_quantity INTEGER,
                        start_date_str TEXT
                    )
                ''')

            # Проверяем, существует ли запись с тем же продуктом и датой
            cursor.execute('SELECT * FROM stock_data WHERE product_code = ? AND start_date_str = ?',
                           (product_code, start_date_str))
            existing = cursor.fetchone()

            if not existing:
                cursor.execute('''
                    INSERT INTO stock_data (product_name, product_code, stock_quantity, start_date_str)
                    VALUES (?, ?, ?, ?)
                ''', (product_name, product_code, stock_quantity, start_date_str))

            conn.commit()
            conn.close()
            break  # Если запись успешна, выходим из цикла

        except sqlite3.OperationalError as e:
            print(f"Ошибка записи в базу данных: {e}. Повторная попытка...")
            time.sleep(1)  # Ожидание перед повторной попыткой
        except Exception as e:
            print(f"Неожиданная ошибка: {e}. Повторная попытка...")
            time.sleep(1)  # Ожидание перед повторной попыткой


# Основная функция для обработки данных
def products(start_date_str):
    # Очистка существующих данных с указанной даты
    clear_stock_data(start_date_str)

    url = "https://api.moysklad.ru/api/remap/1.2/report/stock/all"
    store_ids = [
        "https://api.moysklad.ru/api/remap/1.2/entity/store/09304ed8-2391-11e9-9109-f8fc00017cb5",
        "https://api.moysklad.ru/api/remap/1.2/entity/store/2e8092ab-71bd-11ef-0a80-0c29000f676c"
    ]

    current_date = datetime.now()
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")

    while start_date <= current_date:
        last_day_of_month = calendar.monthrange(start_date.year, start_date.month)[1]
        # Определяем конечный день для текущего месяца (либо last_day_of_month, либо текущая дата)
        end_day = min(last_day_of_month,
                      current_date.day) if start_date.month == current_date.month and start_date.year == current_date.year else last_day_of_month

        # Вложенный цикл по дням до end_day
        for day in range(1, end_day + 1):
            try:
                current_day = start_date.replace(day=day)
            except ValueError:
                continue  # Пропускаем день, если такой день отсутствует (например, 30-е февраля)

            start_date_str = current_day.strftime("%Y-%m-%d")
            filter_store = ";".join([f"store={store_id}" for store_id in store_ids])
            more_data = True
            offset = 0

            while more_data:
                params = {
                    "filter": f"moment={start_date_str};{filter_store}",
                    "limit": 1000,
                    "offset": offset
                }
                try:
                    response = requests.get(url, headers=headers, auth=HTTPBasicAuth(username, password), params=params)
                    response.raise_for_status()
                except requests.exceptions.RequestException as e:
                    print(f"Ошибка при выполнении запроса: {e}")
                    break

                stock_data = response.json()

                if stock_data.get('rows'):
                    for item in stock_data.get('rows', []):
                        product_name = item.get('name', 'Товар не указан')
                        stock_quantity = item.get('stock', 0)
                        product_code = item.get('code', 'Код не указан')

                        #print(f"Товар: {product_name}")
                        #print(f"Код товара: {product_code}")
                        #print(f"Остаток: {stock_quantity}")
                        #print(f"Дата: {start_date_str}")
                        #print("-" * 50)

                        if stock_quantity > 0:
                            save_to_db(product_name, product_code, stock_quantity, start_date_str)
                else:
                    print(f"Нет данных за {start_date_str}")

                if len(stock_data.get('rows', [])) < 1000:
                    more_data = False
                else:
                    offset += 1000

        # Переход к следующему месяцу
        if start_date.month == 12:
            start_date = start_date.replace(year=start_date.year + 1, month=1, day=1)
        else:
            start_date = start_date.replace(month=start_date.month + 1, day=1)


# Вспомогательная функция для запуска products с передачей start_date
def run_products(start_date):
    # Преобразование даты в строку, если она передана как объект datetime
    if isinstance(start_date, datetime):
        start_date_str = start_date.strftime("%Y-%m-%d")
    else:
        start_date_str = start_date
    products(start_date_str)
    print('Закончен сбор остатков с даты:' + str(start_date))
