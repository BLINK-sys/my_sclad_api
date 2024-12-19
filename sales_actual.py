import sqlite3
import time
import requests
from datetime import datetime
from threading import Lock


class MoySkladClient:
    BASE_URL = 'https://api.moysklad.ru/api/remap/1.2/'

    def __init__(self, username, password):
        self.session = requests.Session()
        self.session.auth = (username, password)

    def get(self, endpoint, params=None):
        if endpoint.startswith("http"):  # Проверяем, является ли `endpoint` полным URL
            url = endpoint
        else:
            url = self.BASE_URL + endpoint
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def get_retail_demand(self, organization_url, start_date, limit=1000, offset=0):
        params = {
            'filter': f'organization={organization_url};moment>={start_date}',
            'limit': limit,
            'offset': offset
        }
        return self.get('entity/retaildemand', params=params)


# Функция для удаления данных из базы начиная с указанной даты
def clear_existing_data(start_date):
    try:
            conn = sqlite3.connect('sales_data.db')
            cursor = conn.cursor()
            cursor.execute('DELETE FROM sales WHERE date >= ?', (start_date + " 00:00:00",))
            conn.commit()
            conn.close()
            print(f"Удалены записи, начиная с даты {start_date}.")
    except sqlite3.OperationalError as e:
        print(f"Ошибка при очистке данных: {e}")
    except Exception as e:
        print(f"Неожиданная ошибка при очистке данных: {e}")


# Функция для получения данных о сотруднике с обработкой ошибок
def get_employee_data(employee, sklad):
    try:
        if employee.startswith(MoySkladClient.BASE_URL):
            return requests.get(employee, auth=sklad.session.auth).json()
        else:
            return sklad.get(employee)
    except requests.RequestException as e:
        print(f"Ошибка получения данных сотрудника: {e}")
        return {"name": "Неизвестный пользователь"}


# Функция для получения данных о позициях с обработкой ошибок
def get_positions_data(positions_href, sklad):
    try:
        if positions_href.startswith(MoySkladClient.BASE_URL):
            return requests.get(positions_href, auth=sklad.session.auth).json()
        else:
            return sklad.get(positions_href)
    except requests.RequestException as e:
        print(f"Ошибка получения данных позиций: {e}")
        return {"rows": []}


# Функция для сохранения данных в базу
def save_sales_data(document_number, date, seller, positions):
    while True:
        try:
                conn = sqlite3.connect('sales_data.db')
                cursor = conn.cursor()
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS sales (
                        document_number TEXT,
                        date TEXT,
                        seller TEXT,
                        product TEXT,
                        quantity INTEGER,
                        price INTEGER
                    )
                ''')
                for position in positions:
                    product = position['product']
                    quantity = position['quantity']
                    price = position['price']

                    # Проверка на дубликаты перед вставкой
                    cursor.execute('SELECT * FROM sales WHERE document_number = ? AND date = ? AND product = ?',
                                   (document_number, date, product))
                    if not cursor.fetchone():
                        cursor.execute('''
                            INSERT INTO sales (document_number, date, seller, product, quantity, price)
                            VALUES (?, ?, ?, ?, ?, ?)
                        ''', (document_number, date, seller, product, quantity, price))
                conn.commit()
                conn.close()
                break
        except sqlite3.OperationalError as e:
            print(f"Ошибка записи в базу данных: {e}. Повторная попытка...")
            time.sleep(1)
        except Exception as e:
            print(f"Неожиданная ошибка: {e}. Повторная попытка...")
            time.sleep(1)


# Функция для экспорта данных о продажах
def export_sales_data(start_date):
    clear_existing_data(start_date)
    formatted_start_date = f"{start_date} 00:00:00"

    sklad = MoySkladClient('admin@bayzak1', 'Pospro2023!')
    organization_url = 'https://api.moysklad.ru/api/remap/1.2/entity/organization/092e4f5f-2391-11e9-9109-f8fc00017cb3'

    limit = 1000
    offset = 0

    while True:
        response = sklad.get_retail_demand(organization_url, formatted_start_date, limit, offset)
        if 'rows' not in response or not response['rows']:
            break

        for item in response['rows']:
            name = item.get('name')
            moment = item.get('moment').replace(".000", "")
            total_sum = item.get('sum') / 100

            # Информация о продавце (пользователе)
            employee = item.get('owner', {}).get('meta', {}).get('href')
            if employee:
                employee_data = get_employee_data(employee, sklad)
                employee_name = employee_data.get('name', 'Неизвестный пользователь')
            else:
                employee_name = "Неизвестный пользователь"

            # print("-" * 50)
            # print(f"Документ: {name}, Дата: {moment}, Сумма: {int(total_sum)}, Продавец: {employee_name}")

            positions_list = []
            positions_meta = item.get('positions')
            if positions_meta:
                positions_href = positions_meta.get('meta', {}).get('href')
                if positions_href:
                    positions_response = get_positions_data(positions_href, sklad)

                    if 'rows' in positions_response:
                        for position in positions_response['rows']:
                            assortment_meta = position.get('assortment', {}).get('meta', {})
                            if assortment_meta:
                                assortment_href = assortment_meta.get('href')
                                assortment_data = sklad.get(assortment_href)
                                position_name = assortment_data.get('name', "Неизвестный товар")
                            else:
                                position_name = "Неизвестный товар"

                            quantity = position.get('quantity')
                            price = position.get('price') / 100

                            # print(f"\tПозиция: {position_name}, Кол-во: {int(quantity)}, Цена: {int(price)}")

                            positions_list.append({
                                'product': position_name,
                                'quantity': int(quantity),
                                'price': int(price)
                            })
                    else:
                        print("\tНет позиций в документе.")
                else:
                    print("\tНе удалось получить ссылку на позиции.")
            else:
                print("\tНет информации о позициях.")

            save_sales_data(name, moment, employee_name, positions_list)
        offset += limit
    print('Закончен сбор продаж с даты: ' + str(start_date))
