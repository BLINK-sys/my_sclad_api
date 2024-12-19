import sqlite3
import time
import requests
from datetime import datetime


class MoySkladClient:
    BASE_URL = 'https://api.moysklad.ru/api/remap/1.2/'

    def __init__(self, username, password):
        self.session = requests.Session()
        self.session.auth = (username, password)

    def get(self, endpoint, params=None):
        if endpoint.startswith("http"):
            url = endpoint
        else:
            url = self.BASE_URL + endpoint
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def get_supply(self, organization_url, start_date, limit=1000, offset=0):
        params = {
            'filter': f'organization={organization_url};moment>={start_date}',
            'limit': limit,
            'offset': offset
        }
        return self.get('entity/supply', params=params)


# Функция для очистки данных по приходам
def clear_existing_prihod_data(start_date):
    try:
        conn = sqlite3.connect('sales_data.db')
        cursor = conn.cursor()
        cursor.execute('DELETE FROM prihod WHERE date >= ?', (start_date + " 00:00:00",))
        conn.commit()
        conn.close()
        print(f"Удалены записи приходов, начиная с даты {start_date}.")
    except sqlite3.OperationalError as e:
        print(f"Ошибка при очистке данных: {e}")
    except Exception as e:
        print(f"Неожиданная ошибка при очистке данных: {e}")


# Функция для сохранения данных о приходах в базу
def save_prihod_data(document_number, date, supplier, positions):
    while True:
        try:
            conn = sqlite3.connect('sales_data.db')
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS prihod (
                    document_number TEXT,
                    date TEXT,
                    supplier TEXT,
                    product TEXT,
                    quantity INTEGER,
                    price INTEGER
                )
            ''')
            for position in positions:
                product = position['product']
                quantity = position['quantity']
                price = position['price']

                cursor.execute('SELECT * FROM prihod WHERE document_number = ? AND date = ? AND product = ?',
                               (document_number, date, product))
                if not cursor.fetchone():
                    cursor.execute('''
                        INSERT INTO prihod (document_number, date, supplier, product, quantity, price)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (document_number, date, supplier, product, quantity, price))
            conn.commit()
            conn.close()
            break
        except sqlite3.OperationalError as e:
            print(f"Ошибка записи в базу данных: {e}. Повторная попытка...")
            time.sleep(1)
        except Exception as e:
            print(f"Неожиданная ошибка: {e}. Повторная попытка...")
            time.sleep(1)


# Функция для экспорта данных по приходам
def export_prihod_data(start_date):
    clear_existing_prihod_data(start_date)
    formatted_start_date = f"{start_date} 00:00:00"

    sklad = MoySkladClient('admin@bayzak1', 'Pospro2023!')
    organization_url = 'https://api.moysklad.ru/api/remap/1.2/entity/organization/092e4f5f-2391-11e9-9109-f8fc00017cb3'

    limit = 1000
    offset = 0

    while True:
        response = sklad.get_supply(organization_url, formatted_start_date, limit, offset)
        if 'rows' not in response or not response['rows']:
            break

        for item in response['rows']:
            name = item.get('name')
            moment = item.get('moment').replace(".000", "")

            # Информация о поставщике
            supplier = item.get('agent', {}).get('name', 'Неизвестный поставщик')

            positions_list = []
            positions_meta = item.get('positions')
            if positions_meta:
                positions_href = positions_meta.get('meta', {}).get('href')
                if positions_href:
                    positions_response = sklad.get(positions_href)

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

            save_prihod_data(name, moment, supplier, positions_list)
        offset += limit
    print('Закончен сбор данных по приходам с даты: ' + str(start_date))


#start_date = '2021-01-01'
#export_prihod_data(start_date)
