import sqlite3
import json
import os
import re


# DB_PATH = "/var/data/sales_data.db"
# JSON_DIR_PATH = "/var/data/products_json/"

DB_PATH = "sales_data.db"
JSON_DIR_PATH = "products_json/"

# Создаём папку для JSON, если её нет
os.makedirs(JSON_DIR_PATH, exist_ok=True)


def sanitize_filename(name):
    """Заменяем недопустимые символы на подчеркивания."""
    return re.sub(r'[<>:"/\\|?*]', '_', name)


def list_json_files():
    """
    Возвращает список всех файлов в директории JSON_DIR_PATH.
    Не создаёт/не обновляет никаких файлов — только читает директорию.
    """
    return [
        f for f in os.listdir(JSON_DIR_PATH)
        if os.path.isfile(os.path.join(JSON_DIR_PATH, f))
    ]


def create_json_files():
    """
    1. Удаляем все файлы в JSON_DIR_PATH (полная очистка папки).
    2. Подключаемся к БД, берём список товаров и их остатки.
    3. По каждому товару формируем JSON, сохраняем на диск.
    4. Возвращаем список (уже новых) файлов в директории.
    """
    # Шаг 1. Полная очистка папки с JSON‑файлами
    existing_files = list_json_files()
    for filename in existing_files:
        os.remove(os.path.join(JSON_DIR_PATH, filename))

    # Шаг 2. Подключаемся к БД и получаем товары
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT DISTINCT product_name, stock_quantity
        FROM stock_data
        WHERE stock_quantity > 0
    """)
    products = cursor.fetchall()

    # Шаг 3. Формируем для каждого товара отдельный JSON‑файл
    for product_name, stock_quantity in products:
        product_data = {
            "product_name": product_name,
            "stock": stock_quantity,
            "history": []
        }

        # Собираем приходы и продажи
        cursor.execute("""
            SELECT date, quantity, price, 'prihod' AS type
            FROM prihod
            WHERE product = ? AND date >= '2024-06-01'
            UNION ALL
            SELECT date, quantity, price, 'sales' AS type
            FROM sales
            WHERE product = ? AND date >= '2024-06-01'
            ORDER BY date
        """, (product_name, product_name))
        transactions = cursor.fetchall()

        current_stock = stock_quantity

        for date, quantity, price, trans_type in transactions:
            if trans_type == 'prihod':
                product_data["history"].append({
                    "type": "prihod",
                    "date": date,
                    "quantity": quantity,
                    "price": price,
                    "stock_after": current_stock
                })
                current_stock += quantity
            else:  # sales
                product_data["history"].append({
                    "type": "sales",
                    "date": date,
                    "quantity": quantity,
                    "price": price,
                    "stock_after": current_stock - quantity
                })
                current_stock -= quantity

        # Сохраняем в отдельный JSON‑файл
        json_file_path = os.path.join(JSON_DIR_PATH, f"{sanitize_filename(product_name)}.json")
        with open(json_file_path, "w", encoding="utf-8") as f:
            json.dump(product_data, f, ensure_ascii=False, indent=4)

    conn.close()

