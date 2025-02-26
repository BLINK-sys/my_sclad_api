import json

from flask import Flask, jsonify, Response, request
from flask_cors import CORS
import sqlite3
from datetime import datetime
import threading
import time
import schedule

from chatgpt_api import gpt_api
from sales_actual import export_sales_data
from server_for_analiz_gpt import create_json_files, list_json_files
from stock_actual import run_products
from prihod_actual import export_prihod_data

app = Flask(__name__)
CORS(app)


# Function to get database connection
def get_db_connection():
    conn = sqlite3.connect('sales_data.db')
    conn.row_factory = sqlite3.Row
    return conn


# Helper function to parse date and extract year-month
def extract_year_month(date_str):
    try:
        return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m')
    except ValueError:
        try:
            return datetime.strptime(date_str, '%Y-%m-%d').strftime('%Y-%m')
        except ValueError:
            return None


@app.route('/sleep')
def sleep():
    sleep = [
        {
            "sleep": "Я проснулся!"
        }
    ]
    return jsonify(sleep)


@app.route('/data')
def get_data():
    data = [
        {
            "product": "Сканер Zebra",
            "competitors": [
                {"store": "PosPro", "price": "15000"},
                {"store": "Ozon", "price": "11000"}
            ]
        },
        {
            "product": "Принтер чеков",
            "competitors": [
                {"store": "PosPro", "price": "21000"},
                {"store": "WB", "price": "27000"},
                {"store": "Ozon", "price": "27000"},
                {"store": "Kaspi", "price": "20000"}
            ]
        },
        {
            "product": "Холодильник",
            "competitors": [
                {"store": "PosPro", "price": "240000"},
                {"store": "WB", "price": "213000"}
            ]
        }
    ]
    return jsonify(data)


# Route to process data and return the required summary
@app.route('/summary', methods=['GET'])
def get_summary():
    conn = get_db_connection()

    # Извлечение уникальных SKU из stock_data
    stock_query = """
        SELECT strftime('%Y-%m', start_date_str) AS month_year, product_code
        FROM stock_data
        GROUP BY month_year, product_code
    """
    stock_data = conn.execute(stock_query).fetchall()

    # Извлечение уникальных наименований из prihod за каждый месяц
    prihod_query = """
        SELECT strftime('%Y-%m', date) AS month_year, product AS product_name
        FROM prihod
        GROUP BY month_year, product_name
    """
    prihod_results = conn.execute(prihod_query).fetchall()

    # Извлечение уникальных проданных SKU из sales
    sku_query = """
        SELECT strftime('%Y-%m', date) AS month_year, COUNT(DISTINCT product) AS unique_sales_sku
        FROM sales
        GROUP BY month_year
    """
    sku_results = conn.execute(sku_query).fetchall()

    # Подсчет общей выручки по месяцам
    revenue_query = """
        SELECT strftime('%Y-%m', date) AS month_year, SUM(price * quantity) AS total_revenue
        FROM sales
        GROUP BY month_year
    """
    revenue_results = conn.execute(revenue_query).fetchall()

    conn.close()

    # Обработка данных
    stock_dict = {}
    for row in stock_data:
        month_year = row['month_year']
        product_code = row['product_code']
        if month_year not in stock_dict:
            stock_dict[month_year] = set()
        stock_dict[month_year].add(product_code)

    prihod_dict = {}
    for row in prihod_results:
        month_year = row['month_year']
        product_name = row['product_name']
        if month_year not in prihod_dict:
            prihod_dict[month_year] = set()
        prihod_dict[month_year].add(product_name)

    summary = []
    for month_year, stock_products in stock_dict.items():
        prihod_products = prihod_dict.get(month_year, set())

        # Добавляем недостающие товары из prihod
        unique_sku_count = len(stock_products | prihod_products)

        # Получаем данные о продажах SKU и выручке
        sku_data = next((sku for sku in sku_results if sku['month_year'] == month_year), None)
        revenue_data = next((rev for rev in revenue_results if rev['month_year'] == month_year), None)

        summary.append({
            'Дата': f'{month_year}-01',
            'SKU': unique_sku_count,
            'Продаж SKU': sku_data['unique_sales_sku'] if sku_data else 0,
            'Выручка': revenue_data['total_revenue'] if revenue_data else 0
        })

    return jsonify(summary)


@app.route("/files", methods=["GET"])
def files():
    return Response(json.dumps(list_json_files(), ensure_ascii=False), mimetype="application/json")


from flask import request, jsonify


@app.route("/gpt_analiz", methods=["GET", "POST"])
def gpt_analiz():
    if request.method == "GET":
        file_name = request.args.get("file_name")
        dostavka = request.args.get("dostavka", type=int)
        zapas = request.args.get("zapas", type=int)
    else:  # POST-запрос
        data = request.get_json()
        file_name = data.get("file_name")
        dostavka = data.get("dostavka", 0)
        zapas = data.get("zapas", 0)

    if not file_name:
        return jsonify({"error": "file_name is required"}), 400

    print(f"Генерация прогноза для товара{file_name}, со сроком доставки {dostavka} дней и запасом {zapas} шт.")

    return gpt_api(file_name, dostavka, zapas)


# Function to determine the start date and export sales data
def actual_date():
    start_date = datetime.now().replace(day=1).strftime("%Y-%m-%d")
    # start_date = '2024-12-01'
    export_sales_data(start_date)
    print(f"Data export completed for start date {start_date}", flush=True)


def actual_stock():
    start_date = datetime.now().replace(day=1).strftime("%Y-%m-%d")
    # start_date = '2024-12-01'
    run_products(start_date)
    print(f"Stock data update completed for start date {start_date}", flush=True)


def actual_prihod():
    start_date = datetime.now().replace(day=1).strftime("%Y-%m-%d")
    # start_date = '2024-12-01'
    export_prihod_data(start_date)
    print(f"Stock data update completed for start date {start_date}", flush=True)


def update_file_list():
    file_list = create_json_files()
    Response(json.dumps(file_list, ensure_ascii=False), mimetype="application/json")
    print(f"Обновление списка пакетов из директории завершено")


# Function to schedule daily task at 22:00
def schedule_task():
    schedule.every().day.at("11:15").do(actual_date)
    schedule.every().day.at("11:40").do(actual_stock)
    schedule.every().day.at("11:59").do(actual_prihod)
    schedule.every().day.at("12:15").do(update_file_list)
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute if the scheduled task should run


def run_initial_tasks():
    actual_date()
    threading.Timer(300, actual_stock).start()
    threading.Timer(600, actual_prihod).start()


def run_initial_tasks_gpt_files():
    update_file_list()


# Start the scheduling in a separate thread
task_thread = threading.Thread(target=schedule_task)
task_thread.start()

# поток запуска при старте
# initial_tasks_thread = threading.Thread(target=run_initial_tasks)
# initial_tasks_thread.start()

# поток запуска при старте
initial_tasks_thread = threading.Thread(target=run_initial_tasks_gpt_files)
initial_tasks_thread.start()

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=3000)
