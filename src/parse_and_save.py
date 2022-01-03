from datetime import datetime
from dateutil.relativedelta import relativedelta

import pandas as pd
import sqlalchemy as sa

PROJECT_PATH = '..'

wb_key = open(p_join(PROJECT_PATH, 'configs', 'wildberries_api64.txt'), mode='r', encoding='utf-8').read()
today = datetime.now().date()
yesterday = today - relativedelta(days=1)

# Поставки
print('Парсим поставки...')
supplies_df = parse_supplies(
    date_from=str(yesterday),
    wb_key=wb_key,
)
print('ок')

# Склад
print('Парсим склад...')
storage_df = parse_storage(
    date_from=str(yesterday),
    wb_key=wb_key,
)
print('ок')

# Заказы
print('Парсим заказы...')
purchase_df = parse_purchases(
    date_from=str(yesterday),
    wb_key=wb_key,
    flag=1,
)
print('ок')

# Продажи
print('Парсим продажи...')
sales_df = parse_sales(
    date_from=str(yesterday),
    wb_key=wb_key,
    flag=1,
)
print('ок')

# Отчёт о продажах по реализации
print('Парсим отчёт о продажах по реализации...')
report_df = parse_report(
    date_from=str(yesterday),
    wb_key=wb_key,
    limit=100_000,
)
print('ок')


sql_db_credentials = json.load(
    open(p_join(PROJECT_PATH, 'configs', 'sql_db_creadentials.json'), mode='r', encoding='utf-8')
)

print('Инициализируем подключение к db...')
db = 'yarik'
eng = sa.create_engine(
    f"mysql+pymysql://{sql_db_credentials[db]['login']}:{sql_db_credentials[db]['password']}@{sql_db_credentials[db]['host']}:{sql_db_credentials[db]['port']}"
)
eng.connect()
print('ок')

if sales_df is not None:
    if len(sales_df) > 0:
        print('Записываем продажи в db...')
        sales_df.to_sql(
            schema='wb_yarik',
            name='sales',
            con=eng,
            if_exists='append'
        )
        print('ок')

if purchase_df is not None:
    if len(purchase_df) > 0:
        print('Записываем заказы в db...')
        purchase_df.to_sql(
            schema='wb_yarik',
            name='purchases',
            con=eng,
            if_exists='append'
        )
        print('ок')

if storage_df is not None:
    if len(storage_df) > 0:
        print('Записываем склад в db...')
        storage_df.to_sql(
            schema='wb_yarik',
            name='storage',
            con=eng,
            if_exists='append'
        )
        print('ок')

if supplies_df is not None:
    if len(supplies_df) > 0:
        print('Записываем поставки в db...')
        supplies_df.to_sql(
            schema='wb_yarik',
            name='supplies',
            con=eng,
            if_exists='append'
        )
        print('ок')

if report_df is not None:
    if len(report_df) > 0:
        print('Записываем отчёт в db...')
        report_df.to_sql(
            schema='wb_yarik',
            name='report',
            con=eng,
            if_exists='append'
        )
        print('ок')
