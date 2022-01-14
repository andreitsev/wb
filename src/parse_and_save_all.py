import sys
sys.path.insert(0, '..')
import json
from os.path import join as p_join
from datetime import datetime
from dateutil.relativedelta import relativedelta

import pandas as pd
import sqlalchemy as sa

print('Now:', str(datetime.now()))

from src.parse_utils import (
    parse_supplies,
    parse_purchases,
    parse_sales,
    parse_storage,
    parse_report
)

PROJECT_PATH = '..'

wb_key = open(p_join(PROJECT_PATH, 'configs', 'wildberries_api64.txt'), mode='r', encoding='utf-8').read()
DATE = '2021-01-01'

# Поставки
print('Парсим поставки...')
supplies_df = parse_supplies(
    date_from=DATE,
    wb_key=wb_key,
)
print('ок')

# Склад
print('Парсим склад...')
storage_df = parse_storage(
    date_from=DATE,
    wb_key=wb_key,
)
print('ок')

# Заказы
print('Парсим заказы...')
purchase_df = parse_purchases(
    date_from=DATE,
    wb_key=wb_key,
    flag=0,
)
print('ок')

# Продажи
print('Парсим продажи...')
sales_df = parse_sales(
    date_from=DATE,
    wb_key=wb_key,
    flag=0,
)
print('ок')

# Отчёт о продажах по реализации
print('Парсим отчёт о продажах по реализации...')
report_df = parse_report(
    date_from=DATE,
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
        print('Записываем продажи в tmp базу...')
        sales_df.to_sql(
            schema='wb_yarik',
            name='sales_tmp',
            con=eng,
            if_exists='replace'
        )
        print("ок")
        print('t_main_sales union t_tmp_sales...')
        tmp_df = pd.read_sql(
                """
                with t_main_sales as
                    (
                        select
                            date
                            , lastChangeDate
                            , supplierArticle
                            , techSize
                            , barcode
                            , quantity
                            , totalPrice
                            , discountPercent
                            , isSupply
                            , isRealization
                            , orderId
                            , promoCodeDiscount
                            , warehouseName
                            , countryName
                            , oblastOkrugName
                            , regionName
                            , incomeID
                            , saleID
                            , odid
                            , spp
                            , forPay
                            , finishedPrice
                            , priceWithDisc
                            , nmId
                            , subject
                            , category
                            , brand
                            , IsStorno
                            , gNumber
                        from
                            wb_yarik.sales
                    ),

                    t_tmp_sales as
                    (
                        select
                            date
                            , lastChangeDate
                            , supplierArticle
                            , techSize
                            , barcode
                            , quantity
                            , totalPrice
                            , discountPercent
                            , isSupply
                            , isRealization
                            , orderId
                            , promoCodeDiscount
                            , warehouseName
                            , countryName
                            , oblastOkrugName
                            , regionName
                            , incomeID
                            , saleID
                            , odid
                            , spp
                            , forPay
                            , finishedPrice
                            , priceWithDisc
                            , nmId
                            , subject
                            , category
                            , brand
                            , IsStorno
                            , gNumber
                        from
                            wb_yarik.sales_tmp
                    )

                select 
                    *
                from
                    t_main_sales
                union 

                select 
                    *
                from
                    t_tmp_sales
                """,
                eng
            )
        print("ок")
        print('Сохраняем обновлённые sales...')
        try:
            tmp_df.to_sql(
                schema='wb_yarik',
                name='sales',
                con=eng,
                if_exists='replace'
            )
            print('ок')

        except:
            print('Что-то пошло не так: обновлённая таблица sales не сохранилась')

if purchase_df is not None:
    if len(purchase_df) > 0:
        print('Записываем заказы в tmp базу...')
        purchase_df.to_sql(
            schema='wb_yarik',
            name='purchases_tmp',
            con=eng,
            if_exists='replace'
        )
        print("ок")
        print('t_main_purchases union t_tmp_purchases...')
        tmp_df = pd.read_sql(
            """
            with t_main as
                (
                    select
                        date
                        , lastChangeDate
                        , supplierArticle
                        , techSize
                        , barcode
                        , quantity
                        , totalPrice
                        , discountPercent
                        , warehouseName
                        , oblast
                        , incomeID
                        , odid
                        , nmId
                        , subject
                        , category
                        , brand
                        , isCancel
                        , cancel_dt
                        , gNumber
                    from
                        wb_yarik.purchases
                ),

                t_tmp as
                (
                    select
                        date
                        , lastChangeDate
                        , supplierArticle
                        , techSize
                        , barcode
                        , quantity
                        , totalPrice
                        , discountPercent
                        , warehouseName
                        , oblast
                        , incomeID
                        , odid
                        , nmId
                        , subject
                        , category
                        , brand
                        , isCancel
                        , cancel_dt
                        , gNumber
                    from
                        wb_yarik.purchases_tmp
                )

            select 
                *
            from
                t_main
            union 

            select 
                *
            from
                t_tmp
            """,
            eng
        )
        try:
            tmp_df.to_sql(
                schema='wb_yarik',
                name='purchases',
                con=eng,
                if_exists='replace'
            )
            print('ок')

        except:
            print('Что-то пошло не так: обновлённая таблица purchases не сохранилась')

if storage_df is not None:
    if len(storage_df) > 0:
        print('Записываем склад в tmp базу...')
        storage_df.to_sql(
            schema='wb_yarik',
            name='storage_tmp',
            con=eng,
            if_exists='replace'
        )
        print("ок")
        print('t_main_storage union t_tmp_storage...')
        tmp_df = pd.read_sql(
            """
            with t_main as
                (
                    select
                        lastChangeDate
                        , supplierArticle
                        , techSize
                        , barcode
                        , quantity
                        , isSupply
                        , isRealization
                        , quantityFull
                        , quantityNotInOrders
                        , warehouseName
                        , inWayToClient
                        , inWayFromClient
                        , nmId
                        , subject
                        , category
                        , daysOnSite
                        , brand
                        , SCCode
                        , Price
                        , Discount
                    from
                        wb_yarik.storage
                ),

                t_tmp as
                (
                    select
                        lastChangeDate
                        , supplierArticle
                        , techSize
                        , barcode
                        , quantity
                        , isSupply
                        , isRealization
                        , quantityFull
                        , quantityNotInOrders
                        , warehouseName
                        , inWayToClient
                        , inWayFromClient
                        , nmId
                        , subject
                        , category
                        , daysOnSite
                        , brand
                        , SCCode
                        , Price
                        , Discount
                    from
                        wb_yarik.storage_tmp
                )

            select 
                *
            from
                t_main
            union 

            select 
                *
            from
                t_tmp
            """,
            eng
        )
        try:
            tmp_df.to_sql(
                schema='wb_yarik',
                name='storage',
                con=eng,
                if_exists='replace'
            )
            print('ок')

        except:
            print('Что-то пошло не так: обновлённая таблица storage не сохранилась')

if supplies_df is not None:
    if len(supplies_df) > 0:
        print('Записываем поставки в tmp базу...')
        supplies_df.to_sql(
            schema='wb_yarik',
            name='supplies_tmp',
            con=eng,
            if_exists='replace'
        )
        print("ок")
        print('t_main_supplies union t_tmp_supplies...')
        tmp_df = pd.read_sql(
            """
            with t_main as
                (
                    select
                        incomeId
                        , number
                        , date
                        , lastChangeDate
                        , supplierArticle
                        , techSize
                        , barcode
                        , quantity
                        , totalPrice
                        , dateClose
                        , warehouseName
                        , nmId
                        , status
                    from
                        wb_yarik.supplies
                ),

                t_tmp as
                (
                    select
                        incomeId
                        , number
                        , date
                        , lastChangeDate
                        , supplierArticle
                        , techSize
                        , barcode
                        , quantity
                        , totalPrice
                        , dateClose
                        , warehouseName
                        , nmId
                        , status
                    from
                        wb_yarik.supplies_tmp
                )

            select 
                *
            from
                t_main
            union 

            select 
                *
            from
                t_tmp
            """,
            eng
        )
        try:
            tmp_df.to_sql(
                schema='wb_yarik',
                name='supplies',
                con=eng,
                if_exists='replace'
            )
            print('ок')

        except:
            print('Что-то пошло не так: обновлённая таблица supplies не сохранилась')

if report_df is not None:
    if len(report_df) > 0:
        print('Записываем отчёт в db...')
        report_df.to_sql(
            schema='wb_yarik',
            name='report',
            con=eng,
            if_exists='replace'
        )
        print('ок')