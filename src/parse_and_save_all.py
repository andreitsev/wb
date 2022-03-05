import sys
sys.path.insert(0, '..')
import json
import os
from os.path import join as p_join
from datetime import datetime
from dateutil.relativedelta import relativedelta

import pandas as pd
import sqlalchemy as sa

print('='*4 + 'Now:' + str(datetime.now()) + '='*40, end='\n'*2)

try:
    from fabulous import color as fb_color
    color_print = lambda x, color='green': print(getattr(fb_color, color)(x)) if 'fb_color' in globals() else print(x)
except:
    color_print = lambda x, color='green': print(x)

from src.parse_utils import (
    parse_supplies,
    parse_purchases,
    parse_sales,
    parse_storage,
    parse_report
)

from src.utils import create_wb_db_connection

try:
    print('Инициализируем подключение к db...')
    eng = create_wb_db_connection()
except:
    color_print('Не удалось подключиться к базе!', color='red')

if 'PYTHONPATH' in os.environ:
    PROJECT_PATH = os.environ["PYTHONPATH"]
    os.chdir(PROJECT_PATH)
else:
    PROJECT_PATH = '..'

wb_key = open(p_join(PROJECT_PATH, 'configs', 'wildberries_api64.txt'), mode='r', encoding='utf-8').read()
DATE = '2021-01-01'

# Поставки
print('Парсим поставки...')
try:
    supplies_df = parse_supplies(
        date_from=DATE,
        wb_key=wb_key,
    )
    color_print('ок')
except:
    supplies_df = None
    color_print('Поставки не спарсились!', color='red')

# Склад
print('Парсим склад...')
try:
    storage_df = parse_storage(
        date_from=DATE,
        wb_key=wb_key,
    )
    color_print('ок')
except:
    storage_df = None
    color_print('Склад не спарсился!', color='red')

# Заказы
print('Парсим заказы...')
try:
    purchase_df = parse_purchases(
        date_from=DATE,
        wb_key=wb_key,
        flag=0,
    )
    color_print('ок')
except:
    purchase_df = None
    color_print("Заказы не спарсились!", color='red')

# Продажи
print('Парсим продажи...')
try:
    sales_df = parse_sales(
        date_from=DATE,
        wb_key=wb_key,
        flag=0,
    )
    color_print('ок')
except:
    sales_df = None
    color_print("Продажи не спарсились!", color='red')
# Отчёт о продажах по реализации
print('Парсим отчёт о продажах по реализации...')
try:
    report_df = parse_report(
        date_from=DATE,
        wb_key=wb_key,
        limit=100_000,
    )
    color_print('ок')
except:
    report_df = None
    color_print("Отчёт о продажах по реализации не спарсился!", color='red')


if sales_df is not None:
    if len(sales_df) > 0:
        print('Записываем продажи в tmp базу...')
        try:
            sales_df.to_sql(
                schema='wb_yarik',
                name='sales_tmp',
                con=eng,
                if_exists='replace'
            )
            color_print("ок")
        except:
            color_print("Не удалось записать продажи в tmp базу!", color='red')

        print('t_main_sales union t_tmp_sales...')
        try:
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
            color_print("ок")
        except:
            color_print("Не удалось объединить t_main_sales и t_tmp_sales!", color='red')

        print('Сохраняем обновлённые sales...')
        try:
            tmp_df.to_sql(
                schema='wb_yarik',
                name='sales',
                con=eng,
                if_exists='replace'
            )
            color_print('ок')

        except:
            color_print('Что-то пошло не так: обновлённая таблица sales не сохранилась', color='red')

if purchase_df is not None:
    if len(purchase_df) > 0:
        print('Записываем заказы в tmp базу...')
        try:
            purchase_df.to_sql(
                schema='wb_yarik',
                name='purchases_tmp',
                con=eng,
                if_exists='replace'
            )
            color_print("ок")
        except:
            color_print("Не удалось записать заказы в tmp базу!", color='red')

        print('t_main_purchases union t_tmp_purchases...')
        try:
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
            color_print("ок")
        except:
            color_print("Не удалось объединить t_main_purchases и t_tmp_purchases!", color='red')

        try:
            tmp_df.to_sql(
                schema='wb_yarik',
                name='purchases',
                con=eng,
                if_exists='replace'
            )
            color_print('ок')

        except:
            color_print('Что-то пошло не так: обновлённая таблица purchases не сохранилась', color='red')

if storage_df is not None:
    if len(storage_df) > 0:
        print('Записываем склад в tmp базу...')
        try:
            storage_df.to_sql(
                schema='wb_yarik',
                name='storage_tmp',
                con=eng,
                if_exists='replace'
            )
            color_print("ок")
        except:
            color_print("Не удалось записать склад в tmp базу!", color='red')

        print('t_main_storage union t_tmp_storage...')
        try:
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
            color_print("ок")
        except:
            color_print("Не удалось объединить t_main_storage и t_tmp_storage!", color='red')

        try:
            tmp_df.to_sql(
                schema='wb_yarik',
                name='storage',
                con=eng,
                if_exists='replace'
            )
            color_print('ок')

        except:
            color_print('Что-то пошло не так: обновлённая таблица storage не сохранилась', color='red')

if supplies_df is not None:
    if len(supplies_df) > 0:
        print('Записываем поставки в tmp базу...')
        try:
            supplies_df.to_sql(
                schema='wb_yarik',
                name='supplies_tmp',
                con=eng,
                if_exists='replace'
            )
            color_print("ок")
        except:
            color_print("Не удалось записать поставки в tmp базу!", color='red')

        print('t_main_supplies union t_tmp_supplies...')
        try:
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
            color_print("ок")
        except:
            color_print("Не удалось объединить t_main_supplies и t_tmp_supplies", color='red')

        try:
            tmp_df.to_sql(
                schema='wb_yarik',
                name='supplies',
                con=eng,
                if_exists='replace'
            )
            color_print('ок')

        except:
            color_print('Что-то пошло не так: обновлённая таблица supplies не сохранилась', color='red')

if report_df is not None:
    if len(report_df) > 0:
        print('Записываем отчёт в db...')
        try:
            report_df.to_sql(
                schema='wb_yarik',
                name='report',
                con=eng,
                if_exists='replace'
            )
            color_print('ок')
        except:
            color_print("Не удалось записать отчёт в db!", color='red')