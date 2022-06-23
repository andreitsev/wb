import sys
# sys.path.insert(0, '..')
import json
import os
from os.path import join as p_join
import argparse
from datetime import datetime

import pandas as pd
import sqlalchemy as sa

try:
    from src.utils import create_wb_db_connection
except:
    raise ImportError('не могу импортировать create_wb_db_connection!')

try:
    print('Инициализируем подключение к базе...')
    eng = create_wb_db_connection()
    print('ок')
except:
    print('Не удалось подключиться к базе!')


def make_unq_subjects_all_dates_df(start_date: str='2021-01-01', end_date: str=None, eng: object=eng):
    if eng is None:
        eng = create_wb_db_connection()
    if start_date is None:
        start_date = '2021-01-01'
    if end_date is None:
        end_date = str(datetime.now().date())

    unq_keys_df = pd.read_sql(
        """
        select 
            distinct supplierArticle
            , techSize
            , barcode
            , subject
            , category
            , brand
        from
            wb_yarik.sales

        union 

        select 
            distinct supplierArticle
            , techSize
            , barcode
            , subject
            , category
            , brand
        from
            wb_yarik.purchases

        union

        select 
            distinct supplierArticle
            , techSize
            , barcode
            , subject
            , category
            , brand
        from
            wb_yarik.storage
        """,
        eng
    )
    unq_keys_df['day'] = None
    unq_keys_df['day'].loc[0] = pd.date_range(start=start_date, end=end_date)
    unq_keys_df['day'].fillna(method='ffill', inplace=True)
    unq_keys_df = unq_keys_df.explode('day').reset_index(drop=True)
    return unq_keys_df



def make_daily_sales(start_date: str='2021-01-01', eng: object=eng):
    if eng is None:
        eng = create_wb_db_connection()

    unq_keys_df = make_unq_subjects_all_dates_df(start_date=start_date, eng=eng)
    unq_keys_df.to_sql(
        schema='wb_yarik',
        name='unq_subjects_and_dates',
        con=eng,
        if_exists='replace',
        index=False,
    )

    daily_sales_df = pd.read_sql(
        """
        with t_unq_subjects_dates_range as 
            (
                select * from wb_yarik.unq_subjects_and_dates
            ),
        
            t_daily_sales as 
            (   
                select 
                    supplierArticle
                    , techSize
                    , subject
                    , category
                    , brand
                    , barcode
                    , cast(date as date) as day
                    , sum(case when quantity > 0 then totalPrice else 0 end) / sum(case when quantity > 0 then 1 else 0 end) as avg_totalPrice
                    , sum(case when quantity > 0 then pricewithdisc else 0 end) / sum(case when quantity > 0 then 1 else 0 end) as avg_pricewithdisc
                    , sum(case when quantity > 0 then forPay else 0 end) / sum(case when quantity > 0 then 1 else 0 end) as avg_forPay
                    , sum(case when quantity > 0 then finishedPrice else 0 end) / sum(case when quantity > 0 then 1 else 0 end) as avg_finishedPrice
                    , sum(case when quantity > 0 then quantity else 0 end) as sum_sales
                    , abs(sum(case when quantity < 0 then quantity else 0 end)) as sum_returned
                    , sum(case when quantity > 0 then quantity * totalPrice else 0 end) as sum_q_totalPrice
                    , sum(case when quantity > 0 then quantity * pricewithdisc else 0 end) as sum_q_pricewithdisc
                    , sum(case when quantity > 0 then quantity * forPay else 0 end) as sum_q_forPay
                    , sum(case when quantity > 0 then quantity * finishedPrice else 0 end) as sum_q_finishedPrice
                    , abs(sum(case when quantity < 0 then quantity * totalPrice else 0 end)) as sum_q_totalPrice_returned
                    , abs(sum(case when quantity < 0 then quantity * pricewithdisc else 0 end)) as sum_q_pricewithdisc_returned
                    , abs(sum(case when quantity < 0 then quantity * forPay else 0 end)) as sum_q_forPay_returned
                    , abs(sum(case when quantity < 0 then quantity * finishedPrice else 0 end)) as sum_q_finishedPrice_returned
                from
                    wb_yarik.sales
                group by
                    supplierArticle
                    , techSize
                    , subject
                    , barcode
                    , category
                    , brand
                    , cast(date as date)
                order by
                    cast(date as date) asc
                    , subject
                    , techSize
                    , date asc
            ),

            t_daily_purchases as 
            (
                select
                    supplierArticle
                    , techSize
                    , subject
                    , barcode
                    , category
                    , brand
                    , cast(date as date) as day
                    , sum(case when quantity > 0 then quantity else 0 end) as sum_q_purchased
                    , sum(case when totalPrice > 0 then totalPrice else 0 end) / sum(case when totalPrice > 0 then 1 else 0 end) as avg_totalPrice_purchased
                from
                    wb_yarik.purchases
                group by
                    supplierArticle
                    , techSize
                    , subject
                    , barcode
                    , category
                    , brand
                    , cast(date as date)
            ),

            t_daily_storage as 
            (
                select
                    cast(lastChangeDate as date) as day
                    , supplierArticle
                    , techSize
                    , barcode
                    , sum(quantity) as sum_quantity_storage
                    , sum(isSupply) as sum_isSupply_storage
                    , sum(isRealization) as sum_isRealization_storage
                    , sum(quantityFull) as sum_quantityFull_storage
                    , sum(quantityNotInOrders) as sum_quantityNotInOrders_storage
                    , sum(inWayToClient) as sum_inWayToClient_storage
                    , sum(inWayFromClient) as sum_inWayFromClient_storage
                    , sum(daysOnSite) as sum_daysOnSite_storage
                    , avg(Price) as avg_Price_storage
                    , avg(Discount) as avg_Discount_storage
                from
                    wb_yarik.storage
                group by
                    supplierArticle
                    , techSize
                    , barcode
                    , cast(lastChangeDate as date)
            ),
            
            t_daily_supplies as
            (
                select
                    cast(lastChangeDate as date) as day
                    , supplierArticle
                    , techSize
                    , barcode
                    , sum(quantity) as sum_q_supplies
                from
                    wb_yarik.storage
                group by
                    supplierArticle
                    , techSize
                    , barcode
                    , cast(lastChangeDate as date)
            )

        select
            t0.supplierArticle
            , t0.techSize
            , t0.subject
            , t0.category
            , t0.brand
            , t0.barcode
            , t0.day

            , t1.avg_totalPrice
            , t1.avg_pricewithdisc
            , t1.avg_forPay
            , t1.avg_finishedPrice

            , coalesce(t1.sum_sales, 0) as sum_sales
            , coalesce(t1.sum_returned, 0) as sum_returned
            , coalesce(t2.sum_q_purchased, 0) as sum_q_purchased
            , coalesce(t4.sum_q_supplies) as sum_q_supplies

            , coalesce(t1.sum_q_totalPrice, 0) as sum_q_totalPrice
            , coalesce(t1.sum_q_pricewithdisc, 0) as sum_q_pricewithdisc
            , coalesce(t1.sum_q_forPay, 0) as sum_q_forPay
            , coalesce(t1.sum_q_finishedPrice, 0) as sum_q_finishedPrice

            , coalesce(t1.sum_q_totalPrice_returned, 0) as sum_q_totalPrice_returned
            , coalesce(t1.sum_q_pricewithdisc_returned, 0) as sum_q_pricewithdisc_returned
            , coalesce(t1.sum_q_forPay_returned, 0) as sum_q_forPay_returned
            , coalesce(t1.sum_q_finishedPrice_returned, 0) as sum_q_finishedPrice_returned

            , coalesce(t3.sum_quantity_storage, 0) as sum_quantity_storage
            , coalesce(t3.sum_isSupply_storage, 0) as sum_isSupply_storage
            , coalesce(t3.sum_isRealization_storage, 0) as sum_isRealization_storage
            , coalesce(t3.sum_quantityFull_storage, 0) as sum_quantityFull_storage
            , coalesce(t3.sum_quantityNotInOrders_storage, 0) as sum_quantityNotInOrders_storage
            , coalesce(t3.sum_inWayToClient_storage, 0) as sum_inWayToClient_storage
            , coalesce(t3.sum_inWayFromClient_storage, 0) as sum_inWayFromClient_storage
            , coalesce(t3.sum_daysOnSite_storage, 0) as sum_daysOnSite_storage
            , coalesce(t3.avg_Price_storage, 0) as avg_Price_storage
            , coalesce(t3.avg_Discount_storage, 0) as avg_Discount_storage
        from
            t_unq_subjects_dates_range t0
            left join 
                t_daily_sales t1
            on
                1 = 1
                and t0.supplierArticle = t1.supplierArticle
                and t0.techSize = t1.techSize
                and t0.day = t1.day
                
            left join
                t_daily_purchases t2
            on
                1 = 1
                and t0.supplierArticle = t2.supplierArticle
                and t0.techSize = t2.techSize
                and t0.day = t2.day

            left join
                t_daily_storage t3
            on
                1 = 1
                and t0.supplierArticle = t3.supplierArticle
                and t0.techSize = t3.techSize
                and t0.day = t3.day
                
            left join
                t_daily_supplies t4
            on
                1 = 1
                and t0.supplierArticle = t4.supplierArticle
                and t0.techSize = t4.techSize
                and t0.day = t4.day
        where
            sum_sales <> 0
            or sum_returned <> 0
            or sum_q_totalPrice <> 0
            or sum_q_supplies <> 0
            or sum_q_pricewithdisc <> 0
            or sum_q_forPay <> 0
            or sum_q_finishedPrice <> 0
            or sum_q_totalPrice_returned <> 0
            or sum_q_pricewithdisc_returned <> 0
            or sum_q_forPay_returned <> 0
            or sum_q_finishedPrice_returned <> 0
            or sum_quantity_storage <> 0
            or sum_isSupply_storage <> 0
            or sum_isRealization_storage <> 0
            or sum_quantityFull_storage <> 0
            or sum_quantityNotInOrders_storage <> 0
            or sum_inWayToClient_storage <> 0
            or sum_inWayFromClient_storage <> 0
            or sum_daysOnSite_storage <> 0
        """,
        eng
    )
    return daily_sales_df

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    feature_parser = parser.add_mutually_exclusive_group(required=False)
    feature_parser.add_argument('--store_db', dest='db_store', action='store_true')
    feature_parser.add_argument('--dont_store_db', dest='db_store', action='store_false')
    parser.set_defaults(db_store=True)

    args = parser.parse_args()
    db_store = args.db_store

    daily_sales_df = make_daily_sales()
    if db_store:
        try:
            print(f"Сохраняем daily_sales...")
            daily_sales_df.to_sql(
                schema='wb_yarik',
                name='daily_sales',
                con=eng,
                if_exists='replace'
            )
            print('ок')
        except:
            print(f'Не получилось сохранить daily_sales_df в базу данных (дата: {str(datetime.now())}')