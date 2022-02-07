import sys
sys.path.insert(0, '..')
import json
import os
from os.path import join as p_join
import argparse
from datetime import datetime

import pandas as pd
import sqlalchemy as sa


if 'PYTHONPATH' in os.environ:
    PROJECT_PATH = os.environ["PYTHONPATH"]
    os.chdir(PROJECT_PATH)
else:
    PROJECT_PATH = '..'

wb_key = open(p_join(PROJECT_PATH, 'configs', 'wildberries_api64.txt'), mode='r', encoding='utf-8').read()
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

def make_daily_sales():
    daily_sales_df = pd.read_sql(
        """
        with t_daily_sales as 
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
                    , barcode
                    , cast(date as date) as day
                    , sum(quantity) as sum_purchases
                from
                    wb_yarik.purchases
                group by
                    supplierArticle
                    , techSize
                    , barcode
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
            )

        select
            t1.supplierArticle
            , t1.techSize
            , t1.subject
            , t1.category
            , t1.brand
            , t1.barcode
            , t1.day

            , t1.avg_totalPrice
            , t1.avg_pricewithdisc
            , t1.avg_forPay
            , t1.avg_finishedPrice

            , t1.sum_sales
            , t1.sum_returned
            , coalesce(t2.sum_purchases, 0) as sum_purchases

            , t1.sum_q_totalPrice
            , t1.sum_q_pricewithdisc
            , t1.sum_q_forPay
            , t1.sum_q_finishedPrice

            , t1.sum_q_totalPrice_returned
            , t1.sum_q_pricewithdisc_returned
            , t1.sum_q_forPay_returned
            , t1.sum_q_finishedPrice_returned

            , t3.sum_quantity_storage
            , t3.sum_isSupply_storage
            , t3.sum_isRealization_storage
            , t3.sum_quantityFull_storage
            , t3.sum_quantityNotInOrders_storage
            , t3.sum_inWayToClient_storage
            , t3.sum_inWayFromClient_storage
            , t3.sum_daysOnSite_storage
            , t3.avg_Price_storage
            , t3.avg_Discount_storage
        from
            t_daily_sales t1
            left join
                t_daily_purchases t2
            on
                1 = 1
                and t1.supplierArticle = t2.supplierArticle
                and t1.techSize = t2.techSize
                and t1.day = t2.day

            left join
                t_daily_storage t3
            on
                1 = 1
                and t1.supplierArticle = t3.supplierArticle
                and t1.techSize = t3.techSize
                and t1.day = t3.day
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