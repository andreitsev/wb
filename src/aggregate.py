import sys
sys.path.insert(0, '..')
import json
from os.path import join as p_join

import pandas as pd
import sqlalchemy as sa


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
                    , sum(case when quantity > 0 then quantity else 0 end) as sum_sales
                    , abs(sum(case when quantity < 0 then quantity else 0 end)) as sum_returned
                    , sum(case when quantity > 0 then quantity * totalPrice else 0 end) as sum_totalPrice
                    , sum(case when quantity > 0 then quantity * pricewithdisc else 0 end) as sum_pricewithdisc
                    , sum(case when quantity > 0 then quantity * forPay else 0 end) as sum_forPay
                    , sum(case when quantity > 0 then quantity * finishedPrice else 0 end) as sum_finishedPrice
                    , abs(sum(case when quantity < 0 then quantity * totalPrice else 0 end)) as sum_totalPrice_returned
                    , abs(sum(case when quantity < 0 then quantity * pricewithdisc else 0 end)) as sum_pricewithdisc_returned
                    , abs(sum(case when quantity < 0 then quantity * forPay else 0 end)) as sum_forPay_returned
                    , abs(sum(case when quantity < 0 then quantity * finishedPrice else 0 end)) as sum_finishedPrice_returned
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
            )
        
        select
            t1.supplierArticle
            , t1.techSize
            , t1.subject
            , t1.category
            , t1.brand
            , t1.barcode
            , t1.day
            
            , t1.sum_sales
            , t1.sum_returned
            , t2.sum_purchases
            
            , t1.sum_totalPrice
            , t1.sum_pricewithdisc
            , t1.sum_forPay
            , t1.sum_finishedPrice
            
            , t1.sum_totalPrice_returned
            , t1.sum_pricewithdisc_returned
            , t1.sum_forPay_returned
            , t1.sum_finishedPrice_returned
            
        from
            t_daily_sales t1
            inner join
                t_daily_purchases t2
            on
                1 = 1
                and t1.supplierArticle = t2.supplierArticle
                and t1.techSize = t2.techSize
                and t1.day = t2.day
        """,
        eng
    )
    return daily_sales_df

if __name__ == '__main__':
    daily_sales_df = make_daily_sales()
    print(f"Сохраняем daily_sales...")
    daily_sales_df.to_sql(
        schema='wb_yarik',
        name='daily_sales',
        con=eng,
        if_exists='replace'
    )
    print('ок')