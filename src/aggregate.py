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


daily_sales_df = pd.read_sql(
    """
    with t_agg as 
    (
        select 
            supplierArticle
            , techSize
            , subject
            , category
            , brand
            , cast(date as date) as day
            , sum(quantity) as sum_quantity
            , sum(quantity * totalPrice) as sum_totalPrice
            , sum(quantity * pricewithdisc) as sum_pricewithdisc
            , sum(quantity * forPay) as sum_forPay
            , sum(quantity * finishedPrice) as sum_finishedPrice
        from
            wb_yarik.sales
        where
            quantity > 0
        group by
            supplierArticle
            , techSize
            , subject
            , category
            , brand
            , cast(date as date)
        order by
            cast(date as date) asc
            , subject
            , techSize
            , date asc
    )

    select
        *
    from
        t_agg
    """,
    eng
)
print(f"Сохраняем daily_sales...")
daily_sales_df.to_sql(
    schema='wb_yarik',
    name='daily_sales',
    con=eng,
    if_exists='replace'
)
print('ок')