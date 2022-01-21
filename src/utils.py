import json
import os
from os.path import join as p_join
import sys
# sys.path.insert(0, '..')
import sqlalchemy as sa

if 'PYTHONPATH' in os.environ:
    PROJECT_PATH = os.environ["PYTHONPATH"]
    os.chdir(PROJECT_PATH)
else:
    PROJECT_PATH = '..'

wb_key = open(p_join(PROJECT_PATH, 'configs', 'wildberries_api64.txt'), mode='r', encoding='utf-8').read()
var_name_dict = json.load(
    open(p_join(PROJECT_PATH, 'configs', 'wb_columns_dict.json'), mode='r', encoding='utf-8')
)

sql_db_credentials = json.load(
    open(p_join(PROJECT_PATH, 'configs', 'sql_db_creadentials.json'), mode='r', encoding='utf-8')
)
db = 'yarik'


def create_wb_db_connection() -> object:
    login = sql_db_credentials[db]['login']
    passwd = sql_db_credentials[db]['password']
    host = sql_db_credentials[db]['host']
    port = sql_db_credentials[db]['port']
    eng = sa.create_engine(
        f"mysql+pymysql://{login}:{passwd}@{host}:{port}"
    )
    eng.connect()
    return eng