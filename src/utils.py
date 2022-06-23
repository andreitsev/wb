from typing import Dict
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

def create_wb_db_connection(
        db: str='yarik',
        sql_db_credentials: Dict[str, Dict]=sql_db_credentials,
        db_type: str=None,
) -> object:
    if db_type is None:
        db_type = sql_db_credentials[db].get("type", "mysql")
    login = sql_db_credentials[db]['login']
    passwd = sql_db_credentials[db]['password']
    host = sql_db_credentials[db]['host']
    port = sql_db_credentials[db]['port']
    db_name = sql_db_credentials[db].get("database", "")
    if db_type == 'mysql':
        connection_str = f"mysql+pymysql://{login}:{passwd}@{host}:{port}/{db_name}"
    elif db_type == 'postgres':
        connection_str = f'postgresql+psycopg2://{login}:{passwd}@{host}:{port}/{db_name}'
    else:
        raise ValueError('пока реализовано подключение только к mysql и postgres!')

    eng = sa.create_engine(
        connection_str
    )
    eng.connect()
    return eng