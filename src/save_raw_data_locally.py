import sys
sys.path.insert(0, '..')
import json
import os
from os.path import join as p_join
from datetime import datetime
import argparse
from dateutil.relativedelta import relativedelta

import pandas as pd
import sqlalchemy as sa

from src.parse_utils import (
    parse_supplies,
    parse_purchases,
    parse_sales,
    parse_storage,
    parse_report
)

from src.utils import create_wb_db_connection
eng = create_wb_db_connection()

parser = argparse.ArgumentParser()
parser.add_argument("--backup_path", help="Путь, куда сохранять бекапы локально",
                    type=str, default='/home/ec2-user/wb_backup_data')
args = parser.parse_args()
backup_path = args.backup_path

try:
    print("Сохраняем sales локально...")
    pd.read_sql("select * from wb_yarik.sales", eng).to_csv(p_join(backup_path, 'raw_sales.csv'), index=False)
    print("ок")
except:
    print("Что-то пошло не так!")
try:
    print("Сохраняем purchases локально...")
    pd.read_sql("select * from wb_yarik.purchases", eng).to_csv(p_join(backup_path, 'raw_purchases.csv'), index=False)
    print("ок")
except:
    print("Что-то пошло не так!")
try:
    print("Сохраняем storage локально...")
    pd.read_sql("select * from wb_yarik.storage", eng).to_csv(p_join(backup_path, 'raw_storage.csv'), index=False)
    print("ок")
except:
    print("Что-то пошло не так!")
try:
    print("Сохраняем supplies локально...")
    pd.read_sql("select * from wb_yarik.supplies", eng).to_csv(p_join(backup_path, 'raw_supplies.csv'), index=False)
    print("ок")
except:
    print("Что-то пошло не так!")