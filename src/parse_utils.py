from typing import Dict
import os
from datetime import datetime
import sys
import time
import requests
import numpy as np
import pandas as pd


def parse_supplies(
    date_from: str,
    wb_key: str,
    var_name_dict: Dict=None
) -> pd.DataFrame:

    supplies_df = requests.get(
        url=f'https://suppliers-stats.wildberries.ru/api/v1/supplier/incomes?dateFrom={date_from}&key={wb_key}',
    )
    if supplies_df.text != '':
        supplies_df = pd.DataFrame(supplies_df.json())
        for col in supplies_df.columns:
            if 'date' in col.lower():
                supplies_df[col] = pd.to_datetime(supplies_df[col])
        if var_name_dict is not None:
            supplies_df.columns = [var_name_dict.get(col, col) for col in supplies_df.columns]
        return supplies_df
    else:
        return None


def parse_storage(
    date_from: str,
    wb_key: str,
    var_name_dict: Dict=None
) -> pd.DataFrame:
    # Склад
    storage_df = requests.get(
        url=f"https://suppliers-stats.wildberries.ru/api/v1/supplier/stocks?dateFrom={date_from}&key={wb_key}"
    )
    if storage_df.text != '':
        storage_df = pd.DataFrame(storage_df.json())
        for col in storage_df.columns:
            if 'date' in col.lower():
                storage_df[col] = pd.to_datetime(storage_df[col])
        if var_name_dict is not None:
            storage_df.columns = [var_name_dict.get(col, col) for col in storage_df.columns]
        return storage_df
    else:
        return None


def parse_purchases(
    date_from: str,
    wb_key: str,
    flag: int=1,
    max_try: int=10,
    var_name_dict: Dict=None
) -> pd.DataFrame:
    # Заказы
    purchase_df = None
    for i in range(max_try):
        if purchase_df is None:
            purchase_df = requests.get(
                url=f"https://suppliers-stats.wildberries.ru/api/v1/supplier/orders?dateFrom={date_from}&flag={flag}&key={wb_key}"
            )
        if purchase_df.text == '':
            time.sleep(1)
            purchase_df = requests.get(
                url=f"https://suppliers-stats.wildberries.ru/api/v1/supplier/orders?dateFrom={date_from}&flag={flag}&key={wb_key}"
            )
        else:
            break
    if purchase_df.text != '':
        purchase_df = pd.DataFrame(purchase_df.json())
        for col in purchase_df.columns:
            if 'date' in col.lower():
                purchase_df[col] = pd.to_datetime(purchase_df[col])
        if var_name_dict is not None:
            purchase_df.columns = [var_name_dict.get(col, col) for col in purchase_df.columns]
        return purchase_df
    else:
        return None


def parse_sales(
    date_from: str,
    wb_key: str,
    flag: int=1,
    max_try: int=10,
    var_name_dict: Dict=None
) -> pd.DataFrame:
    # Продажи
    sales_df = None
    for i in range(max_try):
        if sales_df is None:
            sales_df = requests.get(
                url=f"https://suppliers-stats.wildberries.ru/api/v1/supplier/sales?dateFrom={date_from}&flag={flag}&key={wb_key}"
            )
        if sales_df.text == '':
            time.sleep(1)
            sales_df = requests.get(
                url=f"https://suppliers-stats.wildberries.ru/api/v1/supplier/sales?dateFrom={date_from}&flag={flag}&key={wb_key}"
            )
        else:
            break
    if sales_df.text != '':
        sales_df = pd.DataFrame(sales_df.json())
        for col in sales_df.columns:
            if 'date' in col.lower():
                sales_df[col] = pd.to_datetime(sales_df[col])
        if var_name_dict is not None:
            sales_df.columns = [var_name_dict.get(col, col) for col in sales_df.columns]
        return sales_df
    else:
        return None

def parse_report(
    date_from: str,
    wb_key: str,
    limit: int=100_000,
    var_name_dict: Dict=None
) -> pd.DataFrame:
    # Отчёт о продажах по реализации
    today = str(datetime.now().date())
    report_df = requests.get(
        url=f"https://suppliers-stats.wildberries.ru/api/v1/supplier/reportDetailByPeriod?dateFrom={date_from}&key={wb_key}&limit={limit}&rrdid=0&dateto={today}"
    )
    report_df = pd.DataFrame(report_df.json())
    for col in report_df.columns:
        if 'date' in col.lower():
            report_df[col] = pd.to_datetime(report_df[col])
    if var_name_dict is not None:
        report_df.columns = [var_name_dict.get(col, col) for col in report_df.columns]
    return report_df












