import os
from os.path import join as p_join
import sys
# sys.path.insert(0, '..')
import time
from datetime import datetime
import json
import pickle
from tqdm import tqdm

import numpy as np
import pandas as pd

from pmdarima.arima import auto_arima

try:
    from src.utils import create_wb_db_connection
    eng = create_wb_db_connection()
except:
    print("Не удалось установить подключение к базе данных!")
if 'PYTHONPATH' in os.environ:
    PROJECT_PATH = os.environ["PYTHONPATH"]
    os.chdir(PROJECT_PATH)
else:
    PROJECT_PATH = '..'

def find_latest_model() -> str:
    now = datetime.now()
    current_min_days_diff = np.inf
    latest_model = None
    for folder in os.listdir(p_join(PROJECT_PATH, 'models')):
        if '=' in folder:
            models_train_date = folder.split('=')[-1]
            curr_diff = (now - pd.Timestamp(models_train_date)).days
            if curr_diff <= current_min_days_diff:
                current_min_days_diff = curr_diff
                latest_model = folder
        else:
            print(f'у модели {folder} нет даты обучения в названии!')
            continue
    return latest_model


def make_forecast(schema: str='wb_yarik', table_name: str='daily_sales_forecasts', eng: object=eng,
                  n_steps_forecast: int=30, model_name: str=None) -> None:
    if model_name is None:
        model_name = find_latest_model()

    models_dict = pickle.load(
        open(p_join(PROJECT_PATH, 'models', model_name, 'arima_models.pkl'), mode='rb')
    )
    max_train_date = json.load(
        open(p_join(PROJECT_PATH, 'models', model_name, 'dates.json'), mode='r', encoding='utf-8'),
    )['max_train_date']

    forecast_df = None

    for subject in tqdm(models_dict):
        preds, confint = models_dict[subject].predict(n_periods=n_steps_forecast, return_conf_int=True)
        preds = np.array([max(val, 0) for val in preds])
        forecast_dates = [(pd.Timestamp(max_train_date) + pd.DateOffset(days=i)).date() for i in range(1, n_steps_forecast + 1)]

        if forecast_df is None:
            forecast_df = pd.DataFrame({
                "day": forecast_dates,
                f'{subject}_forecast': preds,
                f'{subject}_forecast_confint_lower': confint[:, 0],
                f'{subject}_forecast_confint_upper': confint[:, 1],
            })
        else:
            forecast_df[f'{subject}_forecast'] = preds
            forecast_df[f'{subject}_forecast_confint_lower'] = confint[:, 0]
            forecast_df[f'{subject}_forecast_confint_upper'] = confint[:, 1]

    print(f"Сохраняем предсказания...")
    forecast_df.to_sql(
        schema=schema,
        name=table_name,
        con=eng,
        if_exists='replace'
    )
    print("ок")
    return

if __name__ == '__main__':
    make_forecast()