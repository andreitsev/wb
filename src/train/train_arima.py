import pickle
import time
import json
from datetime import datetime
from tqdm import tqdm
import os
from os.path import join as p_join
import sys
# sys.path.insert(0, '..')
import time

import argparse

from typing import Dict, Callable, List
import numpy as np
import pandas as pd
from pmdarima.arima import auto_arima

from src.utils import create_wb_db_connection

try:
    eng = create_wb_db_connection()
except:
    print('Не удалось подключиться к базе!')
if 'PYTHONPATH' in os.environ:
    PROJECT_PATH = os.environ["PYTHONPATH"]
    os.chdir(PROJECT_PATH)
else:
    PROJECT_PATH = '..'

today = str(datetime.now().date())

def make_df_for_arima(subjects_list: List[str]=None, save_path: str=None) -> pd.DataFrame:

    """
    Подготавливает датафрейм для обучения Арима моделей (на основе ежедневных продаж wb_yarik.daily_sales)
    :param subjects_list: Список subject'ов для которых вернуть датафрейм
    :return:
        Датафрейм с дневными продажами каждого subject'а
    """

    df_for_forecast = pd.read_sql(
        """
        select
            day
            , subject
            , sum(sum_sales) as sum_sales
        from
            wb_yarik.daily_sales
        group by
            day
            , subject
        order by subject, day asc
        """,
        eng
    )

    if subjects_list is not None:
        df_for_forecast = df_for_forecast.loc[(df_for_forecast['subject'].isin(set(subjects_list)))]

    df_for_forecast['day'] = pd.to_datetime(df_for_forecast['day'])

    min_date, max_date = df_for_forecast['day'].min(), df_for_forecast['day'].max()
    days_range = (max_date - min_date).days

    exploded_df = pd.DataFrame(df_for_forecast['subject'].unique(), columns=['subject'])
    exploded_df['day'] = None
    exploded_df['day'].loc[0] = [(min_date + pd.DateOffset(days=i)).date() for i in range(days_range + 1)]
    exploded_df.fillna(method='ffill', inplace=True)
    exploded_df = exploded_df.explode(column='day')
    exploded_df['day'] = pd.to_datetime(exploded_df['day'])

    df_for_forecast = (
        exploded_df
            .merge(
            df_for_forecast,
            on=['subject', 'day'],
            how='left'
        )
    ).sort_values(by=['subject', 'day'], ascending=True)

    df_for_forecast['sum_sales'].fillna(0, inplace=True)
    df_for_forecast['subject'].fillna(method='ffill', inplace=True)

    if save_path is not None:
        print(f'Сохраняем df_for_forecast в {save_path}...')
        df_for_forecast.to_csv(save_path, index=False)
        print('ок')

    return df_for_forecast



def train_arimas(subjects_list: List[str]=None, save_models: bool=True, df_for_training_path: str=None) -> Dict[str, Callable]:

    """
    Обучает Арима модели для subject'ов
    :param subjects_list: Список subject для которых обучать аримы
    :param save_models: Сохранять ли словарь с моделями
    :param df_for_training_path: путь до датафрейма для обучения
    :return:
        Словарь с моделями:
            ключ - название subject'а
            значение - обученная модель класса pmdarima.arima.auto_arima
    """

    if df_for_training_path is not None:
        df_for_forecast = pd.read_csv(df_for_training_path)
    else:
        df_for_forecast = make_df_for_arima(subjects_list=subjects_list)
    min_date, max_date = df_for_forecast['day'].min(), df_for_forecast['day'].max()

    # Обучаем Аримы ----------------------------------------------------
    print("Обучаем Аримы...")
    arima_start_time = time.perf_counter()
    models_dict = {}
    for subject in tqdm(df_for_forecast.subject.unique()) if subjects_list is None else tqdm(subjects_list):
        print('='*4 + f'Обучаем ARIMA для subject={subject}...' + '='*50, end='\n'*2)
        try:
            st = time.perf_counter()
            arima_model = auto_arima(
                y=df_for_forecast['sum_sales'][(df_for_forecast['subject'] == subject)],
                start_p=0,
                d=1,
                start_q=0,
                max_p=5,
                max_d=5,
                max_q=5,
                start_P=0,
                D=1,
                start_Q=0,
                max_P=5,
                max_D=5,
                max_Q=5,
                m=12,
                seasonal=True,
                error_action='warn',
                trace=True,
                supress_warnings=True,
                stepwise=True,
                random_state=20,
                n_fits=50
            )
            print('ок')
            end_ = time.perf_counter()
            print(f"Модель для {subject} обучилась за {(end_ - st) // 60} минут {(end_ - st) % 60} секунд", end='\n'*2)
            models_dict[subject] = arima_model
        except:
            print(f"Не получилось обучить arima для {subject}!", end='\n'*2)

    arima_end_time = time.perf_counter()
    print(
        f"Аримы для всех subjects обучились за {(arima_end_time - arima_start_time) // 60} минут {(arima_end_time - arima_start_time) % 60} секунд")

    if save_models:
        print("Сохраняем модели...")
        models_name = f'subjects_arima_models_calcdate={today}'
        if models_name not in os.listdir(p_join(PROJECT_PATH, 'models')):
            os.mkdir(p_join(PROJECT_PATH, 'models', models_name))
        pickle.dump(
            models_dict,
            open(p_join(PROJECT_PATH, 'models', models_name, 'arima_models.pkl'), mode='wb')
        )
        json.dump(
            {
                "min_train_date": str(min_date.date() if 'date' in dir(min_date) else min_date),
                "max_train_date": str(max_date.date() if 'date' in dir(max_date) else max_date),
            },
            open(p_join(PROJECT_PATH, 'models', models_name, 'dates.json'), mode='w', encoding='utf-8'),
            indent=2,
            ensure_ascii=False
        )
        print('ок')
    return models_dict, max_date

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--df_save_path", help="Путь, куда сохранять датафрейм для обучения арима моделей",
                        type=str, default=None)
    parser.add_argument("--df_load_path", help="Путь, откуда взять сохранённый датафрейм для обучения арима моделей",
                        type=str, default=None)

    # Обучать ли Арима модели
    feature_parser = parser.add_mutually_exclusive_group(required=False)
    feature_parser.add_argument('--train', dest='need_model_training', action='store_true')
    feature_parser.add_argument('--no-train', dest='need_model_training', action='store_false')
    parser.set_defaults(need_model_training=True)

    args = parser.parse_args()
    df_save_path = args.df_save_path
    df_load_path = args.df_load_path
    need_model_training = args.need_model_training
    if df_save_path is not None:
        eng = create_wb_db_connection()
        _ = make_df_for_arima(save_path=df_save_path)

    if need_model_training:
        _ = train_arimas(df_for_training_path=df_load_path)
