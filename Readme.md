<pre>
├── configs
│   ├── sql_db_creadentials.json
│   ├── wb_columns_dict.json
│   └── wildberries_api64.txt
├── env.sh
├── models -> содержит обученные модели
│   
├── notebooks
│   ├── Analyze.ipynb
│   ├── Modelling.ipynb -> Обучение моделей прогнозирования
│   ├── Readme.md
│   └── wb_parse.ipynb -> Парсинг данных wb
├── sql
│   └── metabase_requests -> скрипты для Metabase
│       ├── all_subjects_daily_sales.sql 
│       └── cutlery_daily_sales_and_forecasts.sql
└── src
    ├── aggregate.py  ->  Составление дневных продаж из сырых
    ├── parse_and_save.py
    ├── parse_and_save_all.py  ->  Парсинг данных wb (с использованием ключа)
    ├── parse_utils.py
    ├── predict
    │   └── arimas_forecast.py
    ├── train
    │   └── train_arima.py
    └── utils.py
</pre>

Usage pipeline: (```$cd <project folder>```)

0) 
```shell
source env.sh
```
1) Raw data parsing (and loading to mysql database)
```python
python3 src/parse_and_save_all.py
```
2) Getting processed daily sales from raw sales (and loading to mysql database)
```python
python3 src/aggregate.py
```
3) Training ARIMA models
```python
python3 src/train/train_arima.py
```
4) Making forecasts with trained ARIMA models
```python
python3 src/predict/arimas_forecast.py
```
5) Deploying Metabase (docker-based) as a dashboard
```shell
docker run -d --name meta -p 3000:3000 \
          -v $PWD/metabase-data:/metabase-data \
          -e "MB_DB_FILE=/metabase-data/metabase.db" \
        metabase/metabase
```

Example of the resulting dashboard:

![image](https://user-images.githubusercontent.com/27732957/152297312-b15bebe2-34c0-48cd-9d3c-03ca1ad21c52.png)
