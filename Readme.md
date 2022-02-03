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

Пайплайн использования: (находясь в папке  проекта)

0) 
```shell
source env.sh
```
1) Парсинг данных (и сохранение их в mysql базу данных)
```python
python3 src/parse_and_save_all.py
```
2) Получение дневных продаж из сырых (и сохранение их в mysql базу данных)
```python
python3 src/aggregate.py
```
3) Обучение Арима моделей 
```python
python3 src/train/train_arima.py
```
4) Получение скоров от Арима моделей
```python
python3 src/predict/arimas_forecast.py
```
5) Развёртываение Metabase в докере для BI аналитики
```shell
docker run -d --name meta -p 3000:3000 \
          -v $PWD/metabase-data:/metabase-data \
          -e "MB_DB_FILE=/metabase-data/metabase.db" \
        metabase/metabase
```

Пример полученного дашборда:

![image](https://user-images.githubusercontent.com/27732957/152297312-b15bebe2-34c0-48cd-9d3c-03ca1ad21c52.png)
