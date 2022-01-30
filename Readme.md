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