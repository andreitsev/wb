with t_sales1 as
    (
        select
            day
            , sum(sum_sales) as sum_sales
            , avg(avg_forPay) as avg_forPay
        from
            wb_yarik.daily_sales
        where
            subject = 'Наборы столовых приборов'
        group by
            day
        order by day asc
    ),

    t_forecasts1 as
    (
        select
            day
            , `Наборы столовых приборов_forecast` as forecast
            , `Наборы столовых приборов_forecast_confint_lower` as confint_lower
            , `Наборы столовых приборов_forecast_confint_upper` as confint_upper
        from
            wb_yarik.daily_sales_forecasts
    )

select
    case
        when t1.day is not null then t1.day
        else t2.day end
    as day
    , t1.sum_sales as 'Исторические продажи'
    , t1.avg_forPay as 'Средняя цена forPay'
    , t2.forecast as 'Прогноз'
    , t2.confint_lower as 'Доверительный интервал прогноза (нижний)'
    , t2.confint_upper as 'Доверительный интервал прогноза (верхний)'
from
    t_sales1 t1
    left join
        t_forecasts1 t2
    on
        t1.day = t2.day

union

select
    case
        when t1.day is not null then t1.day
        else t2.day end
    as day
    , t1.sum_sales as 'Исторические продажи'
    , t1.avg_forPay as 'Средняя цена forPay'
    , t2.forecast as 'Прогноз'
    , t2.confint_lower as 'Доверительный интервал прогноза (нижний)'
    , t2.confint_upper as 'Доверительный интервал прогноза (верхний)'
from
    t_sales1 t1
    right join
        t_forecasts1 t2
    on
        t1.day = t2.day


