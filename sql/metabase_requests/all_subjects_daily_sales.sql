with t_bandage as
    (
        select
            day
            , sum(sum_sales) as bandage_s
            -- , sum(sum_purchases) as bandage_p
        from
            wb_yarik.daily_sales
        where
            subject = 'Бандажи коленные'
        group by
            day
        order by
            day asc
    ),

    t_koltunorezki as
    (
        select
            day
            , sum(sum_sales) as koltunorezki_s
            -- , sum(sum_purchases) as koltunorezki_p
        from
            wb_yarik.daily_sales
        where
            subject = 'Колтунорезки'
        group by
            day
        order by
            day asc
    ),

    t_korzinki as
    (
        select
            day
            , sum(sum_sales) as korzinki_s
            -- , sum(sum_purchases) as korzinki_p
        from
            wb_yarik.daily_sales
        where
            subject = 'Корзинки'
        group by
            day
        order by
            day asc
    ),

    t_spoons as
    (
        select
            day
            , sum(sum_sales) as spoons_s
            -- , sum(sum_purchases) as spoons_p
        from
            wb_yarik.daily_sales
        where
            subject = 'Ложки чайные'
        group by
            day
        order by
            day asc
    ),

    t_kitchen as
    (
        select
            day
            , sum(sum_sales) as kitchen_s
            -- , sum(sum_purchases) as kitchen_p
        from
            wb_yarik.daily_sales
        where
            subject = 'Наборы кухонных принадлежностей'
        group by
            day
        order by
            day asc
    ),

    t_dream_mask as
    (
        select
            day
            , sum(sum_sales) as dream_mask_s
            -- , sum(sum_purchases) as dream_mask_p
        from
            wb_yarik.daily_sales
        where
            subject = 'Маски для сна'
        group by
            day
        order by
            day asc
    ),

    t_animal_plate as
    (
        select
            day
            , sum(sum_sales) as animal_plate_s
            -- , sum(sum_purchases) as animal_plate_p
        from
            wb_yarik.daily_sales
        where
            subject = 'Миски для животных'
        group by
            day
        order by
            day asc
    ),

    t_cutlery as
    (
        select
            day
            , sum(sum_sales) as cutlery_s
            -- , sum(sum_purchases) as cutlery_p
        from
            wb_yarik.daily_sales
        where
            subject = 'Наборы столовых приборов'
        group by
            day
        order by
            day asc
    ),

    t_organizer as
    (
        select
            day
            , sum(sum_sales) as organizer_s
            -- , sum(sum_purchases) as organizer_p
        from
            wb_yarik.daily_sales
        where
            subject = 'Органайзеры для хранения'
        group by
            day
        order by
            day asc
    ),

    t_bags as
    (
        select
            day
            , sum(sum_sales) as bags_s
            -- , sum(sum_purchases) as bags_p
        from
            wb_yarik.daily_sales
        where
            subject = 'Сумки'
        group by
            day
        order by
            day asc
    )


select
    case when
        t1.day is null then t2.day else t1.day end as day
    , case when
        t1.bandage_s is null then 0 else t1.bandage_s end as Бандажи_коленные
    , case when
        t2.koltunorezki_s is null then 0 else t2.koltunorezki_s end as Колтунорезки
    , case when
        t3.korzinki_s is null then 0 else t3.korzinki_s end as Корзинки
    , case when
        t4.spoons_s is null then 0 else t4.spoons_s end as Ложки_чайные
    , case when
        t5.kitchen_s is null then 0 else t5.kitchen_s end as Наборы_кухонных_принадлежностей
    , case when
        t6.dream_mask_s is null then 0 else t6.dream_mask_s end as Маски_для_сна
    , case when
        t7.animal_plate_s is null then 0 else t7.animal_plate_s end as Миски_для_животных
    , case when
        t8.cutlery_s is null then 0 else t8.cutlery_s end as Наборы_столовых_приборов
    , case when
        t9.organizer_s is null then 0 else t9.organizer_s end as Органайзеры_для_хранения
    , case when
        t10.bags_s is null then 0 else t10.bags_s end as Сумки

from
    t_bandage t1
    left join
        t_koltunorezki t2
    on
        t1.day = t2.day

    left join
        t_korzinki t3
    on
        t1.day = t3.day

    left join
        t_spoons t4
    on
        t1.day = t4.day

    left join
        t_kitchen t5
    on
        t1.day = t5.day

    left join
        t_dream_mask t6
    on
        t1.day = t6.day

    left join
        t_animal_plate t7
    on
        t1.day = t7.day

    left join
        t_cutlery t8
    on
        t1.day = t8.day

    left join
        t_organizer t9
    on
        t1.day = t9.day

    left join
        t_bags t10
    on
        t1.day = t10.day
