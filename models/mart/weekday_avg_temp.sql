-- with weekday_avg as (
--     select 
--         avg_temp,
--         avg(gbp) as avg_gbp,
--         avg(usd) as avg_usd,
--         avg(inr) as avg_inr
--     from {{ref('prep_temp')}}
--     group by base_currency
-- )

-- select *
-- from weekday_avg

with prep_temp_data as (
    SELECT * 
    FROM {{ref('prep_temp')}}
),
weekday_avg as(
    SELECT avg(avgtemp_c) AS, weekday
    from prep_temp_data
    WHERE weekday is not null
    GROUP BY weekday
)
select *
FROM weekday_avg