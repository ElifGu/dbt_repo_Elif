-- WITH temp_daily AS (
--     SELECT * 
--     FROM {{ref('staging_weather')}}
-- ),
-- add_weekday AS (
--     SELECT *,
--         date_part ('weekday', date)AS weekday,
--         date_part('day', date) AS day_num
--     FROM temp_daily
-- )
-- SELECT *
-- FROM add_weekday


WITH temp_daily AS (
    SELECT * 
    FROM {{ref('staging_weather')}}
),
add_weekday AS (
    SELECT *,
        trim(to_char(date, 'Day')) AS weekday,
        date_part('day', date) AS day_num
    FROM temp_daily
)
SELECT *
FROM add_weekday



