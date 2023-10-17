with prep_temp_data as (
    SELECT * 
    FROM {{ref('prep_temp')}}
),
windy_locations as(
    SELECT date_part('month', date) AS month,
    max(maxwind_kph) AS max_wind,
    city,
    country
    from prep_temp_data
    WHERE date is not null
    GROUP BY city, country, month
    ORDER BY city, month
)
select *
FROM windy_locations
