with prep_temp_data as (
    SELECT * 
    FROM {{ref('prep_temp')}}
),
monthly_max as(
    SELECT date_part('month', date) AS month,
    max(maxtemp_c) AS max_temp,
    city,
    country,
    lat,
    lon
    from prep_temp_data
    WHERE date is not null
    GROUP BY month, city, country
    ORDER BY month, max_temp
)
select *
FROM monthly_max