with prep_temp_data as (
    SELECT * 
    FROM {{ref('prep_temp')}}
),
monthly_min as(
    SELECT date_part('month', date) AS month,
    min(mintemp_c) AS min_temp,
    city,
    country,
    lat, lon
    from prep_temp_data
    WHERE date is not null
    GROUP BY month, city, country, lat, lon
    ORDER BY month, min_temp
)
select *
FROM monthly_min


