with prep_temp_data as (
    SELECT * 
    FROM {{ref('prep_temp')}}
),
max_precip as(
    SELECT date_part('month', date) AS month,
    max(totalprecip_mm) AS max_precip_mm,
    city,
    country,
    lat,
    lon
    from prep_temp_data
    WHERE date is not null
    GROUP BY city, country, month, lat, lon
    ORDER BY city, month
)
select *
FROM max_precip

