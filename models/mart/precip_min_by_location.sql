with prep_temp_data as (
    SELECT * 
    FROM {{ref('prep_temp')}}
),
min_precip as(
    SELECT date_part('month', date) AS month,
    min(totalprecip_mm) AS min_precip_mm,
    city
    from prep_temp_data
    WHERE date is not null
    GROUP BY city, month
    ORDER BY city, month
)
select *
FROM min_precip