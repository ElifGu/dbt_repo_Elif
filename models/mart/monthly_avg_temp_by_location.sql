
with prep_temp_data as (
    SELECT * 
    FROM {{ref('prep_temp')}}
),
monthly_avg as(
    SELECT date_part('month', date) AS month,
    avg(avgtemp_c) AS avg_temp,
    city
    from prep_temp_data
    WHERE date is not null
    GROUP BY month, city
    ORDER BY month, avg_temp
)
select *
FROM monthly_avg;