WITH weekly_avg AS (
    SELECT
        city,
        date_trunc('week', date) AS week_start,
        AVG(avgtemp_c) AS avg_temp_weekly
    FROM {{ ref('prep_temp') }}
    GROUP BY city, date_trunc('week', date)
)

SELECT 
    city,
    week_start,
    avg_temp_weekly
FROM weekly_avg
ORDER BY city, week_start;
