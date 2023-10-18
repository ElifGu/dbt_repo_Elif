WITH temperature_daily AS (
    SELECT 
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'date')::VARCHAR)::date  AS date,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'maxtemp_c')::VARCHAR)::FLOAT AS maxtemp_c,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'mintemp_c')::VARCHAR)::FLOAT AS mintemp_c,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'avgtemp_c')::VARCHAR)::FLOAT AS avgtemp_c,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'maxwind_kph')::VARCHAR)::FLOAT AS maxwind_kph,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'totalprecip_mm')::VARCHAR)::FLOAT AS totalprecip_mm,
        (extracted_data -> 'location' -> 'name')::VARCHAR  AS city,
        (extracted_data -> 'location' -> 'region')::VARCHAR  AS region,
         (extracted_data -> 'location' -> 'country')::VARCHAR  AS original_country,
        ((extracted_data -> 'location' -> 'lat')::VARCHAR)::NUMERIC  AS lat, 
        ((extracted_data -> 'location' -> 'lon')::VARCHAR)::NUMERIC  AS lon,
         (extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'condition'-> 'text')::VARCHAR  AS cond
    FROM {{source("staging", "raw_temp")}})
SELECT 
    date,
    maxtemp_c,
    mintemp_c,
    avgtemp_c,
    maxwind_kph,
    totalprecip_mm,
    REPLACE (city, '"', '') as city,
    CASE 
        WHEN REPLACE (original_country, '"', '') = 'Russia' THEN 'Russian Federation'
        WHEN REPLACE (original_country, '"', '') = 'Congo' THEN 'Congo, Democratic Republic of the'
        ELSE REPLACE (original_country, '"', '')
    END as country,
    lat, 
    lon,
    REPLACE (cond, '"', '') as cond

FROM temperature_daily


