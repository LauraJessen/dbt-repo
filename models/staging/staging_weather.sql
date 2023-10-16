WITH temperature_daily AS (
    SELECT ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'date')::VARCHAR)::date  AS date,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'maxtemp_c')::VARCHAR)::FLOAT AS maxtemp,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'mintemp_c')::VARCHAR)::FLOAT AS mintemp,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'avgtemp_c')::VARCHAR)::FLOAT AS avgtemp,
        ((extracted_data -> 'location' -> 'lat')::VARCHAR)::NUMERIC  AS lat, 
        ((extracted_data -> 'location' -> 'lon')::VARCHAR)::NUMERIC  AS lon,
        (extracted_data -> 'location' -> 'name')::VARCHAR  AS city,
        (extracted_data -> 'location' -> 'country')::VARCHAR  AS country,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'maxwind_kph')::VARCHAR)::FLOAT AS max_wind,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'totalprecip_mm')::VARCHAR)::FLOAT AS total_prec,
        (extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'condition'-> 'text')::VARCHAR  AS cond

--old    FROM {{source("staging", "raw_weather_all")}})
--old SELECT * 
--old FROM temperature_daily
    FROM {{source("staging", "raw_weather_all")}})
SELECT
    REPLACE (city, '"', '') as city,
    REPLACE (country, '"', '') as country,
    date,
    maxtemp,
    mintemp,
    avgtemp,
    lat,
    lon,
    max_wind,
    total_prec,
    REPLACE (cond, '"', '') as cond
FROM temperature_daily