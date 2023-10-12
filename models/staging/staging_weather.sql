WITH temperature_daily AS (
    SELECT ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'date')::VARCHAR)::date  AS date,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'maxtemp_c')::VARCHAR)::FLOAT AS maxtemp,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'mintemp_c')::VARCHAR)::FLOAT AS mintemp,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'avgtemp_c')::VARCHAR)::FLOAT AS avgtemp,
        (extracted_data -> 'location' -> 'name')::VARCHAR  AS city,
        (extracted_data -> 'location' -> 'country')::VARCHAR  AS country,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'maxwind_kph')::VARCHAR)::FLOAT AS max_wind,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'totalprecip')::VARCHAR)::FLOAT AS total_prec,
        (extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'condition'-> 'text')::VARCHAR  AS cond

    FROM {{source("staging", "raw_temp")}})
SELECT * 
FROM temperature_daily