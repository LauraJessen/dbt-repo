WITH temp_daily AS (
    SELECT * 
    FROM {{ref('staging_weather')}}
),
add_weekday AS (
    SELECT *,
        trim(to_char(date, 'day')) AS weekday,
        date_part('day', date)AS day_num,
        date_part('week', date) AS week_num
    FROM temp_daily
)
SELECT *
FROM add_weekday
