WITH prep_temp_data AS (
    SELECT * 
    FROM {{ref('prep_temp')}}
),
weekly_avg_temp AS (
     SELECT week_num,
     avg(avgtemp) as avg_temp,
     city,
     country,
     lat,
     lon
     FROM prep_temp_data
     --adding city here
     group by(week_num, city, country, lat, lon)
)
SELECT * FROM weekly_avg_temp



--column "prep_temp_data.date" must appear in the GROUP BY clause or be used in an aggregate function
