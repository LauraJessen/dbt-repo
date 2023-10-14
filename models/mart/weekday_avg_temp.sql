WITH prep_temp_data AS (
    SELECT * 
    FROM {{ref('prep_temp')}}
),
weekly_avg_temp AS (
     SELECT date_part('week', date) as week_num,
     avg(avgtemp) as avg_temp
     FROM prep_temp_data
     group by(week_num, date)
)
SELECT * FROM weekly_avg_temp



--column "prep_temp_data.date" must appear in the GROUP BY clause or be used in an aggregate function