WITH prep_temp_data AS (
    SELECT * 
    FROM {{ref('prep_temp')}}
),
weekly_avg_temp AS (
     SELECT date_part('week', date) as week_num,
     avg(avgtemp_c) as avg_temp
     FROM prep_temp_data
     group by(week_num)
)
SELECT * FROM weekly_avg_temp;
