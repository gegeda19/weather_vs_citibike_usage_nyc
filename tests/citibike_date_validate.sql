SELECT *
FROM {{ref('mart_citibike_usage')}}
where 
date(start_date) > CURRENT_DATE()
or date(start_date) < date('2010-01-01')