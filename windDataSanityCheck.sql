select datadate,count(*) 
from  wind_data_daily
group by datadate
order by datadate
limit 3000;