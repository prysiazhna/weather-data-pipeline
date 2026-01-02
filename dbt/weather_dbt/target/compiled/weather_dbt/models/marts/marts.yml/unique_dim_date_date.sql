
    
    

select
    date as unique_field,
    count(*) as n_records

from "weather"."analytics_core"."dim_date"
where date is not null
group by date
having count(*) > 1


