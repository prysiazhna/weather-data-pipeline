
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        location_id, date
    from "weather"."analytics_staging_models"."stg_weather_daily"
    group by location_id, date
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test