
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select location_id
from "weather"."analytics_staging_models"."stg_weather_daily"
where location_id is null



  
  
      
    ) dbt_internal_test