{{
  config(
    materialized='table',
    description='Time dimension table for analytics'
  )
}}

with years as (
    select distinct data_year as year
    from {{ ref('stg_demography__population') }}
    where data_year is not null
),

time_dim as (
    select
        year,
        year as year_number,
        case 
            when year % 10 = 0 then year || 's decade'
            else (year - (year % 10)) || 's decade'
        end as decade,
        case 
            when year < 2000 then '20th century'
            when year < 2100 then '21st century'
            else 'Future'
        end as century,
        case 
            when year <= 2000 then 'pre_2000'
            when year <= 2010 then '2000s'
            when year <= 2020 then '2010s'
            else '2020s'
        end as period,
        current_timestamp as created_at,
        current_timestamp as updated_at
        
    from years
)

select * from time_dim