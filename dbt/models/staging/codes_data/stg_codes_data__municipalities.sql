{{
  config(
    materialized='view',
    description='Staging layer for municipality dictionary - our atomic geographic reference'
  )
}}

with source_data as (
    select 
        -- Geographic identifiers (as integers - source of truth)
        autonomous_community_code,
        province_code,
        municipality_code,
        check_digit,
        
        -- Municipality information
        municipality_name
        
    from {{ ref('municipality_dictionary') }}
    
    -- Basic data quality
    where municipality_name is not null
      and municipality_name != ''
      and autonomous_community_code between 1 and 19
      and province_code between 1 and 52
)

select * from source_data