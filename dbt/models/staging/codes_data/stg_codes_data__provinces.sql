{{
  config(
    materialized='view',
    description='Staging layer for provinces and autonomous communities mapping'
  )
}}

with source_data as (
    select 
        -- Geographic identifiers (as integers - source of truth)
        autonomous_community_code,
        autonomous_community_name,
        province_code,
        province_name
        
    from {{ ref('provinces_autonomous_communities') }}
    
    -- Basic data quality
    where autonomous_community_name is not null
      and autonomous_community_name != ''
      and province_name is not null
      and province_name != ''
      and autonomous_community_code between 1 and 19
      and province_code between 1 and 52
)

select * from source_data