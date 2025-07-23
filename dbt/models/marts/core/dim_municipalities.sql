{{
  config(
    materialized='table',
    description='Municipality dimension table for analytics'
  )
}}

with municipality_dim as (
    select
        -- Surrogate key
        md5(province_code || '-' || municipality_code) as municipality_key,
        
        -- Natural keys
        province_code,
        municipality_code,
        
        -- Attributes
        municipality_name,
        province_name,
        
        -- Metadata
        last_seen_year,
        current_timestamp as created_at,
        current_timestamp as updated_at
        
    from {{ ref('int_municipalities__master') }}
)

select * from municipality_dim