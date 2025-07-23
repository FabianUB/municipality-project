{{
  config(
    materialized='table',
    description='Province dimension table for analytics'
  )
}}

with province_dim as (
    select distinct
        province_code,
        province_name,
        count(distinct municipality_code) as municipality_count,
        current_timestamp as created_at,
        current_timestamp as updated_at
        
    from {{ ref('int_municipalities__master') }}
    where province_code is not null
    group by province_code, province_name
)

select * from province_dim