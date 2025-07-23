{{
  config(
    materialized='table',
    description='Master municipality dimension with the most recent information'
  )
}}

with latest_municipality_data as (
    select
        province_code,
        municipality_code,
        municipality_name,
        province_name,
        data_year,
        row_number() over (
            partition by province_code, municipality_code 
            order by data_year desc
        ) as rn
    from {{ ref('stg_demography__population') }}
    where municipality_code is not null
      and municipality_name is not null
),

municipality_master as (
    select
        province_code,
        municipality_code,
        municipality_name,
        province_name,
        data_year as last_seen_year
    from latest_municipality_data
    where rn = 1
)

select * from municipality_master