{{
  config(
    materialized='view',
    description='Staging layer for demography population data - our atomic building blocks'
  )
}}

with source_data as (
    select 
        -- Geographic identifiers (converted to integers to match codes_data)
        province_code::numeric::integer as province_code,
        province_name,
        cmun::integer as municipality_code,
        municipality_name,
        
        -- Population metrics (validated)
        case 
            when population_total >= 0 then population_total 
            else null 
        end as population_total,
        case 
            when population_male >= 0 then population_male 
            else null 
        end as population_male,
        case 
            when population_female >= 0 then population_female 
            else null 
        end as population_female,
        
        -- Time dimension
        data_year,
        
        -- Data lineage
        source_file,
        data_source,
        ingestion_timestamp
        
    from {{ source('raw', 'raw_demography_population') }}
    
    -- Basic data quality filters
    where municipality_name is not null
      and municipality_name != ''
      and data_year between 1996 and 2024
      and population_total is not null
)

select * from source_data