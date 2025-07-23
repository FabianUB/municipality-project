{{
  config(
    materialized='view',
    description='Standardized raw demography population data with basic cleaning'
  )
}}

with source_data as (
    select 
        -- Geographic identifiers  
        province_code,
        coalesce(province_name, '') as province_name,
        case 
            when cmun is not null then lpad(cast(cmun as text), 3, '0')
            else null
        end as municipality_code,
        municipality_name,
        
        -- Population metrics
        population_total,
        population_male,
        population_female,
        
        -- Data lineage
        data_year,
        source_file,
        data_source,
        data_source_full,
        data_category,
        source_url,
        source_description,
        ingestion_timestamp
        
    from {{ source('raw', 'raw_demography_population') }}
),

cleaned_data as (
    select
        -- Geographic identifiers (standardized)
        lpad(province_code::text, 2, '0') as province_code,
        trim(province_name) as province_name,
        municipality_code,
        trim(municipality_name) as municipality_name,
        
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
        
        -- Data lineage
        data_year,
        source_file,
        data_source,
        data_source_full,
        data_category,
        source_url,
        source_description,
        ingestion_timestamp
        
    from source_data
    where municipality_name is not null
      and municipality_name != ''
      and data_year between 1996 and 2024
)

select * from cleaned_data