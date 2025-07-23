{{
  config(
    materialized='table',
    description='Population fact table for demographic analytics'
  )
}}

with population_facts as (
    select
        -- Keys
        md5(province_code || '-' || municipality_code) as municipality_key,
        data_year as year_key,
        
        -- Natural keys
        province_code,
        municipality_code,
        data_year,
        
        -- Measures
        population_total,
        population_male,
        population_female,
        population_calculated_total,
        male_percentage,
        female_percentage,
        
        -- Quality indicators
        data_quality_flag,
        gender_balance_flag,
        
        -- Metadata
        source_file,
        data_source,
        ingestion_timestamp,
        current_timestamp as created_at
        
    from {{ ref('int_demography__population_clean') }}
    where data_quality_flag in ('valid', 'missing_gender_breakdown')  -- Exclude severely bad data
)

select * from population_facts