{{
  config(
    materialized='table',
    description='Clean demography data with calculated fields and data quality flags'
  )
}}

with population_data as (
    select *,
        -- Calculate derived metrics
        case 
            when population_male is not null and population_female is not null 
            then population_male + population_female
            else null
        end as population_calculated_total,
        
        case 
            when population_male is not null and population_total is not null and population_total > 0
            then round((population_male::numeric / population_total::numeric) * 100, 2)
            else null
        end as male_percentage,
        
        case 
            when population_female is not null and population_total is not null and population_total > 0
            then round((population_female::numeric / population_total::numeric) * 100, 2)
            else null
        end as female_percentage
        
    from {{ ref('stg_demography__population') }}
),

with_quality_flags as (
    select *,
        -- Data quality flags
        case 
            when population_total is null then 'missing_total'
            when population_male is null or population_female is null then 'missing_gender_breakdown'
            when abs(population_total - population_calculated_total) > 1 then 'total_mismatch'
            when population_total = 0 then 'zero_population'
            else 'valid'
        end as data_quality_flag,
        
        -- Gender balance flag
        case 
            when male_percentage > 60 then 'male_majority'
            when female_percentage > 60 then 'female_majority'
            when abs(male_percentage - 50) <= 5 then 'balanced'
            else 'other'
        end as gender_balance_flag
        
    from population_data
)

select * from with_quality_flags