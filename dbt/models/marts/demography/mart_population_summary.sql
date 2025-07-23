{{
  config(
    materialized='table',
    description='Population summary mart for reporting and analytics'
  )
}}

with population_trends as (
    select
        m.municipality_name,
        m.province_name,
        f.data_year,
        f.population_total,
        f.population_male,
        f.population_female,
        f.male_percentage,
        f.female_percentage,
        
        -- Calculate year-over-year changes
        lag(f.population_total) over (
            partition by f.municipality_key 
            order by f.data_year
        ) as previous_year_population,
        
        f.population_total - lag(f.population_total) over (
            partition by f.municipality_key 
            order by f.data_year
        ) as population_change,
        
        case 
            when lag(f.population_total) over (
                partition by f.municipality_key 
                order by f.data_year
            ) > 0 then
                round(
                    ((f.population_total - lag(f.population_total) over (
                        partition by f.municipality_key 
                        order by f.data_year
                    ))::numeric / lag(f.population_total) over (
                        partition by f.municipality_key 
                        order by f.data_year
                    )::numeric) * 100, 2
                )
            else null
        end as population_change_pct
        
    from {{ ref('fct_population') }} f
    join {{ ref('dim_municipalities') }} m
        on f.municipality_key = m.municipality_key
    where f.data_quality_flag = 'valid'
),

with_rankings as (
    select *,
        -- Rankings within province by year
        rank() over (
            partition by province_name, data_year 
            order by population_total desc
        ) as province_population_rank,
        
        
        -- Population size categories
        case 
            when population_total >= 100000 then 'Large (100k+)'
            when population_total >= 20000 then 'Medium (20k-100k)'
            when population_total >= 5000 then 'Small (5k-20k)'
            when population_total >= 1000 then 'Very Small (1k-5k)'
            else 'Micro (<1k)'
        end as municipality_size_category
        
    from population_trends
)

select * from with_rankings