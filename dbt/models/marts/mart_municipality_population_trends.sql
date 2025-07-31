{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['municipality_code', 'data_year'], 'type': 'btree'},
      {'columns': ['data_year'], 'type': 'btree'}
    ]
  )
}}

with population_base as (
  select
    pop.municipality_code,
    pop.municipality_name,
    pop.province_code,
    codes.autonomous_community_code,
    pop.data_year,
    -- Use population_total if available, otherwise sum male + female
    coalesce(
      pop.population_total,
      coalesce(pop.population_male, 0) + coalesce(pop.population_female, 0)
    ) as total_population
  from {{ ref('stg_ine_demography__population') }} pop
  left join {{ ref('stg_ine_codes_data__municipalities') }} codes
    on pop.municipality_code = codes.municipality_code
    and pop.province_code = codes.province_code
  where pop.municipality_code is not null
    and pop.municipality_code != 0  -- Exclude placeholder codes
),

population_with_trends as (
  select 
    curr.municipality_code,
    curr.municipality_name,
    curr.province_code,
    curr.autonomous_community_code,
    curr.data_year,
    curr.total_population,
    
    -- Previous year population using self-join (more reliable than LAG)
    prev.total_population as previous_year_population,
    
    -- Calculate absolute change from previous year
    curr.total_population - prev.total_population as population_change,
    
    -- Calculate percentage change from previous year
    case 
      when prev.total_population > 0 then
        round(
          ((curr.total_population - prev.total_population)::numeric / prev.total_population::numeric) * 100, 
          2
        )
      else null
    end as population_change_pct
    
  from population_base curr
  left join population_base prev
    on curr.municipality_code = prev.municipality_code
    and curr.province_code = prev.province_code  -- Ensure same municipality
    and curr.data_year = prev.data_year + 1      -- Join with previous year
),

population_with_final_metrics as (
  select 
    municipality_code,
    municipality_name,
    province_code,
    autonomous_community_code,
    data_year,
    total_population,
    previous_year_population,
    population_change,
    population_change_pct,
    
    -- Rank municipalities by population within each year
    row_number() over (
      partition by data_year 
      order by total_population desc
    ) as population_rank_national,
    
    -- Rank municipalities within their province each year
    row_number() over (
      partition by data_year, province_code
      order by total_population desc
    ) as population_rank_provincial

  from population_with_trends
)

select 
  municipality_code,
  municipality_name,
  province_code,
  autonomous_community_code,
  data_year,
  total_population,
  previous_year_population,
  population_change,
  population_change_pct,
  population_rank_national,
  population_rank_provincial,
  
  -- Classify population change trends
  case 
    when population_change is null then 'no_data'
    when population_change > 0 then 'growth'
    when population_change < 0 then 'decline'
    else 'stable'
  end as population_trend,
  
  -- Classify growth/decline magnitude
  case 
    when population_change_pct is null then 'no_data'
    when population_change_pct >= 5 then 'high_growth'
    when population_change_pct >= 1 then 'moderate_growth'
    when population_change_pct > -1 then 'stable'
    when population_change_pct > -5 then 'moderate_decline'
    else 'high_decline'
  end as population_trend_category,
  
  -- Add metadata
  current_timestamp as created_at

from population_with_final_metrics
order by municipality_code, data_year