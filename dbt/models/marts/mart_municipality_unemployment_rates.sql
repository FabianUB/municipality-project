{{
  config(
    materialized='incremental',
    unique_key=['municipality_code', 'data_year'],
    on_schema_change='sync_all_columns',
    indexes=[
      {'columns': ['municipality_code', 'data_year'], 'type': 'btree'},
      {'columns': ['data_year'], 'type': 'btree'},
      {'columns': ['unemployment_rate'], 'type': 'btree'},
      {'columns': ['province_code'], 'type': 'btree'}
    ]
  )
}}

-- Streamlined socioeconomic mart focusing on core unemployment rates
-- Incremental approach for performance with large datasets
with core_data as (
  select
    demo.municipality_code,
    demo.municipality_name,
    demo.province_code,
    demo.autonomous_community_code,
    demo.data_year,
    
    -- Core demographic metrics
    demo.total_population,
    demo.population_change_pct,
    demo.population_trend,
    
    -- Core employment metrics
    emp.primary_total_unemployment,
    emp.unemployment_change_pct,
    emp.unemployment_trend,
    emp.data_coverage_quality,
    
    -- Key derived metric: unemployment rate
    case 
      when demo.total_population > 0 and emp.primary_total_unemployment >= 0 then
        round((emp.primary_total_unemployment::numeric / demo.total_population::numeric) * 100, 2)
      else null
    end as unemployment_rate
    
  from {{ ref('mart_municipality_population_trends') }} demo
  inner join {{ ref('int_sepe_unemployment_yearly') }} emp
    on demo.municipality_code = emp.municipality_code
    and demo.data_year = emp.data_year
  where demo.data_year between 2020 and 2024  -- Test with recent years only
    and demo.total_population > 0
    and emp.primary_total_unemployment >= 0
    
  {% if is_incremental() %}
    -- Only process recent years for incremental builds
    and demo.data_year >= (select coalesce(max(data_year), 2020) - 1 from {{ this }})
  {% endif %}
),

-- Add year-over-year trend analysis (simplified)
with_trends as (
  select
    curr.*,
    
    -- Previous year unemployment rate for comparison
    prev.unemployment_rate as prev_year_unemployment_rate,
    
    -- Unemployment rate change
    case 
      when prev.unemployment_rate > 0 then
        round(curr.unemployment_rate - prev.unemployment_rate, 2)
      else null
    end as unemployment_rate_change,
    
    -- Combined socioeconomic pattern (simplified)
    case 
      when curr.population_change_pct > 0 and curr.unemployment_change_pct < 0 then 'positive_growth'
      when curr.population_change_pct < 0 and curr.unemployment_change_pct > 0 then 'economic_decline'
      when curr.population_change_pct > 0 and curr.unemployment_change_pct > 0 then 'growing_with_challenges'
      when curr.population_change_pct < 0 and curr.unemployment_change_pct < 0 then 'declining_but_improving'
      else 'stable'
    end as socioeconomic_pattern
    
  from core_data curr
  left join core_data prev
    on curr.municipality_code = prev.municipality_code
    and curr.data_year = prev.data_year + 1
)

select
  municipality_code,
  municipality_name,
  province_code,
  autonomous_community_code,
  data_year,
  
  -- Core metrics
  total_population,
  primary_total_unemployment,
  unemployment_rate,
  
  -- Trend analysis
  population_change_pct,
  unemployment_change_pct,
  prev_year_unemployment_rate,
  unemployment_rate_change,
  
  -- Classifications (simplified)
  case 
    when unemployment_rate is null then 'no_data'
    when unemployment_rate < 5 then 'low_unemployment'
    when unemployment_rate < 10 then 'moderate_unemployment'
    when unemployment_rate < 20 then 'high_unemployment'
    else 'very_high_unemployment'
  end as unemployment_category,
  
  case 
    when unemployment_rate < 8 and population_change_pct > 1 then 'thriving'
    when unemployment_rate < 12 and population_change_pct > 0 then 'growing'
    when unemployment_rate > 15 and population_change_pct < -2 then 'struggling'
    when unemployment_rate > 20 and population_change_pct < -5 then 'critical'
    else 'stable'
  end as socioeconomic_health,
  
  socioeconomic_pattern,
  population_trend,
  unemployment_trend,
  data_coverage_quality,
  
  -- Metadata
  current_timestamp as created_at

from with_trends
order by municipality_code, data_year