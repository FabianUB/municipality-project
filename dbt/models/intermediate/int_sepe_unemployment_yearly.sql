{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['municipality_code', 'data_year'], 'type': 'btree'},
      {'columns': ['data_year'], 'type': 'btree'},
      {'columns': ['province_code'], 'type': 'btree'}
    ]
  )
}}

-- Aggregate monthly SEPE unemployment data to yearly metrics
-- Handles temporal granularity alignment with demographic data
with monthly_unemployment as (
  select
    municipality_code,
    municipality_name,
    province_code,
    data_year,
    data_month,
    total_unemployment,
    total_men_unemployment,
    total_women_unemployment,
    -- Age group totals
    men_under_25 + women_under_25 as total_under_25_unemployment,
    men_25_44 + women_25_44 as total_25_44_unemployment,
    men_45_plus + women_45_plus as total_45_plus_unemployment,
    -- Sector breakdowns
    agriculture_sector,
    industry_sector,
    construction_sector,
    services_sector,
    no_previous_employment
  from {{ ref('stg_sepe__unemployment') }}
  where municipality_code is not null
    and total_unemployment is not null
    and total_unemployment >= 0
),

-- Calculate yearly aggregations with comprehensive metrics
yearly_aggregations as (
  select
    municipality_code,
    municipality_name,
    province_code,
    data_year,
    
    -- Data coverage and quality metrics
    count(*) as months_with_data,
    count(case when total_unemployment > 0 then 1 end) as months_with_unemployment,
    
    -- Primary unemployment metrics (multiple aggregation methods)
    round(avg(total_unemployment), 0) as avg_total_unemployment,
    max(total_unemployment) as max_total_unemployment,  -- Peak unemployment
    min(total_unemployment) as min_total_unemployment,  -- Lowest unemployment
    
    -- December snapshot (most stable for year-end comparison)
    max(case when data_month = 12 then total_unemployment end) as dec_total_unemployment,
    
    -- Q4 average (Oct-Dec) for stability
    round(avg(case when data_month in (10, 11, 12) then total_unemployment end), 0) as q4_avg_unemployment,
    
    -- Gender breakdown (yearly averages)
    round(avg(total_men_unemployment), 0) as avg_men_unemployment,
    round(avg(total_women_unemployment), 0) as avg_women_unemployment,
    
    -- Age group breakdown (yearly averages)
    round(avg(total_under_25_unemployment), 0) as avg_under_25_unemployment,
    round(avg(total_25_44_unemployment), 0) as avg_25_44_unemployment,
    round(avg(total_45_plus_unemployment), 0) as avg_45_plus_unemployment,
    
    -- Sector breakdown (yearly averages)
    round(avg(agriculture_sector), 0) as avg_agriculture_unemployment,
    round(avg(industry_sector), 0) as avg_industry_unemployment,
    round(avg(construction_sector), 0) as avg_construction_unemployment,
    round(avg(services_sector), 0) as avg_services_unemployment,
    round(avg(no_previous_employment), 0) as avg_no_previous_employment,
    
    -- Seasonality and volatility metrics
    case 
      when count(*) >= 11 then  -- Need almost full year data
        round(
          (stddev(total_unemployment) / nullif(avg(total_unemployment), 0)) * 100, 
          2
        )
      else null 
    end as unemployment_volatility_pct,
    
    -- Seasonal trend indicators
    avg(case when data_month in (12, 1, 2) then total_unemployment end) as winter_avg_unemployment,
    avg(case when data_month in (6, 7, 8) then total_unemployment end) as summer_avg_unemployment
    
  from monthly_unemployment
  group by municipality_code, municipality_name, province_code, data_year
),

-- Add year-over-year analysis using self-join (avoiding LAG due to known issues)
yearly_with_trends as (
  select
    curr.*,
    
    -- Previous year unemployment for comparison (using December or Q4 average as most reliable)
    prev.dec_total_unemployment as prev_dec_unemployment,
    coalesce(prev.dec_total_unemployment, prev.q4_avg_unemployment, prev.avg_total_unemployment) as prev_year_unemployment,
    
    -- Year-over-year change calculations
    coalesce(curr.dec_total_unemployment, curr.q4_avg_unemployment, curr.avg_total_unemployment) - 
    coalesce(prev.dec_total_unemployment, prev.q4_avg_unemployment, prev.avg_total_unemployment) as unemployment_change,
    
    -- Year-over-year percentage change
    case 
      when coalesce(prev.dec_total_unemployment, prev.q4_avg_unemployment, prev.avg_total_unemployment) > 0 then
        round(
          ((coalesce(curr.dec_total_unemployment, curr.q4_avg_unemployment, curr.avg_total_unemployment) - 
            coalesce(prev.dec_total_unemployment, prev.q4_avg_unemployment, prev.avg_total_unemployment))::numeric / 
           coalesce(prev.dec_total_unemployment, prev.q4_avg_unemployment, prev.avg_total_unemployment)::numeric) * 100, 
          2
        )
      else null
    end as unemployment_change_pct,
    
    -- Data quality indicators
    prev.months_with_data as prev_year_months_coverage
    
  from yearly_aggregations curr
  left join yearly_aggregations prev
    on curr.municipality_code = prev.municipality_code
    and curr.data_year = prev.data_year + 1
)

select
  municipality_code,
  municipality_name,
  province_code,
  data_year,
  
  -- Data quality and coverage
  months_with_data,
  months_with_unemployment,
  case 
    when months_with_data >= 11 then 'complete'
    when months_with_data >= 6 then 'partial'
    else 'limited'
  end as data_coverage_quality,
  
  -- Primary unemployment metrics (prioritize December, then Q4, then average)
  coalesce(dec_total_unemployment, q4_avg_unemployment, avg_total_unemployment) as primary_total_unemployment,
  avg_total_unemployment,
  dec_total_unemployment,
  q4_avg_unemployment,
  max_total_unemployment,
  min_total_unemployment,
  
  -- Demographics
  avg_men_unemployment,
  avg_women_unemployment,
  avg_under_25_unemployment,
  avg_25_44_unemployment,
  avg_45_plus_unemployment,
  
  -- Economic sectors
  avg_agriculture_unemployment,
  avg_industry_unemployment,
  avg_construction_unemployment,
  avg_services_unemployment,
  avg_no_previous_employment,
  
  -- Trend analysis
  prev_year_unemployment,
  unemployment_change,
  unemployment_change_pct,
  
  -- Seasonality
  unemployment_volatility_pct,
  winter_avg_unemployment,
  summer_avg_unemployment,
  case 
    when winter_avg_unemployment > 0 and summer_avg_unemployment > 0 then
      round(((winter_avg_unemployment - summer_avg_unemployment) / summer_avg_unemployment) * 100, 2)
    else null
  end as seasonal_variation_pct,
  
  -- Trend classification
  case 
    when unemployment_change_pct is null then 'no_comparison_data'
    when unemployment_change_pct > 10 then 'high_increase'
    when unemployment_change_pct > 2 then 'moderate_increase'
    when unemployment_change_pct > -2 then 'stable'
    when unemployment_change_pct > -10 then 'moderate_decrease'
    else 'high_decrease'
  end as unemployment_trend,
  
  -- Economic indicator flags
  case when max_total_unemployment - min_total_unemployment > avg_total_unemployment * 0.3 then true else false end as high_volatility_flag,
  case when unemployment_volatility_pct > 15 then true else false end as unstable_employment_flag,
  
  -- Metadata
  current_timestamp as created_at

from yearly_with_trends
where months_with_data >= 3  -- Require at least quarterly data for meaningful aggregation
order by municipality_code, data_year