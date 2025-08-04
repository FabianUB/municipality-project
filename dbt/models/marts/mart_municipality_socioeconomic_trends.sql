{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['municipality_code', 'data_year'], 'type': 'btree'},
      {'columns': ['data_year'], 'type': 'btree'},
      {'columns': ['province_code'], 'type': 'btree'},
      {'columns': ['unemployment_rate'], 'type': 'btree'}
    ]
  )
}}

-- Combined socioeconomic analysis mart aligning demographic and employment data
-- Handles temporal granularity differences (yearly demographics vs monthly employment)
with demographic_data as (
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
    population_trend,
    population_trend_category,
    population_rank_national,
    population_rank_provincial
  from {{ ref('mart_municipality_population_trends') }}
  where total_population > 0  -- Ensure valid demographic data
),

employment_data as (
  select
    municipality_code,
    municipality_name,
    province_code,
    data_year,
    primary_total_unemployment,
    avg_total_unemployment,
    unemployment_change,
    unemployment_change_pct,
    unemployment_trend,
    months_with_data,
    data_coverage_quality,
    -- Demographics
    avg_men_unemployment,
    avg_women_unemployment,
    avg_under_25_unemployment,
    avg_25_44_unemployment,
    avg_45_plus_unemployment,
    -- Economic indicators
    unemployment_volatility_pct,
    seasonal_variation_pct,
    high_volatility_flag,
    unstable_employment_flag
  from {{ ref('int_sepe_unemployment_yearly') }}
  where primary_total_unemployment > 0  -- Ensure valid employment data
),

-- Optimize join by using inner join and limiting to overlap period upfront
combined_base as (
  select
    demo.municipality_code,
    demo.municipality_name,
    demo.province_code,
    demo.autonomous_community_code,
    demo.data_year,
    
    -- Population metrics
    demo.total_population,
    demo.population_change,
    demo.population_change_pct,
    demo.population_trend,
    demo.population_rank_national,
    
    -- Employment metrics  
    emp.primary_total_unemployment,
    emp.unemployment_change,
    emp.unemployment_change_pct,
    emp.unemployment_trend,
    emp.data_coverage_quality,
    emp.avg_men_unemployment,
    emp.avg_women_unemployment,
    emp.avg_under_25_unemployment,
    emp.unemployment_volatility_pct,
    
    -- Simplified flags
    true as has_demographic_data,
    true as has_employment_data
    
  from demographic_data demo
  inner join employment_data emp
    on demo.municipality_code = emp.municipality_code
    and demo.data_year = emp.data_year
  where demo.data_year between 2005 and 2024  -- Limit to overlap period upfront
),

-- Calculate derived socioeconomic metrics
socioeconomic_metrics as (
  select
    *,
    
    -- Core socioeconomic indicators
    case 
      when total_population > 0 and primary_total_unemployment > 0 then
        round((primary_total_unemployment::numeric / total_population::numeric) * 100, 2)
      else null
    end as unemployment_rate,
    
    -- Employment pressure indicators
    case 
      when total_population > 0 and primary_total_unemployment > 0 then
        round(primary_total_unemployment::numeric / (total_population::numeric / 1000), 1)  -- Per 1000 inhabitants
      else null
    end as unemployment_per_1000_inhabitants,
    
    -- Gender employment analysis
    case 
      when avg_men_unemployment > 0 and avg_women_unemployment > 0 then
        round((avg_men_unemployment::numeric / avg_women_unemployment::numeric), 2)
      else null
    end as men_women_unemployment_ratio,
    
    -- Age group analysis (youth unemployment as % of total)
    case 
      when primary_total_unemployment > 0 and avg_under_25_unemployment > 0 then
        round((avg_under_25_unemployment::numeric / primary_total_unemployment::numeric) * 100, 1)
      else null
    end as youth_unemployment_share,
    
    -- Economic resilience indicator (population stability during unemployment changes)
    case 
      when unemployment_change_pct is not null and population_change_pct is not null then
        case 
          when unemployment_change_pct > 10 and population_change_pct > -2 then 'resilient'
          when unemployment_change_pct > 10 and population_change_pct < -5 then 'exodus'
          when unemployment_change_pct < -10 and population_change_pct > 2 then 'attractive'
          else 'stable'
        end
      else null
    end as economic_resilience_category
    
  from combined_base
),

-- Add year-over-year correlation analysis
final_with_trends as (
  select
    curr.*,
    
    -- Previous year for socioeconomic trend analysis
    prev.unemployment_rate as prev_year_unemployment_rate,
    
    -- Unemployment rate change
    case 
      when prev.unemployment_rate > 0 then
        round(curr.unemployment_rate - prev.unemployment_rate, 2)
      else null
    end as unemployment_rate_change,
    
    -- Socioeconomic trend correlation
    case 
      when curr.population_change_pct is not null and curr.unemployment_change_pct is not null then
        case 
          when curr.population_change_pct > 0 and curr.unemployment_change_pct < 0 then 'positive_growth'
          when curr.population_change_pct < 0 and curr.unemployment_change_pct > 0 then 'economic_decline'
          when curr.population_change_pct > 0 and curr.unemployment_change_pct > 0 then 'growing_with_challenges'
          when curr.population_change_pct < 0 and curr.unemployment_change_pct < 0 then 'declining_but_improving'
          else 'stable'
        end
      else null
    end as socioeconomic_trend_pattern
    
  from socioeconomic_metrics curr
  left join socioeconomic_metrics prev
    on curr.municipality_code = prev.municipality_code
    and curr.data_year = prev.data_year + 1
)

select
  municipality_code,
  municipality_name,
  province_code,
  autonomous_community_code,
  data_year,
  
  -- Data availability indicators
  has_demographic_data,
  has_employment_data,
  data_coverage_quality,
  
  -- Population metrics
  total_population,
  population_change,
  population_change_pct,
  population_trend,
  population_rank_national,
  
  -- Employment metrics
  primary_total_unemployment,
  unemployment_change,
  unemployment_change_pct,
  unemployment_trend,
  
  -- Socioeconomic indicators
  unemployment_rate,
  prev_year_unemployment_rate,
  unemployment_rate_change,
  unemployment_per_1000_inhabitants,
  
  -- Demographics breakdown
  avg_men_unemployment,
  avg_women_unemployment,
  men_women_unemployment_ratio,
  avg_under_25_unemployment,
  youth_unemployment_share,
  
  -- Economic analysis
  unemployment_volatility_pct,
  economic_resilience_category,
  socioeconomic_trend_pattern,
  
  -- Comprehensive classification
  case 
    when unemployment_rate is null then 'no_employment_data'
    when unemployment_rate < 5 then 'low_unemployment'
    when unemployment_rate < 10 then 'moderate_unemployment'
    when unemployment_rate < 20 then 'high_unemployment'
    else 'very_high_unemployment'
  end as unemployment_category,
  
  case 
    when unemployment_rate is not null and total_population is not null then
      case 
        when unemployment_rate < 8 and population_change_pct > 1 then 'thriving'
        when unemployment_rate < 12 and population_change_pct > 0 then 'growing'
        when unemployment_rate > 15 and population_change_pct < -2 then 'struggling'
        when unemployment_rate > 20 and population_change_pct < -5 then 'critical'
        else 'stable'
      end
    else null
  end as socioeconomic_health_status,
  
  -- Metadata
  current_timestamp as created_at

from final_with_trends
order by municipality_code, data_year