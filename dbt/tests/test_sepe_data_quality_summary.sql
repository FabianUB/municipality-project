{{
  config(
    tags=['data_quality', 'sepe', 'summary'],
    severity='warn'
  )
}}

/*
Data Quality Summary Test: Overview of key data quality metrics across SEPE data

This test provides insights into:
- Municipality code completion effectiveness
- Percentage calculation issues  
- Data coverage by province
- Overall data quality metrics

This test is designed to always "fail" so results are visible - it's informational only.
Results help understand overall data quality and guide improvement efforts.
*/

with unemployment_quality as (
  select 
    'unemployment' as dataset,
    count(*) as total_records,
    count(municipality_code) as records_with_codes,
    sum(case when municipality_code_completed then 1 else 0 end) as codes_completed,
    round((sum(case when municipality_code_completed then 1 else 0 end)::numeric / count(*)::numeric) * 100, 2) as code_completion_rate,
    count(case when men_unemployment_percentage > 100 or women_unemployment_percentage > 100 then 1 end) as percentage_issues,
    count(distinct province_name) as provinces_covered,
    min(data_year) as earliest_year,
    max(data_year) as latest_year
  from {{ ref('stg_sepe__unemployment') }}
),

contracts_quality as (
  select 
    'contracts' as dataset,
    count(*) as total_records,
    count(municipality_code) as records_with_codes,
    sum(case when municipality_code_completed then 1 else 0 end) as codes_completed,
    round((sum(case when municipality_code_completed then 1 else 0 end)::numeric / count(*)::numeric) * 100, 2) as code_completion_rate,
    count(case when men_contracts_percentage > 100 or women_contracts_percentage > 100 or permanent_contracts_percentage > 100 then 1 end) as percentage_issues,
    count(distinct province_name) as provinces_covered,
    min(data_year) as earliest_year,
    max(data_year) as latest_year
  from {{ ref('stg_sepe__contracts') }}
)

select 
  dataset,
  total_records,
  records_with_codes,
  codes_completed,
  code_completion_rate,
  percentage_issues,
  provinces_covered,
  earliest_year,
  latest_year,
  'Data Quality Summary - ' || dataset || ': ' || 
  codes_completed || ' codes completed (' || code_completion_rate || '%), ' ||
  percentage_issues || ' percentage calculation issues' as summary
from unemployment_quality

union all

select 
  dataset,
  total_records,
  records_with_codes,
  codes_completed,
  code_completion_rate,
  percentage_issues,
  provinces_covered,
  earliest_year,
  latest_year,
  'Data Quality Summary - ' || dataset || ': ' || 
  codes_completed || ' codes completed (' || code_completion_rate || '%), ' ||
  percentage_issues || ' percentage calculation issues' as summary
from contracts_quality