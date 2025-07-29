{{
  config(
    tags=['data_quality', 'sepe', 'completeness'],
    severity='warn'
  )
}}

/*
Data Completeness Test: Province-level contracts data consistency across years

This test identifies provinces with significant data gaps in contracts data by comparing 
record counts per province per year. Same logic as unemployment completeness test.

Logic:
1. Calculates records per province per year for contracts data
2. Finds the median record count per province per year (expected baseline)  
3. Flags provinces/years with significantly fewer records than expected

Success Criteria:
- Each province should have similar record counts across years (within reasonable variance)
- No province/year combination should have < 50% of the expected records

Interpretation:
- Failed results indicate potential data collection issues
- Useful for identifying periods with incomplete SEPE contracts data coverage
*/

with province_year_counts as (
  select 
    province_name,
    data_year,
    count(*) as record_count
  from {{ ref('stg_sepe__contracts') }}
  where province_name is not null
  group by province_name, data_year
),

province_medians as (
  select 
    province_name,
    percentile_cont(0.5) within group (order by record_count) as median_records
  from province_year_counts
  group by province_name
),

data_completeness_issues as (
  select 
    pyc.province_name,
    pyc.data_year,
    pyc.record_count,
    pm.median_records,
    round((pyc.record_count::numeric / pm.median_records::numeric) * 100, 1) as completeness_percentage
  from province_year_counts pyc
  join province_medians pm using (province_name)
  where pyc.record_count < (pm.median_records * 0.5)  -- Less than 50% of expected
    and pm.median_records > 50  -- Only for provinces with substantial data
)

select 
  province_name,
  data_year,
  record_count,
  round(median_records) as expected_records,
  completeness_percentage,
  'Contracts data completeness below 50% of expected for ' || province_name || ' in ' || data_year as issue_description
from data_completeness_issues
order by province_name, data_year