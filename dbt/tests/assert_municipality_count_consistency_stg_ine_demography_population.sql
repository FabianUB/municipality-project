{{ config(severity = 'warn') }}

-- Singular test to ensure each year has a similar number of municipalities
-- Expected: Around 8,000+ municipalities per year (varies slightly due to municipal changes)
-- This test helps detect data quality issues like missing data or incomplete loads

with yearly_municipality_counts as (
    select 
        data_year,
        count(distinct concat(province_code, '-', municipality_code)) as municipality_count,
        count(*) as total_records
    from {{ ref('stg_ine_demography__population') }}
    where province_code is not null 
      and municipality_code is not null
      and municipality_name is not null
    group by data_year
),

municipality_stats as (
    select 
        avg(municipality_count) as avg_municipalities,
        stddev(municipality_count) as stddev_municipalities,
        min(municipality_count) as min_municipalities,
        max(municipality_count) as max_municipalities,
        -- Define acceptable range: within 2 standard deviations or at least 7,000 municipalities
        greatest(avg(municipality_count) - 2 * stddev(municipality_count), 7000) as min_acceptable,
        avg(municipality_count) + 2 * stddev(municipality_count) as max_acceptable
    from yearly_municipality_counts
),

outlier_years as (
    select 
        ymc.data_year,
        ymc.municipality_count,
        ymc.total_records,
        ms.min_acceptable,
        ms.max_acceptable,
        ms.avg_municipalities,
        case 
            when ymc.municipality_count < ms.min_acceptable then 'too_few_municipalities'
            when ymc.municipality_count > ms.max_acceptable then 'too_many_municipalities'
        end as issue_type
    from yearly_municipality_counts ymc
    cross join municipality_stats ms
    where ymc.municipality_count < ms.min_acceptable 
       or ymc.municipality_count > ms.max_acceptable
)

-- Return any outlier years - if this query returns rows, the test fails/warns
select 
    data_year,
    municipality_count,
    total_records,
    round(avg_municipalities, 0) as expected_avg_municipalities,
    round(min_acceptable, 0) as min_acceptable_count,
    round(max_acceptable, 0) as max_acceptable_count,
    issue_type,
    case 
        when issue_type = 'too_few_municipalities' then 
            'Year ' || data_year || ' has only ' || municipality_count || ' municipalities (expected ~' || round(avg_municipalities, 0) || ')'
        when issue_type = 'too_many_municipalities' then 
            'Year ' || data_year || ' has ' || municipality_count || ' municipalities (expected ~' || round(avg_municipalities, 0) || ')'
    end as warning_message
from outlier_years
order by data_year