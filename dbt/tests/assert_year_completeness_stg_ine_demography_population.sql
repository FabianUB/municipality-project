{{ config(severity = 'error') }}

-- Singular test to ensure stg_ine_demography__population contains data for all expected years
-- Expected: 1996-2024 except 1997 (known to be missing from source files)
-- This test validates year completeness in our demographic dataset

with expected_years as (
    select generate_series(1996, 2024) as expected_year
    except 
    select 1997 as expected_year  -- 1997 is known to be missing from source files
),

actual_years as (
    select distinct data_year as actual_year
    from {{ ref('stg_ine_demography__population') }}
    where data_year is not null
),

missing_years as (
    select expected_year
    from expected_years
    left join actual_years on expected_years.expected_year = actual_years.actual_year
    where actual_years.actual_year is null
),

unexpected_years as (
    select actual_year
    from actual_years
    left join expected_years on actual_years.actual_year = expected_years.expected_year
    where expected_years.expected_year is null
),

test_results as (
    select 
        'missing_year' as issue_type,
        expected_year::text as year_value,
        'Expected year ' || expected_year || ' is missing from the dataset' as error_message
    from missing_years
    
    union all
    
    select 
        'unexpected_year' as issue_type,
        actual_year::text as year_value,
        'Unexpected year ' || actual_year || ' found in dataset (not in expected range 1996-2024 except 1997)' as error_message
    from unexpected_years
)

-- Return any failures - if this query returns rows, the test fails
select *
from test_results
order by year_value