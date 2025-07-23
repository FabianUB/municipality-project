-- Population Growth Analysis Report
-- Usage: Shows municipalities with highest/lowest population growth
-- Parameters: Adjust year, population threshold, and growth direction

SELECT 
    municipality_name,
    province_name,
    population_total as current_population,
    previous_year_population,
    population_change,
    population_change_pct,
    municipality_size_category
FROM {{ ref('mart_population_summary') }}
WHERE data_year = 2024  -- Target year
  AND population_change_pct IS NOT NULL
  AND population_total > 10000  -- Minimum population threshold
ORDER BY population_change_pct DESC  -- Change to ASC for declining municipalities
LIMIT 20;