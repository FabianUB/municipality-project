-- Top Municipalities by Population Report
-- Usage: Shows the largest municipalities for any given year
-- Parameters: Change data_year and LIMIT as needed

SELECT 
    municipality_name,
    province_name,
    population_total,
    population_male,
    population_female,
    municipality_size_category,
    province_population_rank
FROM {{ ref('mart_population_summary') }}
WHERE data_year = 2024  -- Change year as needed
ORDER BY population_total DESC 
LIMIT 20;  -- Change limit as needed